
from __future__ import annotations

import asyncio
import logging
import random
import re
from typing import Any

from bs4 import BeautifulSoup

from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.daily_run.sc.extractor")

SCI_BASE = "https://www.sci.gov.in"
SCI_HOME = f"{SCI_BASE}/case-status-case-no/"
SCI_AJAX = f"{SCI_BASE}/wp-admin/admin-ajax.php"
MAX_CAPTCHA_RETRIES = 15
IMG_TIMEOUT = 10.0
DETAIL_TIMEOUT = 30

SCI_TABS = [
    "case_details",
    "listing_dates",
    "interlocutory_application_documents",
    "court_fees",
    "notices",
    "defects",
    "judgement_orders",
    "earlier_court_details",
    "similarities",
]

# All case types from SCI website — value is the numeric code used in the API
SCI_CASE_TYPES = [
    {"code": "1", "name": "SPECIAL LEAVE PETITION (CIVIL)"},
    {"code": "2", "name": "SPECIAL LEAVE PETITION (CRIMINAL)"},
    {"code": "3", "name": "CIVIL APPEAL"},
    {"code": "4", "name": "CRIMINAL APPEAL"},
    {"code": "5", "name": "WRIT PETITION (CIVIL)"},
    {"code": "6", "name": "WRIT PETITION(CRIMINAL)"},
    {"code": "7", "name": "TRANSFER PETITION (CIVIL)"},
    {"code": "8", "name": "TRANSFER PETITION (CRIMINAL)"},
    {"code": "9", "name": "REVIEW PETITION (CIVIL)"},
    {"code": "10", "name": "REVIEW PETITION (CRIMINAL)"},
    {"code": "11", "name": "TRANSFERRED CASE (CIVIL)"},
    {"code": "12", "name": "TRANSFERRED CASE (CRIMINAL)"},
    {"code": "13", "name": "SPECIAL LEAVE TO PETITION (CIVIL)..."},
    {"code": "14", "name": "SPECIAL LEAVE TO PETITION (CRIMINAL)..."},
    {"code": "15", "name": "WRIT TO PETITION (CIVIL)..."},
    {"code": "16", "name": "WRIT TO PETITION (CRIMINAL)..."},
    {"code": "17", "name": "ORIGINAL SUIT"},
    {"code": "18", "name": "DEATH REFERENCE CASE"},
    {"code": "19", "name": "CONTEMPT PETITION (CIVIL)"},
    {"code": "20", "name": "CONTEMPT PETITION (CRIMINAL)"},
    {"code": "21", "name": "TAX REFERENCE CASE"},
    {"code": "22", "name": "SPECIAL REFERENCE CASE"},
    {"code": "23", "name": "ELECTION PETITION (CIVIL)"},
    {"code": "24", "name": "ARBITRATION PETITION"},
    {"code": "25", "name": "CURATIVE PETITION(CIVIL)"},
    {"code": "26", "name": "CURATIVE PETITION(CRL)"},
    {"code": "27", "name": "REF. U/A 317(1)"},
    {"code": "28", "name": "MOTION(CRL)"},
    {"code": "31", "name": "DIARYNO AND DIARYYR"},
    {"code": "32", "name": "SUO MOTO WRIT PETITION(CIVIL)"},
    {"code": "33", "name": "SUO MOTO WRIT PETITION(CRIMINAL)"},
    {"code": "34", "name": "SUO MOTO CONTEMPT PETITION(CIVIL)"},
    {"code": "35", "name": "SUO MOTO CONTEMPT PETITION(CRIMINAL)"},
    {"code": "37", "name": "REF. U/S 14 RTI"},
    {"code": "38", "name": "REF. U/S 17 RTI"},
    {"code": "39", "name": "MISCELLANEOUS APPLICATION"},
    {"code": "40", "name": "SUO MOTO TRANSFER PETITION(CIVIL)"},
    {"code": "41", "name": "SUO MOTO TRANSFER PETITION(CRIMINAL)"},
]


class SCIContinuousExtractor:

    SOURCE = "SUPREME_COURT"

    def __init__(
        self,
        session_manager: SessionManager,
        detail_sessions: list[SessionManager] | None = None,
    ) -> None:
        self._sm = session_manager
        self._detail_sessions = detail_sessions or [session_manager]
        self._detail_rr = 0
        self._search_metrics: dict[str, int] = {
            "attempts": 0,
            "captcha_solved": 0,
            "captcha_rejected": 0,
            "captcha_empty": 0,
            "captcha_image_missing": 0,
            "retryable_errors": 0,
            "transport_failures": 0,
            "search_hits": 0,
            "no_records": 0,
            "captcha_exhausted": 0,
            "hard_failures": 0,
        }

    def _pick_detail_sm(self) -> SessionManager:
        sm = self._detail_sessions[self._detail_rr % len(self._detail_sessions)]
        self._detail_rr += 1
        return sm

    def _metric_inc(self, key: str, value: int = 1) -> None:
        self._search_metrics[key] = self._search_metrics.get(key, 0) + value

    def drain_search_metrics(self) -> dict[str, int]:
        snapshot = dict(self._search_metrics)
        for k in self._search_metrics:
            self._search_metrics[k] = 0
        return snapshot

    @property
    def case_types(self) -> list[dict[str, str]]:
        return SCI_CASE_TYPES

    async def get_base_tokens(self) -> tuple[str | None, str | None, str | None]:
        text = await self._sm.get_text(SCI_HOME, label="SCI Case No Page")
        if not text:
            return None, None, None

        soup = BeautifulSoup(text, "html.parser")
        scid_input = soup.find("input", {"name": "scid"})
        tok_input = soup.find("input", id=lambda x: x and x.startswith("tok_"))

        if not scid_input or not tok_input:
            logger.warning("[SCI] Could not locate security tokens.")
            return None, None, None

        scid = scid_input.get("value")
        tok_name = tok_input.get("name")
        tok_value = tok_input.get("value")
        return scid, tok_name, tok_value

    async def _download_captcha_bytes(self, scid: str) -> bytes | None:
        url = f"{SCI_BASE}/?_siwp_captcha&id={scid}&rand={random.random()}"
        content = await self._sm.get_bytes(
            url, timeout=IMG_TIMEOUT, label="SCI captcha image"
        )
        if not content or len(content) < 200:
            return None
        return content

    async def search_by_case_no(
        self,
        case_type: str,
        case_no: int,
        year: int,
        scid: str,
        tok_name: str,
        tok_value: str,
    ) -> dict | None:
        """
        Search SCI by case type + case number + year.

        Uses the Type 2 classifier model for captcha prediction (no brute-force).
        If the model's prediction is wrong, fetches a new captcha and retries.
        Returns the parsed response dict or None if no record.
        """
        from utils.captcha import solve_async as captcha_solve_async

        def log_captcha_attempt(
            attempt_no: int,
            prediction: str | None,
            response: str,
        ) -> None:
            logger.info(
                "[SC] attempt:%d prediction:%s response:%s",
                attempt_no,
                prediction if prediction else "-",
                response,
            )

        for attempt in range(1, MAX_CAPTCHA_RETRIES + 1):
            self._metric_inc("attempts")
            img_bytes = await self._download_captcha_bytes(scid)
            if not img_bytes:
                self._metric_inc("captcha_image_missing")
                log_captcha_attempt(attempt, None, "fail")
                await asyncio.sleep(0.05)
                continue

            # Type 2 model directly predicts the numeric answer
            captcha_val = await captcha_solve_async(img_bytes, 6, "sci")

            if not captcha_val:
                self._metric_inc("captcha_empty")
                log_captcha_attempt(attempt, None, "fail")
                continue

            self._metric_inc("captcha_solved")

            params = {
                "case_type": case_type,
                "case_no": str(case_no),
                "year": str(year),
                "scid": scid,
                tok_name: tok_value,
                "siwp_captcha_value": captcha_val,
                "es_ajax_request": "1",
                "submit": "Search",
                "action": "get_case_status_case_no",
                "language": "en",
            }

            resp = await self._sm.get(
                SCI_AJAX,
                params=params,
                timeout=DETAIL_TIMEOUT,
                label=f"SCI CaseNo({case_type}/{case_no}/{year}) val={captcha_val}",
            )

            if not resp:
                self._metric_inc("transport_failures")
                self._metric_inc("retryable_errors")
                log_captcha_attempt(attempt, captcha_val, "fail")
                return {"_search_state": "retryable_error"}

            success = resp.get("success", False)
            data = resp.get("data", "")

            if not success:
                # Captcha wrong — fetch new image and retry
                is_captcha_err = False
                if isinstance(data, str) and (
                    "incorrect" in data.lower() or "captcha" in data.lower()
                ):
                    is_captcha_err = True
                elif isinstance(data, dict) and "captcha" in str(data).lower():
                    is_captcha_err = True

                if is_captcha_err:
                    self._metric_inc("captcha_rejected")
                    log_captcha_attempt(attempt, captcha_val, "fail")
                    await asyncio.sleep(min(0.2 + random.random() * 0.3, 0.5))
                    continue

                if isinstance(data, str) and (
                    "timeout" in data.lower() or "try again" in data.lower()
                ):
                    self._metric_inc("retryable_errors")
                    log_captcha_attempt(attempt, captcha_val, "fail")
                    return {"_search_state": "retryable_error"}

                if isinstance(data, str) and "no records" in data.lower():
                    self._metric_inc("no_records")
                    log_captcha_attempt(attempt, captcha_val, "success")
                    return None

                self._metric_inc("hard_failures")
                self._metric_inc("retryable_errors")
                log_captcha_attempt(attempt, captcha_val, "fail")
                return {"_search_state": "retryable_error"}

            from utils.captcha import save_captcha_image

            save_captcha_image(img_bytes, captcha_val, "sci")

            results_html = ""
            if isinstance(data, dict):
                results_html = data.get("resultsHtml", "")
            elif isinstance(data, str):
                results_html = data

            if "No records found" in results_html or "notfound" in results_html:
                self._metric_inc("no_records")
                log_captcha_attempt(attempt, captcha_val, "success")
                return None

            row_data = self._extract_result_row(results_html)
            if row_data:
                self._metric_inc("search_hits")
                log_captcha_attempt(attempt, captcha_val, "success")
                return row_data

            self._metric_inc("hard_failures")
            self._metric_inc("retryable_errors")
            log_captcha_attempt(attempt, captcha_val, "fail")
            return {"_search_state": "retryable_error"}

        self._metric_inc("captcha_exhausted")
        return {"_search_state": "captcha_error"}

    def _extract_result_row(self, html: str) -> dict | None:
        """Extract case data from the search result HTML table."""
        soup = BeautifulSoup(html, "html.parser")
        for tr in soup.find_all("tr"):
            if not tr.has_attr("data-diary-no"):
                continue

            diary_no = tr.get("data-diary-no")
            diary_year = tr.get("data-diary-year")

            tds = tr.find_all("td")
            if len(tds) < 6:
                continue

            case_number = tds[2].get_text(strip=True, separator=" ")
            petitioner = tds[3].get_text(separator=" ", strip=True)
            respondent = tds[4].get_text(separator=" ", strip=True)
            status = tds[5].get_text(strip=True)

            return {
                "diary_no": diary_no,
                "diary_year": diary_year,
                "case_number": case_number,
                "petitioner": petitioner,
                "respondent": respondent,
                "status": status,
            }

        return None

    async def fetch_detail(self, diary_no: str, diary_year: str) -> dict[str, str]:
        base_params = {
            "diary_no": diary_no,
            "diary_year": diary_year,
            "action": "get_case_details",
            "es_ajax_request": "1",
            "language": "en",
        }
        sm = self._pick_detail_sm()

        async def fetch_tab(tab_name: str) -> tuple[str, str]:
            params = {**base_params, "tab_name": tab_name}
            resp = await sm.get(
                SCI_AJAX,
                params=params,
                timeout=DETAIL_TIMEOUT,
                label=f"SCI Det {diary_no}/{diary_year} ({tab_name or 'main'})",
            )
            if not resp or not resp.get("success"):
                return tab_name, ""
            return tab_name, str(resp.get("data", ""))

        tasks = [fetch_tab(tab) for tab in SCI_TABS]
        results = await asyncio.gather(*tasks)

        tab_data: dict[str, str] = {}
        for tab_name, html_data in results:
            tab_data[tab_name] = html_data

        return tab_data
