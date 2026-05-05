
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from typing import Any, Tuple
from urllib.parse import quote

from bs4 import BeautifulSoup

from config import COMMON_HEADERS
from daily_run.config import TESTING, VERBOSE_CAPTCHA_LOGS
from utils.logging_utils import captcha_attempt_block, format_kv_block, format_percent
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.daily_run.dc.extractor")

DC_BASE = "https://services.ecourts.gov.in/ecourtindia_v6"
DC_HOME = "https://services.ecourts.gov.in/"

DC_HEADERS = {
    **COMMON_HEADERS,
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://services.ecourts.gov.in",
    "referer": DC_HOME,
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "x-requested-with": "XMLHttpRequest",
}

MAX_CAPTCHA_RETRIES = 50
MAX_CAPTCHA_CONSEC_FAILS = 50
SEARCH_TIMEOUT = 45
DETAIL_TIMEOUT = 30
IMG_TIMEOUT = 15

DC_STATES = [
    {"state_code": "28", "name": "Andaman and Nicobar"},
    {"state_code": "2", "name": "Andhra Pradesh"},
    {"state_code": "36", "name": "Arunachal Pradesh"},
    {"state_code": "6", "name": "Assam"},
    {"state_code": "8", "name": "Bihar"},
    {"state_code": "27", "name": "Chandigarh"},
    {"state_code": "18", "name": "Chhattisgarh"},
    {"state_code": "26", "name": "Delhi"},
    {"state_code": "30", "name": "Goa"},
    {"state_code": "17", "name": "Gujarat"},
    {"state_code": "14", "name": "Haryana"},
    {"state_code": "5", "name": "Himachal Pradesh"},
    {"state_code": "12", "name": "Jammu and Kashmir"},
    {"state_code": "7", "name": "Jharkhand"},
    {"state_code": "3", "name": "Karnataka"},
    {"state_code": "4", "name": "Kerala"},
    {"state_code": "33", "name": "Ladakh"},
    {"state_code": "37", "name": "Lakshadweep"},
    {"state_code": "23", "name": "Madhya Pradesh"},
    {"state_code": "1", "name": "Maharashtra"},
    {"state_code": "25", "name": "Manipur"},
    {"state_code": "21", "name": "Meghalaya"},
    {"state_code": "19", "name": "Mizoram"},
    {"state_code": "34", "name": "Nagaland"},
    {"state_code": "11", "name": "Odisha"},
    {"state_code": "35", "name": "Puducherry"},
    {"state_code": "22", "name": "Punjab"},
    {"state_code": "9", "name": "Rajasthan"},
    {"state_code": "24", "name": "Sikkim"},
    {"state_code": "10", "name": "Tamil Nadu"},
    {"state_code": "29", "name": "Telangana"},
    {"state_code": "38", "name": "The Dadra And Nagar Haveli And Daman And Diu"},
    {"state_code": "20", "name": "Tripura"},
    {"state_code": "15", "name": "Uttarakhand"},
    {"state_code": "13", "name": "Uttar Pradesh"},
    {"state_code": "16", "name": "West Bengal"},
]

class DCContinuousExtractor:

    SOURCE = "DISTRICT_COURT"

    def __init__(
        self,
        session_manager: SessionManager,
        detail_sessions: list[SessionManager] | None = None,
    ) -> None:
        self._sm = session_manager
        self._detail_sessions = detail_sessions or [session_manager]
        self._detail_rr = 0

    def _pick_detail_sm(self) -> SessionManager:
        sm = self._detail_sessions[self._detail_rr % len(self._detail_sessions)]
        self._detail_rr += 1
        return sm

    @property
    def states(self) -> list[dict[str, str]]:
        return DC_STATES

    async def get_districts(self, state_code: str) -> list[dict[str, str]]:
        for attempt in range(1, 4):
            text = await self._sm.post_text(
                f"{DC_BASE}/?p=casestatus/fillDistrict",
                data={"state_code": state_code, "ajax_req": "true", "app_token": ""},
                headers=DC_HEADERS,
                timeout=DETAIL_TIMEOUT,
                label=f"DC districts state={state_code}",
            )
            if not text:
                await asyncio.sleep(2)
                continue

            try:
                data = json.loads(text)
                html = data.get("dist_list", "")
                soup = BeautifulSoup(html, "html.parser")
                return [
                    {
                        "dist_code": opt.get("value"),
                        "dist_name": opt.get_text(strip=True),
                    }
                    for opt in soup.find_all("option")
                    if opt.get("value") and opt.get("value") != "0"
                ]
            except Exception:
                await asyncio.sleep(2)
        return []

    async def get_complexes(
        self, state_code: str, dist_code: str
    ) -> list[dict[str, str]]:
        for attempt in range(1, 4):
            text = await self._sm.post_text(
                f"{DC_BASE}/?p=casestatus/fillcomplex",
                data={
                    "state_code": state_code,
                    "dist_code": dist_code,
                    "ajax_req": "true",
                    "app_token": "",
                },
                headers=DC_HEADERS,
                timeout=DETAIL_TIMEOUT,
                label=f"DC complex {state_code}/{dist_code}",
            )
            if not text:
                await asyncio.sleep(2)
                continue

            try:
                data = json.loads(text)
                html = data.get("complex_list", "")
                soup = BeautifulSoup(html, "html.parser")
                return [
                    {
                        "complex_code": opt.get("value"),
                        "complex_name": opt.get_text(strip=True),
                    }
                    for opt in soup.find_all("option")
                    if opt.get("value")
                ]
            except Exception:
                await asyncio.sleep(2)
        return []

    async def get_establishments(
        self, state_code: str, dist_code: str, complex_code: str
    ) -> list[dict[str, str]]:
        cplx_base = complex_code.split("@")[0] if "@" in complex_code else complex_code
        for attempt in range(1, 4):
            text = await self._sm.post_text(
                f"{DC_BASE}/?p=casestatus/fillCourtEstablishment",
                data={
                    "state_code": state_code,
                    "dist_code": dist_code,
                    "court_complex_code": cplx_base,
                    "ajax_req": "true",
                    "app_token": "",
                },
                headers=DC_HEADERS,
                timeout=DETAIL_TIMEOUT,
                label=f"DC est {state_code}/{dist_code}/{cplx_base}",
            )
            if not text:
                await asyncio.sleep(2)
                continue

            try:
                data = json.loads(text)
                html = data.get("establishment_list", "")
                soup = BeautifulSoup(html, "html.parser")
                return [
                    {"est_code": opt.get("value"), "est_name": opt.get_text(strip=True)}
                    for opt in soup.find_all("option")
                    if opt.get("value")
                ]
            except Exception:
                await asyncio.sleep(2)
        return []

    async def get_case_types(
        self,
        state_code: str,
        dist_code: str,
        complex_code: str,
        est_code: str,
    ) -> list[dict[str, str]]:
        cplx_base = complex_code.split("@")[0] if "@" in complex_code else complex_code
        payload = {
            "state_code": state_code,
            "dist_code": dist_code,
            "court_complex_code": cplx_base,
            "est_code": est_code,
            "ajax_req": "true",
            "app_token": "",
        }
        for attempt in range(1, 4):
            text = await self._sm.post_text(
                f"{DC_BASE}/?p=casestatus/fillCaseType",
                data=payload,
                headers=DC_HEADERS,
                timeout=SEARCH_TIMEOUT,
                label=f"DC case_types {state_code}/{dist_code}/{cplx_base}/{est_code}",
            )
            if TESTING:
                logger.info(
                    "[DC] Discovery: Fetching case types for est_code=%s", est_code
                )

            if not text:
                await asyncio.sleep(2)
                continue

            try:
                data = json.loads(text)
                html = data.get("casetype_list", "")
                soup = BeautifulSoup(html, "html.parser")
                res = [
                    {
                        "type_code": opt.get("value"),
                        "type_name": opt.get_text(strip=True),
                    }
                    for opt in soup.find_all("option")
                    if opt.get("value") and "^" in opt.get("value", "")
                ]
                if TESTING:
                    logger.info("[DC] Discovery: Found %d case types.", len(res))
                return res
            except Exception as e:
                logger.debug("Failed to parse case types: %s", e)
                await asyncio.sleep(2)
        return []

    async def _download_captcha_bytes(self) -> bytes | None:
        url = f"{DC_BASE}/vendor/securimage/securimage_show.php?{random.random()}"
        img_headers = {
            **DC_HEADERS,
            "accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "sec-fetch-dest": "image",
            "sec-fetch-mode": "no-cors",
        }
        content = await self._sm.get_bytes(
            url, headers=img_headers, timeout=IMG_TIMEOUT, label="DC captcha image"
        )
        if not content or len(content) < 200:
            return None
        return content

    async def search_cases_by_type(
        self,
        state_code: str,
        dist_code: str,
        complex_code: str,
        est_code: str,
        year: int,
        case_type_code: str,
        case_status: str,
        search_label: str | None = None,
    ) -> Tuple[list[dict], int, str]:
        from utils.captcha import (
            record_captcha_feedback,
            save_captcha_image,
            solve_async_with_metadata as captcha_solve_async,
        )

        cplx_base = complex_code.split("@")[0] if "@" in complex_code else complex_code
        target_label = (
            search_label
            or (
                f"state={state_code} dist={dist_code} complex={cplx_base} "
                f"est={est_code} type={case_type_code} year={year} status={case_status}"
            )
        )

        await self._sm.post_text(
            f"{DC_BASE}/?p=casestatus/set_data",
            data={
                "complex_code": complex_code,
                "selected_state_code": state_code,
                "selected_dist_code": dist_code,
                "selected_est_code": est_code,
                "ajax_req": "true",
                "app_token": "",
            },
            headers=DC_HEADERS,
            timeout=SEARCH_TIMEOUT,
            label="DC set_data",
        )

        consec_fail = 0
        stats = {
            "attempts": 0,
            "captcha_image_missing": 0,
            "captcha_empty": 0,
            "captcha_solved": 0,
            "captcha_rejected": 0,
            "transport_failures": 0,
            "parse_failures": 0,
            "captcha_accepted": 0,
            "retry_exhausted": 0,
            "consecutive_fail_cutoff": 0,
        }

        def log_captcha_attempt(
            attempt_no: int,
            prediction: str | None,
            outcome: str,
            site_result: str,
            *,
            total: int | None = None,
            will_retry: bool = False,
            solver: str | None = None,
        ) -> None:
            logger.debug(
                captcha_attempt_block(
                    "DC",
                    target_label,
                    attempt_no,
                    MAX_CAPTCHA_RETRIES,
                    prediction,
                    site_result,
                    success=outcome == "success",
                    will_retry=will_retry,
                    total=max(0, int(total or 0)) if outcome == "success" else None,
                    solver=solver,
                )
            )

        def log_summary(state: str, total: int = 0) -> None:
            accepted = max(0, int(stats["captcha_accepted"]))
            rejected = max(0, int(stats["captcha_rejected"]))
            rate = format_percent(accepted, max(accepted + rejected, 1))
            logger.info(
                "[DC] Search done: result=%s cases=%d captcha=%d/%d(%s) | %s",
                state,
                total,
                accepted,
                stats["attempts"],
                rate,
                target_label,
            )

        for attempt in range(1, MAX_CAPTCHA_RETRIES + 1):
            stats["attempts"] += 1
            if attempt > 1:
                await asyncio.sleep(0.3 + random.random() * 0.5)

            img_bytes = await self._download_captcha_bytes()
            if not img_bytes:
                stats["captcha_image_missing"] += 1
                consec_fail += 1
                log_captcha_attempt(
                    attempt,
                    None,
                    "fail",
                    "captcha_image_missing",
                    will_retry=(
                        consec_fail < MAX_CAPTCHA_CONSEC_FAILS
                        and attempt < MAX_CAPTCHA_RETRIES
                    ),
                    solver=solver_name,
                )
                if consec_fail >= MAX_CAPTCHA_CONSEC_FAILS:
                    stats["consecutive_fail_cutoff"] += 1
                    log_summary("retryable_error", 0)
                    return [], 0, "retryable_error"
                continue

            captcha, solver_name = await captcha_solve_async(img_bytes, 6, "dc")

            if not captcha:
                stats["captcha_empty"] += 1
                consec_fail += 1
                log_captcha_attempt(
                    attempt,
                    None,
                    "fail",
                    "captcha_empty",
                    will_retry=(
                        consec_fail < MAX_CAPTCHA_CONSEC_FAILS
                        and attempt < MAX_CAPTCHA_RETRIES
                    ),
                )
                if consec_fail >= MAX_CAPTCHA_CONSEC_FAILS:
                    stats["consecutive_fail_cutoff"] += 1
                    log_summary("retryable_error", 0)
                    return [], 0, "retryable_error"
                continue

            stats["captcha_solved"] += 1

            payload = (
                f"case_type_1={quote(case_type_code)}"
                f"&search_year={year}"
                f"&case_status={case_status}"
                f"&ct_captcha_code={captcha}"
                f"&state_code={state_code}"
                f"&dist_code={dist_code}"
                f"&court_complex_code={cplx_base}"
                f"&est_code={est_code}"
                f"&ajax_req=true"
                f"&app_token="
            )

            text = await self._sm.post_text(
                f"{DC_BASE}/?p=casestatus/submit_case_type",
                data=payload,
                headers={**DC_HEADERS, "accept": "*/*"},
                timeout=SEARCH_TIMEOUT,
                label=f"DC search case_type {case_type_code} year {year}",
            )

            if not text:
                stats["transport_failures"] += 1
                consec_fail += 1
                log_captcha_attempt(
                    attempt,
                    captcha,
                    "fail",
                    "no_response",
                    will_retry=(
                        consec_fail < MAX_CAPTCHA_CONSEC_FAILS
                        and attempt < MAX_CAPTCHA_RETRIES
                    ),
                    solver=solver_name,
                )
                if consec_fail >= MAX_CAPTCHA_CONSEC_FAILS:
                    stats["consecutive_fail_cutoff"] += 1
                    log_summary("retryable_error", 0)
                    return [], 0, "retryable_error"
                continue

            try:
                data = json.loads(text)
            except Exception:
                stats["parse_failures"] += 1
                consec_fail += 1
                log_captcha_attempt(
                    attempt,
                    captcha,
                    "fail",
                    "json_parse_error",
                    will_retry=attempt < MAX_CAPTCHA_RETRIES,
                    solver=solver_name,
                )
                continue

            if (
                data.get("status") == "invalid captcha"
                or "invalid captcha" in str(data).lower()
                or '<div class="alert alert-danger' in text
            ):
                stats["captcha_rejected"] += 1
                record_captcha_feedback("dc", False, solver_name)
                consec_fail += 1
                log_captcha_attempt(
                    attempt,
                    captcha,
                    "fail",
                    "invalid_captcha",
                    will_retry=(
                        consec_fail < MAX_CAPTCHA_CONSEC_FAILS
                        and attempt < MAX_CAPTCHA_RETRIES
                    ),
                    solver=solver_name,
                )
                if consec_fail >= MAX_CAPTCHA_CONSEC_FAILS:
                    stats["consecutive_fail_cutoff"] += 1
                    log_summary("retryable_error", 0)
                    return [], 0, "retryable_error"
                continue

            # DC endpoint can return either `case_data` or `party_data`.
            party_data = data.get("case_data") or data.get("party_data", "")
            if "Total number of cases : 0" in party_data or not party_data:
                stats["captcha_accepted"] += 1
                consec_fail = 0
                record_captcha_feedback("dc", True, solver_name)
                log_captcha_attempt(
                    attempt,
                    captcha,
                    "success",
                    "no_results",
                    total=0,
                    solver=solver_name,
                )
                log_summary("no_results", 0)
                return [], 0, "no_results"

            save_captcha_image(img_bytes, captcha, "dc")
            stats["captcha_accepted"] += 1
            consec_fail = 0
            record_captcha_feedback("dc", True, solver_name)

            soup = BeautifulSoup(party_data, "html.parser")
            case_links = soup.find_all("a", href="#")

            cases: list[dict[str, Any]] = []
            for a in case_links:
                onclick = a.get("onclick") or a.get("onClick")
                if onclick and "viewHistory(" in onclick:
                    match = re.search(r"viewHistory\((.*?)\)", onclick)
                    if match:
                        args = [arg.strip(" '\"") for arg in match.group(1).split(",")]
                        if len(args) >= 8:
                            list_case_ref = ""
                            tr = a.find_parent("tr")
                            if tr:
                                tds = tr.find_all("td")
                                if len(tds) >= 2:
                                    ref = tds[1].get_text(strip=True)
                                    if ref and re.search(
                                        r"[A-Za-z].*/\d+/\d{4}", ref
                                    ):
                                        list_case_ref = ref
                            cases.append(
                                {
                                    "case_no": args[0],
                                    "cino": args[1],
                                    "court_code": args[2],
                                    "search_flag": args[4],
                                    "state_code": args[5],
                                    "dist_code": args[6],
                                    "court_complex_code": args[7],
                                    "search_by": (
                                        args[8] if len(args) > 8 else "CScaseType"
                                    ),
                                    "case_label": a.get("aria-label", ""),
                                    "list_case_ref": list_case_ref,
                                }
                            )

            log_captcha_attempt(
                attempt,
                captcha,
                "success",
                "ok",
                total=len(cases),
                solver=solver_name,
            )
            log_summary("ok", len(cases))
            return cases, len(cases), "ok"

        stats["retry_exhausted"] += 1
        log_summary("retryable_error", 0)
        return [], 0, "retryable_error"

    async def fetch_case_detail(
        self,
        state_code: str,
        dist_code: str,
        complex_code: str,
        case: dict[str, Any],
    ) -> str | None:
        payload = (
            f"court_code={case.get('court_code', '')}&state_code={state_code}"
            f"&dist_code={dist_code}"
            f"&court_complex_code={case.get('court_complex_code', '')}"
            f"&case_no={case.get('case_no', '')}&cino={case.get('cino', '')}"
            f"&hideparty=&search_flag={case.get('search_flag', 'CScaseNumber')}"
            f"&search_by={case.get('search_by', 'CScaseType')}"
            f"&ajax_req=true&app_token="
        )

        for attempt in range(1, 4):
            sm = self._pick_detail_sm()
            text = await sm.post_text(
                f"{DC_BASE}/?p=home/viewHistory",
                data=payload,
                headers=DC_HEADERS,
                timeout=DETAIL_TIMEOUT,
                label=f"DC detail cino={case.get('cino')}",
            )
            if not text:
                continue

            try:
                data = json.loads(text)
                return data.get("data_list") or None
            except Exception:
                return text

        return None
