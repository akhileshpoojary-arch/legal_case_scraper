
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import urllib.parse
from typing import Any

from config import COMMON_HEADERS
from daily_run.config import HC_MAX_DETAIL_RETRIES, TESTING
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.daily_run.hc.extractor")

HC_BASE = "https://hcservices.ecourts.gov.in/hcservices"
HC_SEARCH_URL = f"{HC_BASE}/cases_qry/index_qry.php"
HC_DETAIL_URL = f"{HC_BASE}/cases_qry/o_civil_case_history.php"
HC_HOME = "https://hcservices.ecourts.gov.in/"

HC_HEADERS = {
    **COMMON_HEADERS,
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://hcservices.ecourts.gov.in",
    "referer": HC_HOME,
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "x-requested-with": "XMLHttpRequest",
}

HC_DETAIL_HEADERS = {
    **HC_HEADERS,
    "accept": "text/html, */*;q=0.1",
    "referer": HC_SEARCH_URL,
}

MAX_CAPTCHA_RETRIES = 30
SEARCH_TIMEOUT = 45
DETAIL_TIMEOUT = 30
IMG_TIMEOUT = 15

HIGH_COURTS = [
    {"state_code": "13", "name": "Allahabad High Court"},
    {"state_code": "1", "name": "Bombay High Court"},
    {"state_code": "16", "name": "Calcutta High Court"},
    {"state_code": "6", "name": "Gauhati High Court"},
    {"state_code": "29", "name": "High Court for Telangana"},
    {"state_code": "2", "name": "High Court of Andhra Pradesh"},
    {"state_code": "18", "name": "High Court of Chhattisgarh"},
    {"state_code": "26", "name": "High Court of Delhi"},
    {"state_code": "17", "name": "High Court of Gujarat"},
    {"state_code": "5", "name": "High Court of Himachal Pradesh"},
    {"state_code": "12", "name": "High Court of Jammu and Kashmir"},
    {"state_code": "7", "name": "High Court of Jharkhand"},
    {"state_code": "3", "name": "High Court of Karnataka"},
    {"state_code": "4", "name": "High Court of Kerala"},
    {"state_code": "23", "name": "High Court of Madhya Pradesh"},
    {"state_code": "25", "name": "High Court of Manipur"},
    {"state_code": "21", "name": "High Court of Meghalaya"},
    {"state_code": "11", "name": "High Court of Orissa"},
    {"state_code": "22", "name": "High Court of Punjab and Haryana"},
    {"state_code": "9", "name": "High Court of Rajasthan"},
    {"state_code": "24", "name": "High Court of Sikkim"},
    {"state_code": "20", "name": "High Court of Tripura"},
    {"state_code": "15", "name": "High Court of Uttarakhand"},
    {"state_code": "10", "name": "Madras High Court"},
    {"state_code": "8", "name": "Patna High Court"},
]

SESSION_EXPIRED_SIGNALS = [
    "session expired",
    "your session has",
    "please login",
    "login required",
    "invalid session",
]

def _is_session_expired(text: str) -> bool:
    lower = text.lower().strip()
    if lower.startswith("<!doctype") or lower.startswith("<html"):
        if "case history" not in lower and "case details" not in lower:
            return True
    return any(sig in lower for sig in SESSION_EXPIRED_SIGNALS)

def _classify_response(text: str) -> str:
    if _is_session_expired(text):
        return "session_expired"

    clean = text.strip().lstrip("\ufeff").strip()
    if "THERE IS AN ERROR" in clean:
        return "captcha_error"
    if "Record Not Found" in clean:
        return "no_results"

    try:
        data = json.loads(clean)
    except Exception:
        return "captcha_error"

    error_val = str(data.get("Error", "")).strip()
    if error_val and error_val not in ("", "null", "None"):
        return "captcha_error"

    con = data.get("con")
    if isinstance(con, str) and "invalid captcha" in con.lower():
        return "captcha_error"
    if isinstance(con, list) and con:
        if str(con[0]).strip().lower() == "invalid captcha":
            return "captcha_error"

    try:
        total = int(data.get("totRecords", 0))
    except (TypeError, ValueError):
        total = 0

    return "ok" if total > 0 else "no_results"

def _parse_con_field(data: dict) -> list[dict]:
    cases: list[dict] = []
    con_field = data.get("con")
    if con_field is None:
        return cases
    if isinstance(con_field, (str, dict)):
        con_items = [con_field]
    elif isinstance(con_field, list):
        con_items = con_field
    else:
        return cases

    for chunk in con_items:
        try:
            parsed = json.loads(chunk) if isinstance(chunk, str) else chunk
            if isinstance(parsed, list):
                cases.extend(parsed)
            elif isinstance(parsed, dict):
                cases.append(parsed)
        except Exception:
            pass
    return cases

def _is_detail_html(text: str) -> bool:
    lower = text.lower()
    # Case history pages include one of these markers in successful responses.
    return any(
        marker in lower
        for marker in ("case history", "case details", "petitioner", "respondent")
    )

class HCContinuousExtractor:

    SOURCE = "HIGH_COURT"

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
    def courts(self) -> list[dict[str, str]]:
        return HIGH_COURTS

    async def get_benches(self, state_code: str) -> list[dict[str, str]]:
        for attempt in range(1, 5):
            text = await self._sm.post_text(
                HC_SEARCH_URL,
                data={
                    "action_code": "fillHCBench",
                    "state_code": state_code,
                    "appFlag": "web",
                },
                headers=HC_HEADERS,
                timeout=DETAIL_TIMEOUT,
                label=f"HC benches state={state_code}",
            )
            if text is None:
                await asyncio.sleep(2)
                continue

            if _is_session_expired(text):
                await self._sm.force_refresh(HC_HOME, HC_HEADERS)
                continue

            benches: list[dict[str, str]] = []
            for part in text.strip().split("#"):
                if "~" not in part:
                    continue
                code, name = part.split("~", 1)
                code = code.strip().lstrip("\ufeff")
                if code == "0":
                    continue
                benches.append({"court_code": code, "bench_name": name.strip()})
            return benches
        return []

    async def get_case_types(
        self, state_code: str, court_code: str
    ) -> list[dict[str, str]]:
        for attempt in range(1, 4):
            text = await self._sm.post_text(
                HC_SEARCH_URL,
                data={
                    "action_code": "fillCaseType",
                    "state_code": state_code,
                    "court_code": court_code,
                    "appFlag": "web",
                },
                headers=HC_HEADERS,
                timeout=DETAIL_TIMEOUT,
                label=f"HC case_types state={state_code}/{court_code}",
            )
            if TESTING:
                logger.info(
                    "[HC] Discovery: Fetching case types for court_code=%s", court_code
                )

            if not text:
                await asyncio.sleep(2)
                continue

            if _is_session_expired(text):
                await self._sm.force_refresh(HC_HOME, HC_HEADERS)
                continue

            types: list[dict[str, str]] = []
            for part in text.strip().split("#"):
                if "~" not in part:
                    continue
                code, name = part.split("~", 1)
                code = code.strip().lstrip("\ufeff")
                if code == "0":
                    continue
                types.append({"type_code": code, "type_name": name.strip()})

            if TESTING:
                logger.info("[HC] Discovery: Found %d case types.", len(types))
            return types
        return []

    async def _download_captcha_bytes(self) -> bytes | None:
        url = f"{HC_BASE}/securimage/securimage_show.php?{random.random()}"
        img_headers = {
            **HC_HEADERS,
            "accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "sec-fetch-dest": "image",
            "sec-fetch-mode": "no-cors",
        }
        content = await self._sm.get_bytes(
            url, headers=img_headers, timeout=IMG_TIMEOUT, label="HC captcha image"
        )
        if not content or len(content) < 200:
            return None
        return content

    async def search_cases_by_type(
        self,
        state_code: str,
        court_code: str,
        year: int,
        case_type_code: str,
        case_status: str,
    ) -> tuple[list[dict], int, str]:
        from utils.captcha import solve_async as captcha_solve_async

        consecutive_error_val = 0
        stats = {
            "attempts": 0,
            "captcha_image_missing": 0,
            "captcha_empty": 0,
            "captcha_solved": 0,
            "captcha_rejected": 0,
            "transport_failures": 0,
            "session_refresh": 0,
            "no_records": 0,
            "retry_exhausted": 0,
            "error_val_cutoff": 0,
        }

        def log_captcha_attempt(
            attempt_no: int,
            prediction: str | None,
            response: str,
        ) -> None:
            logger.info(
                "[HC] attempt:%d prediction:%s response:%s",
                attempt_no,
                prediction if prediction else "-",
                response,
            )

        def log_summary(state: str, total: int = 0) -> None:
            logger.info(
                "[HC] Search summary: state=%s court=%s type=%s year=%d status=%s result=%s total=%d attempts=%d solved=%d rejected=%d empty=%d no_image=%d transport=%d",
                state_code,
                court_code,
                case_type_code,
                year,
                case_status,
                state,
                total,
                stats["attempts"],
                stats["captcha_solved"],
                stats["captcha_rejected"],
                stats["captcha_empty"],
                stats["captcha_image_missing"],
                stats["transport_failures"],
            )

        for attempt in range(1, MAX_CAPTCHA_RETRIES + 1):
            stats["attempts"] += 1
            if attempt > 1:
                await asyncio.sleep(min(2 * (1 + random.random()), 6))

            img_bytes = await self._download_captcha_bytes()
            if not img_bytes:
                stats["captcha_image_missing"] += 1
                log_captcha_attempt(attempt, None, "fail")
                continue

            captcha = await captcha_solve_async(img_bytes, 6, "hc")

            if not captcha:
                stats["captcha_empty"] += 1
                log_captcha_attempt(attempt, None, "fail")
                continue

            stats["captcha_solved"] += 1

            payload = {
                "court_code": court_code,
                "state_code": state_code,
                "court_complex_code": court_code,
                "caseStatusSearchType": "CScaseType",
                "captcha": captcha,
                "case_type": case_type_code,
                "search_year": str(year),
                "f": case_status,
            }
            encoded_payload = urllib.parse.urlencode(payload)

            text = await self._sm.post_text(
                f"{HC_SEARCH_URL}?action_code=showRecords",
                data=encoded_payload,
                headers={**HC_HEADERS, "accept": "*/*"},
                timeout=SEARCH_TIMEOUT,
                label=f"HC search {state_code}/{court_code}/{case_type_code}/{year}",
            )

            if not text:
                consecutive_error_val = 0
                stats["transport_failures"] += 1
                log_captcha_attempt(attempt, captcha, "fail")
                continue

            clean = text.strip().lstrip("\ufeff").strip()
            status = _classify_response(text)

            if status == "captcha_error":
                stats["captcha_rejected"] += 1
                log_captcha_attempt(attempt, captcha, "fail")
                is_error_val = (
                    '"Error":"ERROR_VAL"' in clean or '"Error": "ERROR_VAL"' in clean
                )
                if is_error_val:
                    consecutive_error_val += 1
                    if consecutive_error_val >= 2:
                        stats["error_val_cutoff"] += 1
                        log_summary("no_results", 0)
                        return [], 0, "no_results"
                else:
                    consecutive_error_val = 0
                continue

            consecutive_error_val = 0

            if status == "session_expired":
                stats["session_refresh"] += 1
                log_captcha_attempt(attempt, captcha, "fail")
                await self._sm.force_refresh(HC_HOME, HC_HEADERS)
                continue

            if status == "no_results":
                stats["no_records"] += 1
                log_captcha_attempt(attempt, captcha, "success")
                log_summary("no_results", 0)
                return [], 0, "no_results"

            from utils.captcha import save_captcha_image

            save_captcha_image(img_bytes, captcha, "hc")

            try:
                data = json.loads(clean)
            except Exception:
                stats["transport_failures"] += 1
                log_captcha_attempt(attempt, captcha, "fail")
                continue
            cases = _parse_con_field(data)
            total = int(data.get("totRecords", len(cases)))
            log_captcha_attempt(attempt, captcha, "success")
            log_summary("ok", total)
            return cases, total, "ok"

        stats["retry_exhausted"] += 1
        log_summary("retryable_error", 0)
        return [], 0, "retryable_error"

    async def fetch_case_detail(
        self,
        state_code: str,
        court_code: str,
        case_no: str,
        cino: str,
    ) -> str | None:
        payload = {
            "court_code": court_code,
            "state_code": state_code,
            "court_complex_code": court_code,
            "case_no": case_no,
            "cino": cino,
            "appFlag": "",
        }

        for attempt in range(1, max(1, int(HC_MAX_DETAIL_RETRIES)) + 1):
            sm = self._pick_detail_sm()
            text = await sm.post_text(
                HC_DETAIL_URL,
                data=payload,
                headers=HC_DETAIL_HEADERS,
                timeout=DETAIL_TIMEOUT,
                label=f"HC detail cino={cino}",
            )
            if not text:
                reason = sm.consume_last_failure_reason() or "unknown_failure"
                # logger.warning(
                #     "[HC] Detail fetch failed: cino=%s attempt=%d reason=%s",
                #     cino,
                #     attempt,
                #     reason,
                # )
                if reason in {"timeout", "connection_error", "proxy_error"}:
                    await asyncio.sleep(min(0.8 * attempt, 3.0) + random.uniform(0.0, 0.4))
                continue

            if _is_session_expired(text):
                logger.warning(
                    "[HC] Detail fetch session-expired: cino=%s attempt=%d; refreshing session",
                    cino,
                    attempt,
                )
                await sm.force_refresh(HC_HOME, HC_HEADERS)
                continue

            if not _is_detail_html(text):
                logger.warning(
                    "[HC] Detail fetch unexpected payload: cino=%s attempt=%d len=%d",
                    cino,
                    attempt,
                    len(text),
                )
                await asyncio.sleep(min(0.5 * attempt, 2.0) + random.uniform(0.0, 0.3))
                continue

            if "THERE IS AN SQL ERROR" in text.upper():
                return "SQL_ERROR_SKIP"
            return text
        return None
