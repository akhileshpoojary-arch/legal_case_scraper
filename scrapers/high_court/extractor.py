"""
High Court Extractor — API communication with hcservices.ecourts.gov.in.

Architecture:
  Phase 1 (SEQUENTIAL): captcha solve + search — one at a time to avoid
  server caching same captcha for concurrent workers.
  Phase 2 (CONCURRENT): fetch_case_detail for each result — up to 20 parallel.

"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from typing import Any
from urllib.parse import quote

import config
from config import COMMON_HEADERS
from scrapers.base import BaseExtractor
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.hc.extractor")

HC_BASE       = "https://hcservices.ecourts.gov.in/hcservices"
HC_SEARCH_URL = f"{HC_BASE}/cases_qry/index_qry.php"
HC_DETAIL_URL = f"{HC_BASE}/cases_qry/o_civil_case_history.php"
HC_HOME       = "https://hcservices.ecourts.gov.in/"

HC_HEADERS = {
    **COMMON_HEADERS,
    "content-type":       "application/x-www-form-urlencoded; charset=UTF-8",
    "origin":             "https://hcservices.ecourts.gov.in",
    "referer":            HC_HOME,
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
    "x-requested-with":   "XMLHttpRequest",
}

MAX_CAPTCHA_RETRIES = 30
SEARCH_TIMEOUT      = 45
DETAIL_TIMEOUT      = 30
IMG_TIMEOUT         = 15

# All 25 High Courts
HIGH_COURTS = [
    {"state_code": "13", "name": "Allahabad High Court"},
    {"state_code": "1",  "name": "Bombay High Court"},
    {"state_code": "16", "name": "Calcutta High Court"},
    {"state_code": "6",  "name": "Gauhati High Court"},
    {"state_code": "29", "name": "High Court for Telangana"},
    {"state_code": "2",  "name": "High Court of Andhra Pradesh"},
    {"state_code": "18", "name": "High Court of Chhattisgarh"},
    {"state_code": "26", "name": "High Court of Delhi"},
    {"state_code": "17", "name": "High Court of Gujarat"},
    {"state_code": "5",  "name": "High Court of Himachal Pradesh"},
    {"state_code": "12", "name": "High Court of Jammu and Kashmir"},
    {"state_code": "7",  "name": "High Court of Jharkhand"},
    {"state_code": "3",  "name": "High Court of Karnataka"},
    {"state_code": "4",  "name": "High Court of Kerala"},
    {"state_code": "23", "name": "High Court of Madhya Pradesh"},
    {"state_code": "25", "name": "High Court of Manipur"},
    {"state_code": "21", "name": "High Court of Meghalaya"},
    {"state_code": "11", "name": "High Court of Orissa"},
    {"state_code": "22", "name": "High Court of Punjab and Haryana"},
    {"state_code": "9",  "name": "High Court of Rajasthan"},
    {"state_code": "24", "name": "High Court of Sikkim"},
    {"state_code": "20", "name": "High Court of Tripura"},
    {"state_code": "15", "name": "High Court of Uttarakhand"},
    {"state_code": "10", "name": "Madras High Court"},
    {"state_code": "8",  "name": "Patna High Court"},
]

SESSION_EXPIRED_SIGNALS = [
    "session expired", "your session has", "please login",
    "login required", "invalid session",
]


def _is_session_expired(text: str) -> bool:
    lower = text.lower().strip()
    if lower.startswith("<!doctype") or lower.startswith("<html"):
        if "case history" not in lower and "case details" not in lower:
            return True
    return any(sig in lower for sig in SESSION_EXPIRED_SIGNALS)


def _classify_response(text: str) -> str:
    """
    Classify search response: 'captcha_error' | 'session_expired' | 'no_results' | 'ok'.

    Handles:
      • BOM prefix \\ufeff → strip before json.loads
      • {"Error":"ERROR_VAL"}    → captcha_error   (FIX: was falling through to no_results)
      • {"con":"Invalid Captcha"} → captcha_error
      • {"con":["[]"],"totRecords":0} → no_results
      • {"totRecords":N, "con":[...]} → ok
    """
    if _is_session_expired(text):
        return "session_expired"

    # Strip BOM + whitespace (server always prepends \ufeff)
    clean = text.strip().lstrip("\ufeff").strip()

    if "THERE IS AN ERROR" in clean:
        return "captcha_error"
    if "Record Not Found" in clean:
        return "no_results"

    try:
        data = json.loads(clean)
    except Exception:
        return "captcha_error"

    # ── FIX: {"Error":"ERROR_VAL"} is a captcha/session failure ──────────────
    error_val = str(data.get("Error", "")).strip()
    if error_val and error_val not in ("", "null", "None"):
        logger.debug("  _classify_response: Error field = %r → captcha_error", error_val)
        return "captcha_error"

    # ── con field checks ──────────────────────────────────────────────────────
    con = data.get("con")
    if isinstance(con, str) and "invalid captcha" in con.lower():
        return "captcha_error"
    if isinstance(con, list) and con:
        if str(con[0]).strip().lower() == "invalid captcha":
            return "captcha_error"

    # ── totRecords is ground truth ────────────────────────────────────────────
    try:
        total = int(data.get("totRecords", 0))
    except (TypeError, ValueError):
        total = 0

    return "ok" if total > 0 else "no_results"


def _parse_con_field(data: dict) -> list[dict]:
    """Extract case records from the search response 'con' field."""
    cases: list[dict] = []
    for chunk in data.get("con") or []:
        try:
            parsed = json.loads(chunk) if isinstance(chunk, str) else chunk
            if isinstance(parsed, list):
                cases.extend(parsed)
            elif isinstance(parsed, dict):
                cases.append(parsed)
        except Exception:
            pass
    return cases


class HCExtractor(BaseExtractor):
    """Async extractor for High Court cases — captcha search + HTML detail."""

    SOURCE = "HIGH_COURT"

    def __init__(self, session_manager: SessionManager) -> None:
        self._sm = session_manager

    @property
    def courts(self) -> list[dict[str, Any]]:
        return HIGH_COURTS

    # ── Bench Discovery ───────────────────────────────────────────────────────

    async def get_benches(self, state_code: str) -> list[dict[str, str]]:
        """Fetch bench list for a High Court."""
        for attempt in range(1, 4):
            text = await self._sm.post_text(
                HC_SEARCH_URL,
                data={
                    "action_code": "fillHCBench",
                    "state_code":  state_code,
                    "appFlag":     "web",
                },
                timeout=DETAIL_TIMEOUT,
                label=f"HC benches state={state_code}",
            )
            if text is None:
                await asyncio.sleep(2)
                continue

            if _is_session_expired(text):
                logger.info("  Session expired in get_benches, refreshing...")
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

    # ── Phase 1: Sequential Captcha Download + Search ─────────────────────────
    #
    # CRITICAL: both captcha GET and search POST go through self._sm (same
    # aiohttp ClientSession / cookie jar).  This guarantees the server binds
    # the captcha answer to the same session as the search request.
    # Using a separate requests.Session() for the captcha broke this binding
    # and caused {"Error":"ERROR_VAL"} on every attempt.

    async def _download_captcha_bytes(self) -> bytes | None:
        """
        Download captcha image via the session manager (aiohttp).
        Returns raw PNG bytes, or None on failure.
        """
        url = f"{HC_BASE}/securimage/securimage_show.php?{random.random()}"
        img_headers = {
            **HC_HEADERS,
            "accept":        "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "sec-fetch-dest":"image",
            "sec-fetch-mode":"no-cors",
        }
        content = await self._sm.get_bytes(
            url,
            headers=img_headers,
            timeout=IMG_TIMEOUT,
            label="HC captcha image",
        )
        if not content or len(content) < 200:
            return None
        return content

    async def search_cases(
        self,
        state_code: str,
        court_code: str,
        year: int,
        party_name: str,
    ) -> tuple[list[dict], int]:
        """
        Search for cases — SEQUENTIAL (one captcha at a time).
        Returns (cases_list, total_count).

        ERROR_VAL disambiguation:
          {"Error":"ERROR_VAL"} is returned for TWO distinct reasons:
            1. Wrong captcha / session mismatch  → retry with new captcha
            2. Invalid / empty year (e.g. year < 2010 for most HCs) → no data, stop retrying

          We distinguish them by counting consecutive ERROR_VAL responses.
          If we get ERROR_VAL twice in a row with different captcha values,
          it's almost certainly reason (2) — the year simply has no data.
          A session/captcha problem would occasionally succeed; an invalid
          year never will.

        IMPORTANT: ensure HC_YEAR_FROM = 2016 in config.py.
          The server rejects years before ~2010 with ERROR_VAL regardless
          of captcha correctness, wasting all 30 retries per year.
        """
        from utils.captcha import solve as captcha_solve

        consecutive_error_val = 0   # track back-to-back ERROR_VAL responses

        for attempt in range(1, MAX_CAPTCHA_RETRIES + 1):
            if attempt > 1:
                await asyncio.sleep(min(2 * (1 + random.random()), 6))

            # ── Download captcha via same aiohttp session ─────────────────────
            img_bytes = await self._download_captcha_bytes()
            if not img_bytes:
                logger.debug("  Captcha download failed, retry %d/%d", attempt, MAX_CAPTCHA_RETRIES)
                continue

            # ── Solve captcha in thread (CPU-bound OCR) ───────────────────────
            loop = asyncio.get_event_loop()
            captcha = await loop.run_in_executor(None, captcha_solve, img_bytes)

            if not captcha:
                logger.debug("  Captcha OCR empty, retry %d/%d", attempt, MAX_CAPTCHA_RETRIES)
                continue

            logger.debug(
                "  [%d/%d] [OCR] '%s'  state=%s court=%s year=%d",
                attempt, MAX_CAPTCHA_RETRIES, captcha, state_code, court_code, year,
            )

            # ── Search POST via same session ──────────────────────────────────
            payload = (
                f"court_code={court_code}&state_code={state_code}"
                f"&court_complex_code={court_code}"
                f"&caseStatusSearchType=CSpartyName"
                f"&captcha={captcha}"
                f"&f=Both"
                f"&petres_name={quote(party_name)}"
                f"&rgyear={year}"
            )

            text = await self._sm.post_text(
                f"{HC_SEARCH_URL}?action_code=showRecords",
                data=payload,
                headers={**HC_HEADERS, "accept": "*/*"},
                timeout=SEARCH_TIMEOUT,
                label=f"HC search {state_code}/{court_code}/{year}",
            )

            if text is None:
                logger.debug("  Null response, retry %d/%d", attempt, MAX_CAPTCHA_RETRIES)
                consecutive_error_val = 0
                continue

            # ── Classify ──────────────────────────────────────────────────────
            clean = text.strip().lstrip("\ufeff").strip()
            status = _classify_response(text)
            logger.debug("  Response status: %s  (first 200: %s)", status, text[:200])

            # ── ERROR_VAL: captcha error OR invalid/empty year ────────────────
            if status == "captcha_error":
                # Check specifically for ERROR_VAL (not "Invalid Captcha")
                is_error_val = '"Error":"ERROR_VAL"' in clean or '"Error": "ERROR_VAL"' in clean

                if is_error_val:
                    consecutive_error_val += 1
                    logger.debug(
                        "  ERROR_VAL #%d for year=%d (captcha='%s')"
                        " — may be invalid/empty year, retry %d/%d",
                        consecutive_error_val, year, captcha, attempt, MAX_CAPTCHA_RETRIES,
                    )
                    # After 2 consecutive ERROR_VAL with different captchas →
                    # treat as invalid/empty year, don't waste remaining retries
                    if consecutive_error_val >= 2:
                        logger.debug(
                            "  2× ERROR_VAL for year=%d → treating as no data, skip",
                            year,
                        )
                        return [], 0
                else:
                    # "Invalid Captcha" — reset counter, keep retrying
                    consecutive_error_val = 0
                    logger.debug(
                        "  Wrong captcha '%s', retry %d/%d", captcha, attempt, MAX_CAPTCHA_RETRIES
                    )
                continue

            # Any non-ERROR_VAL response resets the counter
            consecutive_error_val = 0

            if status == "session_expired":
                logger.info("  Session expired, refreshing and retrying...")
                await self._sm.force_refresh(HC_HOME, HC_HEADERS)
                continue

            if status == "no_results":
                logger.debug("  [%s/%s/%d] 0 results", state_code, court_code, year)
                return [], 0

            # ── Parse successful response ─────────────────────────────────────
            data  = json.loads(clean)
            cases = _parse_con_field(data)
            total = int(data.get("totRecords", len(cases)))
            logger.info(
                "  [%s/%s/%d] %d cases (server total=%d)",
                state_code, court_code, year, len(cases), total,
            )
            return cases, total

        logger.warning(
            "  All %d captcha retries exhausted for %s/%s/%d",
            MAX_CAPTCHA_RETRIES, state_code, court_code, year,
        )
        return [], 0

    # ── Phase 2: Concurrent Detail Fetch ─────────────────────────────────────

    async def fetch_case_detail(
        self,
        state_code: str,
        court_code: str,
        case_no: str,
        cino: str,
    ) -> str | None:
        """GET case detail HTML page."""
        payload = (
            f"court_code={court_code}&state_code={state_code}"
            f"&court_complex_code={court_code}"
            f"&case_no={case_no}&cino={cino}&appFlag="
        )

        for attempt in range(1, 4):
            text = await self._sm.post_text(
                HC_DETAIL_URL,
                data=payload,
                timeout=DETAIL_TIMEOUT,
                label=f"HC detail cino={cino}",
            )
            if text and _is_session_expired(text):
                logger.info("  Session expired in detail, refreshing...")
                await self._sm.force_refresh(HC_HOME, HC_HEADERS)
                continue
            return text

        return None

    # BaseExtractor interface stubs — HC uses its own flow
    async def search(self, court: dict[str, Any], party_name: str) -> list[dict]:
        return []

    async def fetch_detail(
        self, court: dict[str, Any], search_result: dict[str, Any]
    ) -> dict | None:
        return None
