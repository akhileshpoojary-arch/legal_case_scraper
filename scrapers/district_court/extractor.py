"""
District Court Extractor — API communication with services.ecourts.gov.in.

Architecture:
  Phase 1 (SEQUENTIAL): fetch district/complex/est -> captcha solve -> search
  Phase 2 (CONCURRENT): fetch_case_detail for each result.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from typing import Any
from urllib.parse import quote
from bs4 import BeautifulSoup

from config import COMMON_HEADERS, DC_YEAR_FROM, DC_YEAR_TO
from scrapers.base import BaseExtractor
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.dc.extractor")

DC_BASE       = "https://services.ecourts.gov.in/ecourtindia_v6"
DC_HOME       = "https://services.ecourts.gov.in/"

DC_HEADERS = {
    **COMMON_HEADERS,
    "content-type":       "application/x-www-form-urlencoded; charset=UTF-8",
    "origin":             "https://services.ecourts.gov.in",
    "referer":            DC_HOME,
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
    "x-requested-with":   "XMLHttpRequest",
}

MAX_CAPTCHA_RETRIES = 50
MAX_CAPTCHA_CONSEC_FAILS = 50
SEARCH_TIMEOUT      = 45
DETAIL_TIMEOUT      = 30
IMG_TIMEOUT         = 15

# All States
DC_STATES = [
    {"state_code": "28", "name": "Andaman and Nicobar"},
    {"state_code": "2",  "name": "Andhra Pradesh"},
    {"state_code": "36", "name": "Arunachal Pradesh"},
    {"state_code": "6",  "name": "Assam"},
    {"state_code": "8",  "name": "Bihar"},
    {"state_code": "27", "name": "Chandigarh"},
    {"state_code": "18", "name": "Chhattisgarh"},
    {"state_code": "26", "name": "Delhi"},
    {"state_code": "30", "name": "Goa"},
    {"state_code": "17", "name": "Gujarat"},
    {"state_code": "14", "name": "Haryana"},
    {"state_code": "5",  "name": "Himachal Pradesh"},
    {"state_code": "12", "name": "Jammu and Kashmir"},
    {"state_code": "7",  "name": "Jharkhand"},
    {"state_code": "3",  "name": "Karnataka"},
    {"state_code": "4",  "name": "Kerala"},
    {"state_code": "33", "name": "Ladakh"},
    {"state_code": "37", "name": "Lakshadweep"},
    {"state_code": "23", "name": "Madhya Pradesh"},
    {"state_code": "1",  "name": "Maharashtra"},
    {"state_code": "25", "name": "Manipur"},
    {"state_code": "21", "name": "Meghalaya"},
    {"state_code": "19", "name": "Mizoram"},
    {"state_code": "34", "name": "Nagaland"},
    {"state_code": "11", "name": "Odisha"},
    {"state_code": "35", "name": "Puducherry"},
    {"state_code": "22", "name": "Punjab"},
    {"state_code": "9",  "name": "Rajasthan"},
    {"state_code": "24", "name": "Sikkim"},
    {"state_code": "10", "name": "Tamil Nadu"},
    {"state_code": "29", "name": "Telangana"},
    {"state_code": "38", "name": "The Dadra And Nagar Haveli And Daman And Diu"},
    {"state_code": "20", "name": "Tripura"},
    {"state_code": "15", "name": "Uttarakhand"},
    {"state_code": "13", "name": "Uttar Pradesh"},
    {"state_code": "16", "name": "West Bengal"},
]

SESSION_EXPIRED_SIGNALS = [
    "session expired", "your session has", "please login",
    "login required", "invalid session", "Invalid Captcha"
]

def _is_session_expired(text: str) -> bool:
    lower = text.lower().strip()
    if lower.startswith("<!doctype") or lower.startswith("<html"):
        if "case history" not in lower and "case details" not in lower:
            return True
    return any(sig in lower for sig in SESSION_EXPIRED_SIGNALS)


class DCExtractor(BaseExtractor):
    """Async extractor for District Court cases."""

    SOURCE = "DISTRICT_COURT"

    def __init__(self, session_manager: SessionManager) -> None:
        self._sm = session_manager

    @property
    def states(self) -> list[dict[str, str]]:
        return DC_STATES

    # ── Discovery ───────────────────────────────────────────────────────

    async def get_districts(self, state_code: str) -> list[dict[str, str]]:
        """Fetch district list for a state."""
        for attempt in range(1, 4):
            text = await self._sm.post_text(
                f"{DC_BASE}/?p=casestatus/fillDistrict",
                data={"state_code": state_code, "ajax_req": "true", "app_token": ""},
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
                districts = []
                for opt in soup.find_all("option"):
                    val = opt.get("value")
                    if val and val != "0":
                        districts.append({"dist_code": val, "dist_name": opt.get_text(strip=True)})
                return districts
            except Exception:
                await asyncio.sleep(2)
        return []

    async def get_complexes(self, state_code: str, dist_code: str) -> list[dict[str, str]]:
        """Fetch complex list for a district."""
        for attempt in range(1, 4):
            text = await self._sm.post_text(
                f"{DC_BASE}/?p=casestatus/fillcomplex",
                data={"state_code": state_code, "dist_code": dist_code, "ajax_req": "true", "app_token": ""},
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
                complexes = []
                for opt in soup.find_all("option"):
                    val = opt.get("value")
                    if val:
                        complexes.append({"complex_code": val, "complex_name": opt.get_text(strip=True)})
                return complexes
            except Exception:
                await asyncio.sleep(2)
        return []

    async def get_establishments(self, state_code: str, dist_code: str, complex_code: str) -> list[dict[str, str]]:
        """Fetch establishments list for a complex."""
        for attempt in range(1, 4):
            # The complex code has @ in it sometimes, pass safely.
            # actually we might only need to pass the base code if it has @? Let"s just pass it raw.
            # `court_complex_code=1030210` in cURL even though the <option> value was `1030210@22@Y`.
            # We"ll split by @ and use the first part.
            cplx_base = complex_code.split("@")[0] if "@" in complex_code else complex_code

            text = await self._sm.post_text(
                f"{DC_BASE}/?p=casestatus/fillCourtEstablishment",
                data={"state_code": state_code, "dist_code": dist_code, "court_complex_code": cplx_base, "ajax_req": "true", "app_token": ""},
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
                establishments = []
                for opt in soup.find_all("option"):
                    val = opt.get("value")
                    if val:
                        establishments.append({"est_code": val, "est_name": opt.get_text(strip=True)})
                return establishments
            except Exception:
                await asyncio.sleep(2)
        return []

    # ── Phase 1: Sequential Captcha Download + Search ─────────────────────────

    async def _download_captcha_bytes(self) -> bytes | None:
        """Download captcha image via the session manager (aiohttp)."""
        url = f"{DC_BASE}/vendor/securimage/securimage_show.php?{random.random()}"
        img_headers = {
            **DC_HEADERS,
            "accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "sec-fetch-dest": "image",
            "sec-fetch-mode": "no-cors",
        }
        content = await self._sm.get_bytes(
            url,
            headers=img_headers,
            timeout=IMG_TIMEOUT,
            label="DC captcha image",
        )
        if not content or len(content) < 200:
            return None
        return content

    async def search_cases(
        self,
        state_code: str,
        dist_code: str,
        complex_code: str,
        est_code: str,
        year: int,
        party_name: str,
    ) -> tuple[list[dict], int]:
        from utils.captcha import solve as captcha_solve

        cplx_base = complex_code.split("@")[0] if "@" in complex_code else complex_code

        # First, set data via casestatus/set_data
        set_data_payload = {
            "complex_code": complex_code,
            "selected_state_code": state_code,
            "selected_dist_code": dist_code,
            "selected_est_code": est_code,
            "ajax_req": "true",
            "app_token": ""
        }
        await self._sm.post_text(
            f"{DC_BASE}/?p=casestatus/set_data",
            data=set_data_payload,
            timeout=SEARCH_TIMEOUT,
            label="DC set_data",
        )

        consec_fail = 0

        for attempt in range(1, MAX_CAPTCHA_RETRIES + 1):
            if attempt > 1:
                await asyncio.sleep(min(2 * (1 + random.random()), 6))

            img_bytes = await self._download_captcha_bytes()
            if not img_bytes:
                consec_fail += 1
                logger.debug(
                    "  [CAPTCHA] attempt %d → download failed",
                    attempt,
                )
                if consec_fail >= MAX_CAPTCHA_CONSEC_FAILS:
                    logger.warning(
                        "  [CAPTCHA] %d consecutive failures, skipping %s/%s/%d",
                        consec_fail, state_code, dist_code, year,
                    )
                    return [], 0
                continue

            loop = asyncio.get_event_loop()
            captcha = await loop.run_in_executor(None, captcha_solve, img_bytes, 6, "dc")

            if not captcha:
                consec_fail += 1
                logger.debug(
                    "  [CAPTCHA] attempt %d → OCR empty",
                    attempt,
                )
                if consec_fail >= MAX_CAPTCHA_CONSEC_FAILS:
                    logger.warning(
                        "  [CAPTCHA] %d consecutive failures, skipping %s/%s/%d",
                        consec_fail, state_code, dist_code, year,
                    )
                    return [], 0
                continue

            payload = (
                f"petres_name={quote(party_name)}"
                f"&rgyearP={year}"
                f"&case_status=Both"
                f"&fcaptcha_code={captcha}"
                f"&state_code={state_code}"
                f"&dist_code={dist_code}"
                f"&court_complex_code={cplx_base}"
                f"&est_code={est_code}"
                f"&ajax_req=true"
                f"&app_token="
            )

            text = await self._sm.post_text(
                f"{DC_BASE}/?p=casestatus/submitPartyName",
                data=payload,
                headers={**DC_HEADERS, "accept": "*/*"},
                timeout=SEARCH_TIMEOUT,
                label=f"DC search {state_code}/{dist_code}/{year}",
            )

            if not text:
                consec_fail += 1
                logger.debug(
                    "  [CAPTCHA] attempt %d → %s → NO RESPONSE",
                    attempt, captcha,
                )
                if consec_fail >= MAX_CAPTCHA_CONSEC_FAILS:
                    logger.warning(
                        "  [CAPTCHA] %d consecutive failures, skipping %s/%s/%d",
                        consec_fail, state_code, dist_code, year,
                    )
                    return [], 0
                continue

            try:
                data = json.loads(text)
            except Exception:
                consec_fail += 1
                logger.debug(
                    "  [CAPTCHA] attempt %d → %s → PARSE ERROR",
                    attempt, captcha,
                )
                continue

            if data.get("status") == "invalid captcha" or "invalid captcha" in str(data).lower() or "<div class=\"alert alert-danger" in text:
                consec_fail += 1
                logger.debug(
                    "  [CAPTCHA] attempt %d → %s → FAIL (invalid captcha)",
                    attempt, captcha,
                )
                if consec_fail >= MAX_CAPTCHA_CONSEC_FAILS:
                    logger.warning(
                        "  [CAPTCHA] %d consecutive failures, skipping %s/%s/%d",
                        consec_fail, state_code, dist_code, year,
                    )
                    return [], 0
                continue

            # Captcha was accepted — reset failures
            consec_fail = 0
            logger.debug(
                "  [CAPTCHA] attempt %d → %s → SUCCESS",
                attempt, captcha,
            )

            party_data = data.get("party_data", "")
            if "Total number of cases : 0" in party_data or not party_data:
                return [], 0

            soup = BeautifulSoup(party_data, "html.parser")
            case_links = soup.find_all("a", href="#")

            cases = []
            for a in case_links:
                onclick = a.get("onclick") or a.get("onClick")
                if onclick and "viewHistory(" in onclick:
                    match = re.search(r"viewHistory\((.*?)\)", onclick)
                    if match:
                        args = [arg.strip(" '\"") for arg in match.group(1).split(",")]
                        if len(args) >= 8:
                            cases.append({
                                "case_no": args[0],
                                "cino": args[1],
                                "court_code": args[2],
                                "search_flag": args[4],
                                "state_code": args[5],
                                "dist_code": args[6],
                                "court_complex_code": args[7],
                                "search_by": args[8] if len(args) > 8 else "CSpartyName",
                                "case_label": a.get("aria-label", "")
                            })

            return cases, len(cases)

        logger.warning(
            "  All %d captcha retries exhausted for %s/%s/%s/%d",
            MAX_CAPTCHA_RETRIES, state_code, dist_code, cplx_base, year,
        )
        return [], 0

    # ── Phase 2: Concurrent Detail Fetch ─────────────────────────────────────

    async def fetch_case_detail(
        self,
        state_code: str,
        dist_code: str,
        complex_code: str,
        case: dict,
    ) -> str | None:
        """GET case detail HTML page."""
        payload = (
            f"court_code={case.get('court_code', '')}&state_code={state_code}"
            f"&dist_code={dist_code}"
            f"&court_complex_code={case.get('court_complex_code', '')}"
            f"&case_no={case.get('case_no', '')}&cino={case.get('cino', '')}"
            f"&hideparty=&search_flag={case.get('search_flag', 'CScaseNumber')}"
            f"&search_by={case.get('search_by', 'CSpartyName')}"
            f"&ajax_req=true&app_token="
        )

        for attempt in range(1, 4):
            text = await self._sm.post_text(
                f"{DC_BASE}/?p=home/viewHistory",
                data=payload,
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

    # BaseExtractor interface stubs
    async def search(self, court: dict[str, Any], party_name: str) -> list[dict]:
        return []

    async def fetch_detail(
        self, court: dict[str, Any], search_result: dict[str, Any]
    ) -> dict | None:
        return None
