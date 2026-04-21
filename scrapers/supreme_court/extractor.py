import asyncio
import logging
import random
from typing import Any
from bs4 import BeautifulSoup

from config import HTTP_CLIENT
from utils.session_utils import SessionManager
from scrapers.base import BaseExtractor

logger = logging.getLogger("legal_scraper.supreme_court.extractor")

SCI_BASE = "https://www.sci.gov.in"
SCI_HOME = f"{SCI_BASE}/case-status-party-name/"
SCI_AJAX = f"{SCI_BASE}/wp-admin/admin-ajax.php"
MAX_CAPTCHA_RETRIES = 5
IMG_TIMEOUT = 10.0
DETAIL_TIMEOUT = 30

SCI_TABS = [
    "",
    "listing_dates",
    "interlocutory_application_documents",
    "court_fees",
    "notices",
    "defects",
    "judgement_orders",
    "earlier_court_details",
    "similarities",
]

class SCIExtractor(BaseExtractor):
    """Async extractor for Supreme Court cases — sequential captcha search + parallel HTML detail."""

    SOURCE = "SUPREME_COURT"

    def __init__(self, session_manager: SessionManager) -> None:
        self._sm = session_manager

    # ── Token & Captcha Lifecycle ──────────────────────────────────────────────

    async def get_base_tokens(self) -> tuple[str | None, str | None, str | None]:
        """Fetch base page to extract scid and the dynamic tok_ field."""
        text = await self._sm.get_text(SCI_HOME, label="SCI Base Page")
        if not text:
            return None, None, None

        soup = BeautifulSoup(text, 'html.parser')
        scid_input = soup.find('input', {'name': 'scid'})
        tok_input = soup.find('input', id=lambda x: x and x.startswith('tok_'))

        if not scid_input or not tok_input:
            logger.warning("  [SCI] Could not locate security tokens on base page.")
            return None, None, None

        scid = scid_input.get('value')
        tok_name = tok_input.get('name')
        tok_value = tok_input.get('value')

        return scid, tok_name, tok_value

    async def _download_captcha_bytes(self, scid: str) -> bytes | None:
        """Download captcha image via the session manager (aiohttp)."""
        url = f"{SCI_BASE}/?_siwp_captcha&id={scid}&rand={random.random()}"
        content = await self._sm.get_bytes(
            url,
            timeout=IMG_TIMEOUT,
            label="SCI captcha image",
        )
        if not content or len(content) < 200:
            return None
        return content

    # ── Phase 1: Sequential Captcha Download + Search ─────────────────────────

    async def search(
        self,
        party_name: str,
        year: str,
        party_status: str,
        scid: str,
        tok_name: str,
        tok_value: str,
    ) -> tuple[list[dict], int]:
        """Search cases for a single year using the initialized token."""
        from utils.captcha import solve as captcha_solve
        loop = asyncio.get_event_loop()

        for attempt in range(1, MAX_CAPTCHA_RETRIES + 1):
            # 1. Download Captcha
            img_bytes = await self._download_captcha_bytes(scid)
            if not img_bytes:
                logger.debug("  Captcha download failed, retry %d/%d", attempt, MAX_CAPTCHA_RETRIES)
                continue

            # 2. Solve in thread (OCR mathematical evaluation allows + and -)
            c_text = await loop.run_in_executor(None, captcha_solve, img_bytes, 6, "sci")

            ocr_val = c_text
            if c_text and any(op in c_text for op in ['+', '-']):
                try:
                    ocr_val = str(eval(c_text, {"__builtins__": None}, {}))
                except Exception:
                    ocr_val = c_text
            
            trial_values: list[str] = []
            if ocr_val and ocr_val.isdigit():
                trial_values.append(ocr_val)
            
            # Add brute-force range 0-20
            for n in range(21):
                val = str(n)
                if val not in trial_values:
                    trial_values.append(val)

            for trial_idx, captcha_val in enumerate(trial_values):
                # 3. Submit search parameter
                params = {
                    'party_type': 'any',
                    'party_name': party_name,
                    'year': year,
                    'party_status': party_status,
                    'scid': scid,
                    tok_name: tok_value,
                    'siwp_captcha_value': captcha_val,
                    'es_ajax_request': '1',
                    'submit': 'Search',
                    'action': 'get_case_status_party_name',
                    'language': 'en',
                }

                resp = await self._sm.get(
                    SCI_AJAX,
                    params=params,
                    timeout=DETAIL_TIMEOUT,
                    label=f"SCI Search ({year}) trial={captcha_val}",
                )

                if not resp:
                    logger.warning("  SCI Search transport failure at trial %s.", captcha_val)
                    # Break trial loop to retry with fresh tokens/image in outer loop
                    break

                success = resp.get("success", False)
                data = resp.get("data", "")

                if not success:
                    if isinstance(data, str) and "incorrect" in data.lower():
                        # Only continue trial loop if specifically captcha incorrect
                        continue
                    if isinstance(data, str) and "no records" in data.lower():
                        return [], 0
                    logger.debug("  Search non-success failure: %s", data)
                    return [], 0
                
                # Success! Extract table rows.
                results_html = data.get("resultsHtml", "")
                return self._extract_table_rows(results_html), data.get("total_records", 0)

        logger.warning("  Exceeded %d captcha attempts for SCI.", MAX_CAPTCHA_RETRIES)
        return [], 0

    def _extract_table_rows(self, html: str) -> list[dict]:
        """Extract table rows into structured dictionaries."""
        soup = BeautifulSoup(html, 'html.parser')
        rows = []
        for tr in soup.find_all('tr'):
            if not tr.has_attr('data-diary-no'):
                continue

            diary_no = tr.get('data-diary-no')
            diary_year = tr.get('data-diary-year')

            tds = tr.find_all('td')
            if len(tds) < 6:
                continue

            case_number = tds[2].get_text(strip=True, separator=" ")
            petitioner = tds[3].get_text(separator=" ", strip=True)
            respondent = tds[4].get_text(separator=" ", strip=True)
            status = tds[5].get_text(strip=True)

            rows.append({
                "diary_no": diary_no,
                "diary_year": diary_year,
                "case_number": case_number,
                "petitioner": petitioner,
                "respondent": respondent,
                "status": status,
            })

        return rows

    # ── Phase 2: Concurrent Detail Fetching ───────────────────────────────────

    async def fetch_detail(
        self,
        diary_no: str,
        diary_year: str,
    ) -> dict[str, str]:
        """
        Fetch all detail tabs for a specific case concurrently.
        """
        base_params = {
            'diary_no': diary_no,
            'diary_year': diary_year,
            'action': 'get_case_details',
            'es_ajax_request': '1',
            'language': 'en',
        }

        tab_data = {}
        for tab in SCI_TABS:
            # We don't fetch all concurrently inside the loop to avoid overwhelming
            # the server for a single case, but they are fetched concurrently ACROSS
            # multiple cases via the orchestrator. Let's do a simple gather here too.
            pass

        async def fetch_tab(tab_name: str) -> tuple[str, str]:
            params = {**base_params, 'tab_name': tab_name}
            resp = await self._sm.get(
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

        for tab_name, html_data in results:
            tab_data[tab_name] = html_data

        logger.debug("    [SCI] Fetched details for Diary: %s/%s", diary_no, diary_year)
        return tab_data
