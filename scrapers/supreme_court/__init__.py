"""
⚠️ DEPRECATED — NOT USED FOR PARTY SEARCH ANYMORE.
Party name searches now use Google Sheets via daily_run/sheet_search.py.
Kept for reference only. Use daily_run/supreme_court/ for the 24/7 pipeline.
"""

import asyncio
import logging
from typing import Any

from config import COMMON_HEADERS, HTTP_CLIENT, MAX_CONCURRENT, REQUEST_DELAY
from scrapers.base import BaseScraper
from scrapers.supreme_court.extractor import SCIExtractor
from scrapers.supreme_court.parser import SCIParser
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.supreme_court")


class SupremeCourtScraper(BaseScraper):
    """Full pipeline for Supreme Court case collection."""

    NAME = "supreme_court"
    SOURCE = "SUPREME_COURT"

    def __init__(self) -> None:
        self._sm = SessionManager(
            client_type=HTTP_CLIENT,
            headers={
                **COMMON_HEADERS,
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'accept-language': 'en-US,en;q=0.9',
                'priority': 'u=1, i',
                'referer': 'https://www.sci.gov.in/case-status-party-name/',
                'x-requested-with': 'XMLHttpRequest',
            },
            max_failures=10,
            semaphore_limit=MAX_CONCURRENT,
            request_delay=REQUEST_DELAY,
        )
        self._extractor = SCIExtractor(self._sm)
        self._parser = SCIParser()

    async def close(self) -> None:
        await self._sm.close()

    async def run(self, party_name: str) -> list[dict[str, Any]]:
        from utils.captcha import warm_up_reader
        from config import SCI_YEAR_FROM, SCI_YEAR_TO

        # 1. Warm-up OCR solver
        warm_up_reader()

        # 2. Fetch security tokens once
        logger.info("[SCI] Orchestrating base token lookup...")
        scid, tok_name, tok_value = await self._extractor.get_base_tokens()

        if not scid or not tok_name:
            logger.error("[SCI] Failed to allocate security tokens. Aborting.")
            return []

        # ── PHASE 1: Sequential search (CAPTCHA-bound) ────────────────────────
        # Collect every (case_data, diary_no, diary_year) tuple across all
        # year/status combinations before touching detail endpoints.
        all_cases: list[dict] = []

        for year in range(SCI_YEAR_FROM, SCI_YEAR_TO + 1):
            for status in ['P', 'D']:
                logger.debug(
                    "  [SCI] Searching Year: %d (Party: %s, Status: %s)",
                    year, party_name, status,
                )
                cases, total = await self._extractor.search(
                    party_name=party_name,
                    year=str(year),
                    party_status=status,
                    scid=scid,
                    tok_name=tok_name,
                    tok_value=tok_value,
                )

                if not cases:
                    continue

                logger.info(
                    "    ↳ [%d|%s] Found %d cases",
                    year, status, len(cases)
                )
                all_cases.extend(cases)

        if not all_cases:
            logger.info("Supreme Court: no cases found.")
            return []

        logger.info(
            "[SCI] Phase 1 complete — %d total cases found. "
            "Starting parallel detail fetch...",
            len(all_cases),
        )

        # ── PHASE 2: Fully parallel detail fetch ─────────────────────────────
        # asyncio.gather fires ALL detail requests concurrently.
        # The SessionManager's semaphore (MAX_CONCURRENT) naturally rate-limits
        # in-flight HTTP connections, so we don't flood the server.
        details = await self._fetch_all_details(all_cases)

        logger.info("[SCI] Phase 2 complete — all details fetched.")

        # ── PHASE 3: Parse into schema rows ───────────────────────────────────
        all_rows: list[dict[str, Any]] = []
        for case_data, detail_data in zip(all_cases, details):
            row = self._parser.build_row(
                detail=detail_data,
                fallback=case_data,
                party_name=party_name,
            )
            all_rows.append(row)

        logger.info("Supreme Court total: %d cases.", len(all_rows))
        return all_rows

    async def _fetch_all_details(self, cases: list[dict]) -> list[dict[str, Any]]:
        """
        Fetch all detail tabs for every case in parallel via asyncio.gather.
        Order is preserved — result[i] corresponds to cases[i].
        """
        tasks = [
            self._extractor.fetch_detail(
                diary_no=case["diary_no"],
                diary_year=case["diary_year"],
            )
            for case in cases
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Normalise any unexpected exceptions to empty dicts so parsing doesn't crash
        normalised: list[dict[str, Any]] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # logger.debug(
                #     "    ↳ Detail fetch failed for diary %s/%s: %s",
                #     cases[i]["diary_no"], cases[i]["diary_year"], result,
                # )
                normalised.append({})
            else:
                normalised.append(result)

            if (i + 1) % 50 == 0 or (i + 1) == len(cases):
                logger.info("    ↳ [%d/%d] details fetched", i + 1, len(cases))

        return normalised
