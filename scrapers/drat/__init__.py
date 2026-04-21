"""
DRAT Scraper facade — composes DRATExtractor + DRATParser.

Usage:
    scraper = DRATScraper(session_manager)
    rows = await scraper.run("STATE BANK OF INDIA")
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from config import (
    HTTP_CLIENT,
    MAX_CONCURRENT,
    MAX_FAILURES_BEFORE_ROTATE,
    REQUEST_DELAY,
)
from scrapers.base import BaseScraper
from scrapers.drat.extractor import DRATExtractor
from scrapers.drat.parser import DRATParser
from scrapers.drt.extractor import DRTExtractor
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.drat")


class DRATScraper(BaseScraper):
    """Full DRAT scraping pipeline: search all 5 courts → fetch details → normalize."""

    NAME = "drat"
    SOURCE = "DRAT"

    def __init__(self, session_manager: SessionManager | None = None) -> None:
        # DRAT uses same cookies/headers as DRT (same drt.gov.in domain)
        self._sm = session_manager or SessionManager(
            client_type=HTTP_CLIENT,
            cookies=DRTExtractor.COOKIES,
            headers=DRTExtractor.HEADERS,
            max_failures=MAX_FAILURES_BEFORE_ROTATE,
            semaphore_limit=MAX_CONCURRENT,
            request_delay=REQUEST_DELAY,
        )
        self._extractor = DRATExtractor(self._sm)
        self._parser = DRATParser()

    async def run(self, party_name: str) -> list[dict[str, Any]]:
        """
        Search all DRAT courts for party_name, fetch details, return normalized rows.
        """
        logger.info(
            "DRAT: searching %d courts for '%s'",
            len(self._extractor.courts),
            party_name,
        )

        court_results = await self._extractor.search_all_courts(party_name)

        if not court_results:
            logger.info("DRAT: no results for '%s'", party_name)
            return []

        total_search_hits = sum(len(cases) for _, cases in court_results)
        logger.info(
            "DRAT: %d search hits across %d courts",
            total_search_hits,
            len(court_results),
        )

        # Fetch details for ALL courts concurrently
        async def _fetch_court_details(
            court: dict, search_results: list[dict],
        ) -> list[dict[str, Any]]:
            detail_pairs = await self._extractor.fetch_details_batch(court, search_results)
            rows: list[dict[str, Any]] = []
            for sr, detail in detail_pairs:
                fallback = self._parser.build_fallback(sr)
                row = self._parser.build_row(detail, fallback, party_name, court)
                rows.append(row)
            return rows

        court_row_lists = await asyncio.gather(
            *[_fetch_court_details(court, cases) for court, cases in court_results],
            return_exceptions=True,
        )

        all_rows: list[dict[str, Any]] = []
        for result in court_row_lists:
            if isinstance(result, Exception):
                logger.error("DRAT detail batch failed: %s", result)
                continue
            all_rows.extend(result)

        if total_search_hits > 0:
            logger.info("    ↳ [%d/%d] details fully parsed", len(all_rows), total_search_hits)

        return all_rows

    async def close(self) -> None:
        await self._sm.close()
