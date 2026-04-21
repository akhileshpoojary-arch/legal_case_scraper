"""
⚠️ DEPRECATED — NOT USED FOR PARTY SEARCH ANYMORE.
Party name searches now use Google Sheets via daily_run/sheet_search.py.
Kept for reference only. Use daily_run/high_court/ for the 24/7 pipeline.

High Court Scraper — facade composing extractor + parser.

Architecture:
  Phase 1 (SEQUENTIAL): for each HC → bench → year → captcha + search
  Phase 2 (CONCURRENT): fetch case detail HTML for all cases in a batch
  Phase 3: parse HTML → 19-column schema rows
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import config
from config import COMMON_HEADERS, HTTP_CLIENT, MAX_CONCURRENT, REQUEST_DELAY
from scrapers.base import BaseScraper
from scrapers.high_court.extractor import HC_HEADERS, HCExtractor
from scrapers.high_court.parser import HCParser, parse_detail_html
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.high_court")


class HighCourtScraper(BaseScraper):
    """Full pipeline for High Court case collection."""

    NAME = "high_court"
    SOURCE = "HIGH_COURT"

    def __init__(self) -> None:
        self._sm = SessionManager(
            client_type=HTTP_CLIENT,
            headers=HC_HEADERS,
            max_failures=10,
            semaphore_limit=MAX_CONCURRENT,
            request_delay=REQUEST_DELAY,
        )
        self._extractor = HCExtractor(self._sm)
        self._parser = HCParser()

    async def close(self) -> None:
        await self._sm.close()

    async def run(self, party_name: str) -> list[dict[str, Any]]:
        """
        Scrape all 25 High Courts for a party name.

        Returns list of 19-column dicts.
        """
        from utils.captcha import warm_up_reader

        # Pre-warm OCR reader (sync)
        warm_up_reader()

        all_rows: list[dict[str, Any]] = []
        total_courts = len(self._extractor.courts)

        for idx, hc in enumerate(self._extractor.courts, 1):
            state_code = hc["state_code"]
            hc_name = hc["name"]

            logger.info(
                "HC [%d/%d] %s", idx, total_courts, hc_name
            )

            # Get benches for this HC
            benches = await self._extractor.get_benches(state_code)
            if not benches:
                logger.debug("    ↳ No benches for %s, skipping", hc_name)
                continue

            logger.debug(
                "    ↳ Benches (%d): %s",
                len(benches),
                [b["bench_name"] for b in benches],
            )

            hc_count = 0

            for bench in benches:
                court_code = bench["court_code"]
                bench_name = bench["bench_name"]
                logger.debug("    ↳ %s", bench_name)

                for year in range(config.HC_YEAR_FROM, config.HC_YEAR_TO + 1):
                    # Phase 1: sequential captcha + search
                    cases, total = await self._extractor.search_cases(
                        state_code, court_code, year, party_name
                    )

                    if not cases:
                        continue

                    logger.info(
                        "    ↳ [%d] Found %d cases",
                        year,
                        len(cases),
                    )

                    # Phase 2: concurrent detail fetch
                    details = await self._fetch_all_details(
                        state_code, court_code, cases
                    )

                    # Phase 3: parse into rows
                    for case_data, detail_html in zip(cases, details):
                        parsed_detail = None
                        if detail_html:
                            parsed_detail = parse_detail_html(detail_html)

                        row = self._parser.build_row(
                            detail=parsed_detail,
                            fallback=case_data,
                            party_name=party_name,
                            court={"name": bench_name, "hc_name": hc_name},
                            bench_name=bench_name,
                            hc_name=hc_name,
                        )
                        all_rows.append(row)
                        hc_count += 1

            logger.info("  ✅ %s: %d cases", hc_name, hc_count)

        logger.info(
            "HC total: %d cases across %d courts", len(all_rows), total_courts
        )
        return all_rows

    async def _fetch_all_details(
        self,
        state_code: str,
        court_code: str,
        cases: list[dict],
    ) -> list[str | None]:
        """Concurrently fetch case detail HTML for all cases in a batch."""

        async def _null() -> None:
            return None

        tasks = []
        for case in cases:
            cino = case.get("cino", "")
            case_no = case.get("case_no", "")
            if cino and case_no:
                tasks.append(
                    self._extractor.fetch_case_detail(
                        state_code, court_code, case_no, cino
                    )
                )
            else:
                tasks.append(_null())

        results = await asyncio.gather(*tasks)
        
        # Log progress for batches
        if len(cases) > 50:
             logger.info("    ↳ [%d/%d] details fetched", len(cases), len(cases))
             
        return results
