"""
NCLT Scraper facade — composes NCLTExtractor + NCLTParser.

Two-phase: search all benches → dedup by filing_no → fetch detail → normalize.
"""

from __future__ import annotations

import logging
from typing import Any

from config import HTTP_CLIENT, MAX_CONCURRENT, MAX_FAILURES_BEFORE_ROTATE, REQUEST_DELAY
from scrapers.base import BaseScraper
from scrapers.nclt.extractor import NCLTExtractor, NCLT_HEADERS, NCLT_COOKIES
from scrapers.nclt.parser import NCLTParser
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.nclt")


class NCLTScraper(BaseScraper):
    """Full NCLT pipeline: search → dedup → detail → normalize."""

    NAME = "nclt"
    SOURCE = "NCLT"

    def __init__(self, session_manager: SessionManager | None = None) -> None:
        self._sm = session_manager or SessionManager(
            client_type=HTTP_CLIENT,
            cookies=NCLT_COOKIES,
            headers=NCLT_HEADERS,
            max_failures=MAX_FAILURES_BEFORE_ROTATE,
            semaphore_limit=MAX_CONCURRENT,
            request_delay=REQUEST_DELAY,
        )
        self._extractor = NCLTExtractor(self._sm)
        self._parser = NCLTParser()

    async def run(self, party_name: str) -> list[dict[str, Any]]:
        """Search + detail + parse for a single party."""
        logger.info("NCLT: searching for '%s'", party_name)

        raw_triples = await self._extractor.run_all_tasks(party_name)

        if not raw_triples:
            logger.info("NCLT: no results for '%s'", party_name)
            return []

        all_rows: list[dict[str, Any]] = []
        for idx, (raw_case, task_ctx, detail) in enumerate(raw_triples):
            row = self._parser.build_row(
                detail=detail,
                fallback=raw_case,
                party_name=party_name,
                court={"name": task_ctx["bench_name"], "bench": task_ctx.get("bench_short", "")},
                task_ctx=task_ctx,
            )
            all_rows.append(row)

            if (idx + 1) % 100 == 0:
                logger.info("    ↳ [%d/%d] details parsed", idx + 1, len(raw_triples))

        logger.info("    ↳ [%d/%d] details fully parsed", len(all_rows), len(raw_triples))
        return all_rows

    async def close(self) -> None:
        await self._sm.close()
