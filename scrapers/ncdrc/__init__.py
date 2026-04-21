import logging
from typing import Any

from config import HTTP_CLIENT, MAX_CONCURRENT, REQUEST_DELAY, MAX_FAILURES_BEFORE_ROTATE
from scrapers.base import BaseScraper, EJAGRITI_HEADERS
from scrapers.ncdrc.extractor import NCDRCExtractor
from scrapers.ncdrc.parser import NCDRCParser
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.ncdrc")

class NCDRCScraper(BaseScraper):
    """NCDRC scraping pipeline."""

    NAME = "ncdrc"
    SOURCE = "NCDRC"

    def __init__(self, session_manager: SessionManager | None = None) -> None:
        self._sm = session_manager or SessionManager(
            client_type=HTTP_CLIENT,
            max_failures=MAX_FAILURES_BEFORE_ROTATE,
            semaphore_limit=MAX_CONCURRENT,
            request_delay=REQUEST_DELAY,
            headers=EJAGRITI_HEADERS,
        )
        self._extractor = NCDRCExtractor(self._sm)
        self._parser = NCDRCParser()

    async def run(self, party_name: str) -> list[dict[str, Any]]:
        logger.info("NCDRC: searching for '%s'", party_name)
        court_results = await self._extractor.search_all_courts(party_name)
        
        all_rows = []
        for court, cases in court_results:
            logger.info("    ↳ Found %d cases", len(cases))
            for c in cases:
                fallback = self._parser.build_fallback(c)
                row = self._parser.build_row(c, fallback, party_name, court)
                all_rows.append(row)
        
        return all_rows

    async def close(self) -> None:
        await self._sm.close()
