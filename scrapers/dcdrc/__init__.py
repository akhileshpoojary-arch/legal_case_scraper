import logging
from typing import Any

from config import HTTP_CLIENT, MAX_CONCURRENT, REQUEST_DELAY, MAX_FAILURES_BEFORE_ROTATE
from scrapers.base import BaseScraper, EJAGRITI_HEADERS
from scrapers.dcdrc.extractor import DCDRCExtractor
from scrapers.dcdrc.parser import DCDRCParser
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.dcdrc")

class DCDRCScraper(BaseScraper):
    """DCDRC scraping pipeline."""

    NAME = "dcdrc"
    SOURCE = "DCDRC"
    _districts_fetched = False

    def __init__(self, session_manager: SessionManager | None = None) -> None:
        self._sm = session_manager or SessionManager(
            client_type=HTTP_CLIENT,
            max_failures=MAX_FAILURES_BEFORE_ROTATE,
            semaphore_limit=MAX_CONCURRENT,
            request_delay=REQUEST_DELAY,
            headers=EJAGRITI_HEADERS,
        )
        self._extractor = DCDRCExtractor(self._sm)
        self._parser = DCDRCParser()

    async def run(self, party_name: str) -> list[dict[str, Any]]:
        # Fetch districts once per app lifecycle
        if not DCDRCScraper._districts_fetched:
            await self._extractor.fetch_districts()
            DCDRCScraper._districts_fetched = True
            
        logger.info("DCDRC: searching for '%s' across %d districts", party_name, len(self._extractor.courts))
        court_results = await self._extractor.search_all_courts(party_name)
        
        all_rows = []
        for court, cases in court_results:
            logger.info("    ↳ Found %d cases in %s", len(cases), court['name'])
            for c in cases:
                fallback = self._parser.build_fallback(c)
                row = self._parser.build_row(c, fallback, party_name, court)
                all_rows.append(row)
        
        return all_rows

    async def close(self) -> None:
        await self._sm.close()
