"""
On-demand party search over the locally-collected DC / HC / SC data.

The 24/7 collector records every case it writes into a local SQLite index
(see ``daily_run/case_index.py``). These scrapers query that index instead of
opening every paginated Google Sheet, so a search is near-instant and uses
**zero Google Sheets reads** — which is what previously exhausted the API quota.

FTS5 narrows the index to a small candidate set on the distinctive name tokens;
the precise, abbreviation-aware matchers already used elsewhere
(``utils/party_search`` for individuals, ``utils/company_augment`` for
companies) then decide the final matches, so results stay identical to the old
sheet-scanning behaviour.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import config
from daily_run.case_index import CaseIndex
from scrapers.base import BaseScraper
from utils.company_augment import build_company_search_plan
from utils.party_search import cell_matches_party_query

logger = logging.getLogger("legal_scraper.daily_run.sheet_search")


class BaseSheetSearchScraper(BaseScraper):
    """Scraper that queries the local case index instead of live websites."""

    COURT_TYPE = ""
    SOURCE = ""

    def __init__(self) -> None:
        self._index = CaseIndex.get_or_create()

    async def close(self) -> None:
        pass

    async def run(self, party_name: str) -> list[dict[str, Any]]:
        if not self.COURT_TYPE:
            return []
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._search_sync, party_name)

    def _search_sync(self, party_name: str) -> list[dict[str, Any]]:
        entity_type = getattr(config, "ENTITY_TYPE", "individual")

        # Build the precise per-cell matcher (company- or individual-aware).
        if entity_type == "company":
            matcher = build_company_search_plan(party_name).matcher()
            match_cell = matcher.matches
        else:
            match_cell = lambda cell: cell_matches_party_query(cell, party_name)

        cases = self._index.search(
            party_name, court_type=self.COURT_TYPE, match_fn=match_cell
        )

        results: list[dict[str, Any]] = []
        for case in cases:
            row = dict(case)
            row.pop("_is_continuation", None)  # full rows are stored un-split
            row["partyName"] = party_name
            results.append(row)

        logger.info(
            "[%s] index search '%s' -> %d cases [entity=%s]",
            self.COURT_TYPE.upper(),
            party_name,
            len(results),
            entity_type,
        )
        return results


class DCSheetScraper(BaseSheetSearchScraper):
    NAME = "district_court"
    COURT_TYPE = "dc"
    SOURCE = "DISTRICT_COURT"


class HCSheetScraper(BaseSheetSearchScraper):
    NAME = "high_court"
    COURT_TYPE = "hc"
    SOURCE = "HIGH_COURT"


class SCSheetScraper(BaseSheetSearchScraper):
    NAME = "supreme_court"
    COURT_TYPE = "sc"
    SOURCE = "SUPREME_COURT"
