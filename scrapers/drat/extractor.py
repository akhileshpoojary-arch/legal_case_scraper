"""
DRAT Extractor — API communication with drt.gov.in for DRAT (appellate) cases.

Searches all 5 DRAT courts for a party name, then fetches case details
for each diary number returned by the search.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from scrapers.base import BaseExtractor
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.drat.extractor")

DRAT_BASE_URL = "https://drt.gov.in"
DRAT_SEARCH_API = f"{DRAT_BASE_URL}/drtapi/drat_party_name_wise"
DRAT_DETAIL_API = f"{DRAT_BASE_URL}/drtapi/getDratCaseDetailDiaryNoWise"

# ── 5 DRATs ───────────────────────────────────────────────────
DRAT_COURTS = [
    {"id": 100, "name": "DEBT RECOVERY APPELLATE TRIBUNAL - DELHI"},
    {"id": 101, "name": "DEBT RECOVERY APPELLATE TRIBUNAL - ALLAHABAD"},
    {"id": 102, "name": "DEBT RECOVERY APPELLATE TRIBUNAL - CHENNAI"},
    {"id": 103, "name": "DEBT RECOVERY APPELLATE TRIBUNAL - MUMBAI"},
    {"id": 104, "name": "DEBT RECOVERY APPELLATE TRIBUNAL - KOLKATA"},
]


def _parse_diary(diary_str: str | None) -> tuple[int | None, int | None]:
    """Parse 'diaryno/diaryyear' format."""
    try:
        parts = str(diary_str).split("/")
        return int(parts[0]), int(parts[1])
    except Exception:
        return None, None


class DRATExtractor(BaseExtractor):
    """Async extractor for DRAT (Debt Recovery Appellate Tribunal) cases."""

    SOURCE = "DRAT"

    def __init__(self, session_manager: SessionManager) -> None:
        self._sm = session_manager

    @property
    def courts(self) -> list[dict[str, Any]]:
        return DRAT_COURTS

    async def search(
        self,
        court: dict[str, Any],
        party_name: str,
    ) -> list[dict]:
        """Search a DRAT court for cases matching the party name."""
        payload = {
            "schemeNameDratDrtId": str(court["id"]),
            "partyName": party_name,
        }
        result = await self._sm.post(
            DRAT_SEARCH_API,
            data=payload,
            label=f"DRAT search {court['name']}",
        )

        if result is None:
            return []
            
        if isinstance(result, dict):
            # API returns {"status": "Record Not Fund"} if no cases
            if "status" in result and "Record Not Fund" in str(result.get("status", "")):
                return []
            if "status" in result and "No Record" in str(result.get("status", "")):
                return []
            if "message" in result and "No Record" in str(result.get("message", "")):
                return []
            
            # If it's just a status message dict, don't count it
            if list(result.keys()) == ["status"] or list(result.keys()) == ["message"]:
                return []
                
            return [result]
            
        return result

    async def fetch_detail(
        self,
        court: dict[str, Any],
        search_result: dict[str, Any],
    ) -> dict | None:
        """Fetch full case detail using diary number + year."""
        diary_str = search_result.get("diaryno", "")
        diary_no, diary_year = _parse_diary(diary_str)
        if diary_no is None:
            return None

        return await self._sm.post(
            DRAT_DETAIL_API,
            data={
                "schemeNameDrtId": str(court["id"]),
                "diaryNo": str(diary_no),
                "diaryYear": str(diary_year),
            },
            label=f"DRAT detail {diary_no}/{diary_year}",
        )

    async def search_all_courts(
        self,
        party_name: str,
    ) -> list[tuple[dict, list[dict]]]:
        """Search ALL DRAT courts concurrently for a party."""

        async def _search_one(court: dict) -> tuple[dict, list[dict]]:
            results = await self.search(court, party_name)
            return court, results

        tasks = [_search_one(court) for court in self.courts]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        valid = []
        for court, result in zip(self.courts, raw_results):
            if isinstance(result, Exception):
                logger.error("DRAT search %s failed: %s", court["name"], result)
            elif isinstance(result, tuple):
                _, cases = result
                if cases:
                    valid.append(result)
        return valid

    async def fetch_details_batch(
        self,
        court: dict[str, Any],
        search_results: list[dict],
    ) -> list[tuple[dict, dict | None]]:
        """Fetch case details for all search results from one court concurrently."""

        async def _fetch_one(sr: dict) -> tuple[dict, dict | None]:
            detail = await self.fetch_detail(court, sr)
            return sr, detail

        tasks = [_fetch_one(sr) for sr in search_results]
        return await asyncio.gather(*tasks)
