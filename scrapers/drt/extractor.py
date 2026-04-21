"""
DRT Extractor — API communication with drt.gov.in for DRT cases.

Searches all 39 DRT courts for a party name, then fetches case details
for each filing number returned by the search.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from config import COMMON_HEADERS
from scrapers.base import BaseExtractor
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.drt.extractor")

DRT_BASE_URL = "https://drt.gov.in"
DRT_SEARCH_API = f"{DRT_BASE_URL}/drtapi/casedetail_party_name_wise"
DRT_DETAIL_API = f"{DRT_BASE_URL}/drtapi/getCaseDetailPartyWise"

DRT_HEADERS = {
    **COMMON_HEADERS,
    "Origin": "https://drt.gov.in",
    "Referer": "https://drt.gov.in/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

# Paste fresh cookies when session expires
DRT_COOKIES = {
    "SERVER_135": "drt_app2_135",
    "laravel_session": (
        "eyJpdiI6IlpsOWdpR3dCbjNoTnlXYW1uSXhUU3c9PSIsInZhbHVlIjoibFNWckxS"
        "QWV1Z2xMMTV1d1pSSDE5SVVDWGQva2lEZWIwT2hZSEZMRUhIQXhsa05WbWRBMVhr"
        "a1VGVmI0cTdBZGNYUEFYVFhrcjEwaUNRUXJIYUtMdEFMcUJWWUxOenVVb001V3RE"
        "ZlRlMSthUG1KVVJFOHhsVDBhcW5icTRGU0YiLCJtYWMiOiJmNTM0MGRmYTBjYWNi"
        "ODZmZmEwMDNjYWFiMjRlNTUxZmM0NTEwYWZjYTMyMGMyNGY2YWJiMWUyYjhjMjgy"
        "NTJkIiwidGFnIjoiIn0%3D"
    ),
}

# ── 39 DRTs ───────────────────────────────────────────────────
DRT_COURTS = [
    {"id": 1, "name": "DEBTS RECOVERY TRIBUNAL DELHI (DRT 1)"},
    {"id": 2, "name": "DEBTS RECOVERY TRIBUNAL DELHI (DRT 2)"},
    {"id": 3, "name": "DEBTS RECOVERY TRIBUNAL DELHI (DRT 3)"},
    {"id": 9, "name": "DEBTS RECOVERY TRIBUNAL AHMEDABAD (DRT 1)"},
    {"id": 10, "name": "DEBTS RECOVERY TRIBUNAL AHMEDABAD (DRT 2)"},
    {"id": 11, "name": "DEBTS RECOVERY TRIBUNAL ALLAHABAD"},
    {"id": 12, "name": "DEBTS RECOVERY TRIBUNAL AURANGABAD"},
    {"id": 13, "name": "DEBTS RECOVERY TRIBUNAL BANGALORE (DRT 1)"},
    {"id": 14, "name": "DEBTS RECOVERY TRIBUNAL CHANDIGARH (DRT 1)"},
    {"id": 15, "name": "DEBTS RECOVERY TRIBUNAL CHANDIGARH (DRT 2)"},
    {"id": 16, "name": "DEBTS RECOVERY TRIBUNAL CHENNAI (DRT 1)"},
    {"id": 17, "name": "DEBTS RECOVERY TRIBUNAL CHENNAI (DRT 2)"},
    {"id": 18, "name": "DEBTS RECOVERY TRIBUNAL CHENNAI (DRT 3)"},
    {"id": 19, "name": "DEBTS RECOVERY TRIBUNAL COIMBATORE"},
    {"id": 20, "name": "DEBTS RECOVERY TRIBUNAL CUTTACK"},
    {"id": 21, "name": "DEBTS RECOVERY TRIBUNAL ERNAKULAM (DRT 1)"},
    {"id": 22, "name": "DEBTS RECOVERY TRIBUNAL GUWAHATI"},
    {"id": 23, "name": "DEBTS RECOVERY TRIBUNAL HYDERABAD (DRT 1)"},
    {"id": 24, "name": "DEBTS RECOVERY TRIBUNAL JABALPUR"},
    {"id": 25, "name": "DEBTS RECOVERY TRIBUNAL JAIPUR"},
    {"id": 26, "name": "DEBTS RECOVERY TRIBUNAL KOLKATA (DRT 1)"},
    {"id": 27, "name": "DEBTS RECOVERY TRIBUNAL KOLKATA (DRT 2)"},
    {"id": 28, "name": "DEBTS RECOVERY TRIBUNAL KOLKATA (DRT 3)"},
    {"id": 29, "name": "DEBTS RECOVERY TRIBUNAL LUCKNOW"},
    {"id": 30, "name": "DEBTS RECOVERY TRIBUNAL MADURAI"},
    {"id": 31, "name": "DEBTS RECOVERY TRIBUNAL MUMBAI (DRT 1)"},
    {"id": 32, "name": "DEBTS RECOVERY TRIBUNAL MUMBAI (DRT 2)"},
    {"id": 33, "name": "DEBTS RECOVERY TRIBUNAL MUMBAI (DRT 3)"},
    {"id": 34, "name": "DEBTS RECOVERY TRIBUNAL NAGPUR"},
    {"id": 35, "name": "DEBTS RECOVERY TRIBUNAL PATNA"},
    {"id": 36, "name": "DEBTS RECOVERY TRIBUNAL PUNE"},
    {"id": 37, "name": "DEBTS RECOVERY TRIBUNAL RANCHI"},
    {"id": 38, "name": "DEBTS RECOVERY TRIBUNAL VISHAKHAPATNAM"},
    {"id": 39, "name": "DEBTS RECOVERY TRIBUNAL BANGALORE (DRT 2)"},
    {"id": 40, "name": "DEBTS RECOVERY TRIBUNAL CHANDIGARH (DRT 3)"},
    {"id": 41, "name": "DEBTS RECOVERY TRIBUNAL DEHRADUN"},
    {"id": 42, "name": "DEBTS RECOVERY TRIBUNAL ERNAKULAM (DRT 2)"},
    {"id": 43, "name": "DEBTS RECOVERY TRIBUNAL HYDERABAD (DRT 2)"},
    {"id": 44, "name": "DEBTS RECOVERY TRIBUNAL SILIGURI"},
]

class DRTExtractor(BaseExtractor):
    """Async extractor for DRT (Debts Recovery Tribunal) cases."""

    SOURCE = "DRT"
    COOKIES = DRT_COOKIES
    HEADERS = DRT_HEADERS

    def __init__(self, session_manager: SessionManager) -> None:
        self._sm = session_manager

    @property
    def courts(self) -> list[dict[str, Any]]:
        return DRT_COURTS

    async def search(
        self,
        court: dict[str, Any],
        party_name: str,
    ) -> list[dict]:
        """Search a DRT court for cases matching the party name."""
        payload = {
            "schemeNameDratDrtId": str(court["id"]),
            "partyName": party_name,
        }
        result = await self._sm.post(
            DRT_SEARCH_API,
            data=payload,
            label=f"DRT search {court['name']}",
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
        """Fetch full case detail for a single filing number."""
        filing_no = search_result.get("filingNo")
        if not filing_no:
            return None

        return await self._sm.post(
            DRT_DETAIL_API,
            data={
                "filingNo": str(filing_no),
                "schemeNameDrtId": str(court["id"]),
            },
            label=f"DRT detail filing={filing_no}",
        )

    async def search_all_courts(
        self,
        party_name: str,
    ) -> list[tuple[dict, list[dict]]]:
        """
        Search ALL DRT courts concurrently for a party.

        Returns: [(court, [search_results]), ...]
        """
        async def _search_one(court: dict) -> tuple[dict, list[dict]]:
            results = await self.search(court, party_name)
            return court, results

        tasks = [_search_one(court) for court in self.courts]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        valid = []
        for court, result in zip(self.courts, raw_results):
            if isinstance(result, Exception):
                logger.error("DRT search %s failed: %s", court["name"], result)
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
        """
        Fetch case details for all search results from one court concurrently.

        Returns: [(search_result, detail_or_None), ...]
        """

        async def _fetch_one(sr: dict) -> tuple[dict, dict | None]:
            detail = await self.fetch_detail(court, sr)
            return sr, detail

        tasks = [_fetch_one(sr) for sr in search_results]
        return await asyncio.gather(*tasks)
