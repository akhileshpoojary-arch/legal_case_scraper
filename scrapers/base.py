"""
Abstract base classes for scrapers — Open/Closed Principle extension point.

Every website implements:
  - BaseExtractor subclass → handles HTTP requests to the court API
  - BaseParser subclass    → transforms raw API responses into normalized rows
  - BaseScraper subclass   → facade that composes extractor + parser

Adding a new website = new folder with extractor.py + parser.py + __init__.py.
No changes to this file or the engine.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from utils.date_utils import format_date

logger = logging.getLogger("legal_scraper.base")


class BaseExtractor(ABC):
    """
    Handles API communication for a specific court website.

    Subclasses implement search + detail fetching logic.
    """

    SOURCE: str = ""  # e.g. "DRT", "DRAT", "NCLT"

    @abstractmethod
    async def search(
        self,
        court: dict[str, Any],
        party_name: str,
    ) -> list[dict]:
        """
        Search a single court for cases matching the party name.

        Returns raw API response items (list of dicts).
        """
        ...

    @abstractmethod
    async def fetch_detail(
        self,
        court: dict[str, Any],
        search_result: dict[str, Any],
    ) -> dict | None:
        """
        Fetch detailed case info for a single search result.

        Returns enriched dict or None on failure.
        """
        ...


class BaseParser(ABC):
    """
    Transforms raw API responses into normalized row dicts.

    Each row dict uses CSV_COLUMNS keys from config.
    """

    SOURCE: str = ""

    @abstractmethod
    def build_row(
        self,
        detail: dict | None,
        fallback: dict,
        party_name: str,
        court: dict[str, Any],
        **extra: Any,
    ) -> dict[str, Any]:
        """
        Build a single normalized output row.

        Uses detail data if available, falls back to search result data.
        """
        ...

    @abstractmethod
    def build_fallback(
        self,
        search_result: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Extract fallback fields from the search result.

        Used when detail fetch fails.
        """
        ...


class BaseScraper(ABC):
    """
    Facade — composes an extractor + parser to provide a single run() method.

    Subclasses wire up the correct extractor, parser, and court list.
    """

    NAME: str = ""
    SOURCE: str = ""

    @abstractmethod
    async def run(self, party_name: str) -> list[dict[str, Any]]:
        """
        Full scraping pipeline for one party across all courts.

        Returns list of normalized row dicts ready for sheet writing.
        """
        ...


import re

def clean_party_name(name: str | None) -> str:
    """Strips noisy prefixes and suffixes from a party name and normalizes it."""
    if not name:
        return ""
    text = str(name).strip().upper()
    
    # Reject strings that do not contain any alphabetic characters (e.g. " 0", "-", "123")
    if not any(c.isalpha() for c in text):
        return ""
    
    prefixes = ["M/S.", "M/S ", "M S ", "M/S"]
    for p in prefixes:
        if text.startswith(p):
            text = text[len(p):].strip()
            
    # Remove suffixes
    text = re.sub(r'\s+AND\s+\d*\s*ORS\.?$', '', text)
    text = re.sub(r'\s+AND\s+OTHERS?$', '', text)
    text = re.sub(r'\s+AND\s+ANOTHERS?$', '', text)
    text = re.sub(r'\s+AND\s+ANR\.?$', '', text)
    text = re.sub(r'\s+&AMP;\s+ORS\.?$', '', text)
    text = re.sub(r'\s+&\s+ORS\.?$', '', text)
    

    
    # Remove parentheses block at the end e.g. (Now Assinee...)
    text = re.sub(r'\s*\(.*$', '', text)
    
    # Strip trailing punctuation
    text = text.rstrip(',. ')
    
    return text.strip()

import json

def format_listing_history(details: list[dict[str, Any]] | None) -> str:
    """Takes caseProceedingDetails and formats them to the required date/purpose JSON string."""
    if not details:
        return ""
    
    result = []
    for d in details:
        date_str = d.get("causelistdate", "")
        # Remove empty string or None safety
        if date_str is None:
            date_str = ""
            
        purpose = d.get("purpose")
        
        date_obj = None
        if date_str:
            parts = str(date_str).split('/')
            if len(parts) == 3:
                try:
                    date_obj = {
                        "day": int(parts[0]),
                        "month": int(parts[1]),
                        "year": int(parts[2])
                    }
                except ValueError:
                    pass
        
        result.append({
            "date": date_obj,
            "purpose": purpose
        })
        
    if not result:
        return ""
        
    return json.dumps(result)

import aiohttp
from config import E_JAGRITI_DATE_FROM, E_JAGRITI_DATE_TO

EJAGRITI_HEADERS = {
    "Accept":             "application/json",
    "Accept-Language":    "en-US,en;q=0.9",
    "Connection":         "keep-alive",
    "Referer":            "https://e-jagriti.gov.in/",
    "User-Agent":         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}

EJAGRITI_ALL_STATES = [
    {"commissionId": 11350000, "commissionNameEn": "ANDAMAN NICOBAR"},
    {"commissionId": 11280000, "commissionNameEn": "ANDHRA PRADESH"},
    {"commissionId": 11120000, "commissionNameEn": "ARUNACHAL PRADESH"},
    {"commissionId": 11180000, "commissionNameEn": "ASSAM"},
    {"commissionId": 11100000, "commissionNameEn": "BIHAR"},
    {"commissionId": 11040000, "commissionNameEn": "CHANDIGARH"},
    {"commissionId": 11220000, "commissionNameEn": "CHHATTISGARH"},
    {"commissionId": 11380000, "commissionNameEn": "DADRA AND NAGAR HAVELI AND DAMAN AND DIU"},
    {"commissionId": 11070000, "commissionNameEn": "DELHI"},
    {"commissionId": 11300000, "commissionNameEn": "GOA"},
    {"commissionId": 11240000, "commissionNameEn": "GUJARAT"},
    {"commissionId": 11060000, "commissionNameEn": "HARYANA"},
    {"commissionId": 11020000, "commissionNameEn": "HIMACHAL PRADESH"},
    {"commissionId": 11010000, "commissionNameEn": "J AND K"},
    {"commissionId": 11200000, "commissionNameEn": "JHARKHAND"},
    {"commissionId": 11290000, "commissionNameEn": "KARNATAKA"},
    {"commissionId": 11320000, "commissionNameEn": "KERALA"},
    {"commissionId": 11370000, "commissionNameEn": "LADAKH"},
    {"commissionId": 11310000, "commissionNameEn": "LAKSHADWEEP"},
    {"commissionId": 11230000, "commissionNameEn": "MADHYA PRADESH"},
    {"commissionId": 11270000, "commissionNameEn": "MAHARASHTRA"},
    {"commissionId": 11140000, "commissionNameEn": "MANIPUR"},
    {"commissionId": 11170000, "commissionNameEn": "MEGHALAYA"},
    {"commissionId": 11150000, "commissionNameEn": "MIZORAM"},
    {"commissionId": 11130000, "commissionNameEn": "NAGALAND"},
    {"commissionId": 11210000, "commissionNameEn": "ODISHA"},
    {"commissionId": 11340000, "commissionNameEn": "PONDICHERRY"},
    {"commissionId": 11030000, "commissionNameEn": "PUNJAB"},
    {"commissionId": 11080000, "commissionNameEn": "RAJASTHAN"},
    {"commissionId": 11110000, "commissionNameEn": "SIKKIM"},
    {"commissionId": 11330000, "commissionNameEn": "TAMIL NADU"},
    {"commissionId": 11360000, "commissionNameEn": "TELANGANA"},
    {"commissionId": 11160000, "commissionNameEn": "TRIPURA"},
    {"commissionId": 11050000, "commissionNameEn": "UTTARAKHAND"},
    {"commissionId": 11090000, "commissionNameEn": "UTTAR PRADESH"},
    {"commissionId": 11190000, "commissionNameEn": "WEST BENGAL"},
]

class BaseEjagritiExtractor(BaseExtractor):
    """Shared extractor logic for NCDRC, SCDRC, and DCDRC."""
    TYPE_ID: str = ""
    BASE_URL = "https://e-jagriti.gov.in/services/report/report"

    def __init__(self, session_manager) -> None:
        self.sm = session_manager
        self.courts: list[dict[str, Any]] = []

    async def search(self, court: dict[str, Any], party_name: str) -> list[dict]:
        params = {
            "commissionTypeId": self.TYPE_ID,
            "commissionId": court["id"],
            "filingDate1": E_JAGRITI_DATE_FROM,
            "filingDate2": E_JAGRITI_DATE_TO,
            "complainant_respondent_name_en": party_name,
        }
        url = f"{self.BASE_URL}/getCauseTitleListByCompany"

        try:
            resp = await self.sm.get(url, params=params, timeout=25)
            if not resp: return []
            return self._extract_list(resp)
        except Exception as e:
            logger.debug(f"{self.SOURCE}: Error fetching {court['name']}: {e}")
            return []

    async def search_all_courts(self, party_name: str) -> list[tuple[dict[str, Any], list[dict]]]:
        import asyncio
        coros = [self.search(c, party_name) for c in self.courts]
        results = await asyncio.gather(*coros, return_exceptions=True)
        
        valid = []
        for court, res in zip(self.courts, results):
            if isinstance(res, Exception):
                logger.debug(f"    ↳ {self.SOURCE}: Failed searching court {court['name']}: {res}")
            elif res:
                logger.debug(f"    ↳ {self.SOURCE}: Found {len(res)} cases in {court['name']}")
                valid.append((court, res))
        return valid

    async def fetch_detail(self, court: dict[str, Any], search_result: dict[str, Any]) -> dict | None:
        """E-Jagriti provides all details in search endpoint. No detail fetch needed."""
        return search_result

    def _extract_list(self, resp: Any) -> list[dict]:
        if resp is None: return []
        if isinstance(resp, list): return resp
        if isinstance(resp, dict):
            for key in ("data", "result", "cases", "list", "records", "response"):
                if key in resp and isinstance(resp[key], list):
                    return resp[key]
            if any(k in resp for k in ("caseNumber", "complainantNameEn")):
                return [resp]
        return []

class BaseEjagritiParser(BaseParser):
    """Shared parser logic for NCDRC, SCDRC, and DCDRC mapping."""
    
    def _g(self, raw: dict, *keys: str) -> str:
        for k in keys:
            v = raw.get(k)
            if v not in (None, "", "null", "NULL", "None"):
                return str(v).strip()
        return ""

    def build_row(self, detail: dict | None, fallback: dict, party_name: str, court: dict[str, Any], **extra: Any) -> dict[str, Any]:
        d = detail or fallback
        status = self._g(d, "caseStatus", "case_status", "status", "statusName", "case_stage_name", "stage")
        if not status:
            status = "PENDING"
            
        return {
            "partyName": party_name,
            "caseNumber": self._g(d, "case_number", "caseNumber", "registrationNo", "file_application_number"),
            "courtNumber": "",
            "registrationDate": format_date(self._g(d, "registrationDate", "registration_date", "regDate", "case_filing_date", "filingDate")),
            "nextListingDate": format_date(self._g(d, "date_of_next_hearing", "nextHearingDate", "nextDate")),
            "respondent": clean_party_name(self._g(d, "respondent_name", "respondentNameEn", "respondentName")),
            "otherRespondent": "",
            "respondentAdvocate": self._g(d, "respondent_advocate_name", "respondentAdvocateName"),
            "petitioner": clean_party_name(self._g(d, "complainant_name", "complainantNameEn", "complainantName")),
            "otherPetitioner": "",
            "petitionerAdvocate": self._g(d, "complainant_advocate_name", "complainantAdvocateName"),
            "location": "INDIA",
            "courtType": self.SOURCE,
            "benchName": court["name"],
            "caseType": self._g(d, "case_type_name", "caseTypeName"),
            "caseStatus": status,
            "uniqueness": "",
            "listingHistory": "",
            "applicationDetails": "",
            "status": "PUBLISHED",
        }

    def build_fallback(self, search_result: dict[str, Any]) -> dict[str, Any]:
        return filter_nulls(search_result)

def filter_nulls(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}
