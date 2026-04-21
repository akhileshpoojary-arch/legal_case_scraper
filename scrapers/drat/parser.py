"""
DRAT Parser — transforms raw DRAT API responses into normalized rows.

Handles DRAT-specific field names (applicant vs petitioner, etc.).
"""

from __future__ import annotations

import logging
from typing import Any

from config import CSV_COLUMNS
from scrapers.base import BaseParser, clean_party_name, format_listing_history
from utils.date_utils import format_date

logger = logging.getLogger("legal_scraper.drat.parser")


def _safe_strip(value: Any, suffix: str = "") -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if suffix:
        text = text.rstrip(suffix).strip()
    return text


class DRATParser(BaseParser):
    """Parses DRAT API responses into normalized output rows."""

    SOURCE = "DRAT"

    def build_fallback(self, search_result: dict[str, Any]) -> dict[str, Any]:
        """Extract fallback fields from the DRAT search result."""
        d = dict(search_result)
        d.update({
            "casetype": search_result.get("casetype"),
            "casestatus": search_result.get("casestatus"),
            "petitionerName": (
                search_result.get("petitionerName")
                or search_result.get("applicant")
            ),
            "respondentName": (
                search_result.get("respondentName")
                or search_result.get("respondent")
            ),
        })
        return d

    def build_row(self,detail: dict | None,fallback: dict,party_name: str,court: dict[str, Any],**extra: Any,) -> dict[str, Any]:
        """Build a normalized row from DRAT detail or fallback data."""
        d = dict(fallback)
        if detail:
            d.update({k: v for k, v in detail.items() if v is not None and str(v).strip() != ""})

        row: dict[str, Any] = dict(d)
        for col in CSV_COLUMNS:
            if col not in row:
                row[col] = None

        # caseStatus mapping rules
        raw_status = str(d.get("casestatus") or "").strip().lower()
        if raw_status in ["disposal", "disposed"]:
            case_status = "Disposed"
        else:
            case_status = "Pending"

        row.update(
            {
                "partyName": party_name,
            "caseNumber": d.get("caseno"),
                "courtNumber": d.get("courtNo") or "",
                "registrationDate": format_date(d.get("dateoffiling")),
                "nextListingDate": format_date(d.get("nextlistingdate")),
                "respondent": clean_party_name(d.get("respondentName") or d.get("respondent")),
                "otherRespondent": d.get("additionalpartyres"),
                "respondentAdvocate": d.get("advocateResName") or d.get("respondentadvocate"),
                "petitioner": clean_party_name(d.get("petitionerName") or d.get("applicant")),
                "otherPetitioner": d.get("additionalpartypet"),
                "petitionerAdvocate": d.get("advocatePetName") or d.get("applicantadvocate"),
                "location": "INDIA",
                "courtType": "DRAT",
                "benchName": court["name"],
                "caseType": d.get("casetype"),
                "caseStatus": case_status,
                "uniqueness": "",
                "listingHistory": format_listing_history(d.get("caseProceedingDetails")),
                "applicationDetails": "",
                "status": "PUBLISHED",
            }
        )
        return row
