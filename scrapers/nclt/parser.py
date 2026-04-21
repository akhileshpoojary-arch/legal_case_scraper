"""
NCLT Parser — transforms efiling.nclt.gov.in JSON into the unified 19-column schema.

Handles date-object formatting, listingHistory from proceedings,
applicationDetails from linked IAs, and party extraction from detail response.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from config import CSV_COLUMNS
from scrapers.base import BaseParser, clean_party_name
from utils.date_utils import format_date

logger = logging.getLogger("legal_scraper.nclt.parser")

# Bench location name → standardized benchName enum
_BENCH_NAME_MAP = {
    "new delhi": "New Delhi",
    "principal bench": "New Delhi",
    "mumbai": "Mumbai",
    "cuttack": "Cuttack",
    "ahmedabad": "Ahmedabad",
    "amaravati": "Amaravati",
    "chandigarh": "Chandigarh",
    "kolkata": "Kolkata",
    "jaipur": "Jaipur",
    "bengaluru": "Bengaluru",
    "bangalore": "Bengaluru",
    "chennai": "Chennai",
    "guwahati": "Guwahati",
    "hyderabad": "Hyderabad",
    "kochi": "Kochi",
    "indore": "Indore",
    "allahabad": "Allahabad",
}

# Dates now handled by utils.date_utils.format_date

def _normalize_bench(raw: str | None) -> str:
    """Map bench_location_name or bench_name to standardized enum."""
    if not raw:
        return ""
    key = raw.strip().lower()
    for fragment, canonical in _BENCH_NAME_MAP.items():
        if fragment in key:
            return canonical
    return raw.strip()


def _normalize_status(raw: str | None) -> str:
    """Map case status to enum: 'Pending' or 'Disposed'."""
    if not raw:
        return "Pending"
    s = str(raw).strip().lower()
    if "dispos" in s:
        return "Disposed"
    return "Pending"


def _safe(value: Any) -> str:
    """Return cleaned string or empty string for null-like values."""
    if value in (None, "", "NA", "null", "None", 0, "0"):
        return ""
    s = str(value).strip()
    return "" if s in ("NA", "0", "null", "None") else s


def _build_listing_history(detail: dict | None) -> str:
    """Build listingHistory JSON from allproceedingdtls in detail response."""
    if not detail:
        return ""

    proceedings = detail.get("allproceedingdtls") or []
    if not proceedings:
        return ""

    result = []
    for p in proceedings:
        date_str = p.get("listing_date")
        date_obj = format_date(date_str)
        if not date_obj:
            continue

        entry: dict[str, Any] = {"date": date_obj}

        # The direct 'path' is often not valid. The valid url uses ordersview.drt with encPath.
        enc_path = _safe(p.get("encPath"))
        path_val = _safe(p.get("path"))
        
        # 'lyQYl4Apa1B1m96lSHw/8g==' is a known placeholder for 'No Order' in NCLT API
        is_real_path = enc_path and enc_path != "lyQYl4Apa1B1m96lSHw/8g=="
        
        if is_real_path:
            url = f"https://efiling.nclt.gov.in/ordersview.drt?path={enc_path}"
            entry["orderDetail"] = {"url": url}
            entry["encPath"] = enc_path
        elif path_val and path_val != "NA":
            url = f"https://efiling.nclt.gov.in{path_val}" if path_val.startswith("/") else path_val
            entry["orderDetail"] = {"url": url}

        entry["purpose"] = _safe(p.get("purpose")) or _safe(p.get("next_listing_purpose")) or None
        entry["actionTaken"] = _safe(p.get("today_action")) or None
        result.append(entry)

    return json.dumps(result) if result else ""


def _build_application_details(detail: dict | None) -> str:
    """Build applicationDetails JSON from mainFilnowithIaNoList in detail response."""
    if not detail:
        return ""

    ia_list = detail.get("mainFilnowithIaNoList") or []
    if not ia_list:
        return ""

    result = []
    for ia in ia_list:
        bench_loc = _safe(ia.get("bench_location_name"))
        bench_name = _normalize_bench(bench_loc) if bench_loc else None

        entry: dict[str, Any] = {
            "filingNumber": _safe(ia.get("filing_no")) or None,
            "caseType": _safe(ia.get("case_type_desc_cis")) or None,
            "caseTitle": None,
            "benchName": bench_name,
            "caseStatus": _normalize_status(ia.get("status")),
            "courtNumber": _safe(ia.get("court_no")) or None,
            "applicationNumber": _safe(ia.get("case_no")) or None,
            "registrationDate": format_date(ia.get("regis_date")),
            "filingDate": format_date(ia.get("date_of_filing")),
            "nextListingDate": format_date(ia.get("next_list_date")),
            "disposalDate": format_date(ia.get("disposal_date")),
        }

        # Build caseTitle from case_title1 + case_title2
        t1 = _safe(ia.get("case_title1"))
        t2 = _safe(ia.get("case_title2"))
        if t1 and t2:
            entry["caseTitle"] = f"{t1} vs {t2}"
        elif t1:
            entry["caseTitle"] = t1
        elif t2:
            entry["caseTitle"] = t2

        result.append(entry)

    return json.dumps(result) if result else ""


def _extract_parties(detail: dict | None) -> dict[str, str]:
    """Extract petitioner, respondent, advocates from partydetailslist."""
    info: dict[str, str] = {
        "petitioner": "",
        "otherPetitioner": "",
        "petitionerAdvocate": "",
        "respondent": "",
        "otherRespondent": "",
        "respondentAdvocate": "",
    }
    if not detail:
        return info

    party_list = detail.get("partydetailslist") or []
    if not party_list:
        return info

    petitioners: list[str] = []
    respondents: list[str] = []
    pet_advocates: list[str] = []
    res_advocates: list[str] = []

    for p in party_list:
        ptype = _safe(p.get("party_type")).upper()
        name = clean_party_name(_safe(p.get("party_name")))
        adv = _safe(p.get("party_lawer_name"))

        if ptype.startswith("P"):
            if name:
                petitioners.append(name)
            if adv:
                pet_advocates.append(adv)
        elif ptype.startswith("R"):
            if name:
                respondents.append(name)
            if adv:
                res_advocates.append(adv)

    if petitioners:
        info["petitioner"] = ", ".join(petitioners)
    if respondents:
        info["respondent"] = ", ".join(respondents)

    info["petitionerAdvocate"] = ", ".join(pet_advocates) if pet_advocates else ""
    info["respondentAdvocate"] = ", ".join(res_advocates) if res_advocates else ""

    return info


class NCLTParser(BaseParser):
    """Normalizes NCLT API responses into the unified 19-column schema."""

    SOURCE = "NCLT"

    def build_row(
        self,
        detail: dict[str, Any] | None,
        fallback: dict[str, Any],
        party_name: str,
        court: dict[str, Any],
        **extra: Any,
    ) -> dict[str, Any]:
        """
        Build a normalized row from the search result + detail response.

        Args:
            detail: full detail API response (or None if fetch failed)
            fallback: dict with search result fields + task context
            party_name: searched party name
            court: bench dict
        """
        raw = dict(fallback)
        task_ctx = extra.get("task_ctx", {})
        detail_data = detail

        # Party info from detail partydetailslist (preferred over search result)
        party_info = _extract_parties(detail_data)

        # If detail didn't yield party names, fall back to search result fields
        petitioner = party_info["petitioner"] or clean_party_name(_safe(raw.get("case_title1")))
        respondent = party_info["respondent"] or clean_party_name(_safe(raw.get("case_title2")))

        # Bench name normalization
        bench = _normalize_bench(
            _safe(raw.get("bench_location_name"))
            or task_ctx.get("bench_short", "")
            or court.get("bench", "")
        )

        row: dict[str, Any] = {}
        for col in CSV_COLUMNS:
            row[col] = ""

        row.update({
            "partyName": party_name,
            "caseNumber": _safe(raw.get("case_no")),
            "courtNumber": _safe(raw.get("court_no")),
            "registrationDate": format_date(raw.get("regis_date")),
            "nextListingDate": format_date(raw.get("next_list_date")),
            "respondent": respondent,
            "otherRespondent": party_info["otherRespondent"],
            "respondentAdvocate": party_info["respondentAdvocate"],
            "petitioner": petitioner,
            "otherPetitioner": party_info["otherPetitioner"],
            "petitionerAdvocate": party_info["petitionerAdvocate"],
            "location": "INDIA",
            "courtType": "NCLT",
            "benchName": bench,
            "caseType": _safe(raw.get("case_type_desc_cis")),
            "caseStatus": _normalize_status(raw.get("status")),
            "uniqueness": "",
            "listingHistory": _build_listing_history(detail_data),
            "applicationDetails": _build_application_details(detail_data),
            "status": "PUBLISHED",
        })

        return row

    def build_fallback(self, search_result: dict[str, Any]) -> dict[str, Any]:
        return search_result
