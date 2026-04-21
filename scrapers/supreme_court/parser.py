"""Parse Supreme Court detail tabs into the unified output schema."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from config import CSV_COLUMNS
from scrapers.base import BaseParser
from utils.date_utils import format_date

logger = logging.getLogger("legal_scraper.supreme_court.parser")


def _parse_date_obj(date_str: str) -> dict | None:
    """Parse DD-MM-YYYY into {day, month, year} for application details."""
    if not date_str or not isinstance(date_str, str):
        return None
    s = date_str.strip().split(" ")[0]
    parts = s.split("-")
    if len(parts) != 3:
        return None
    try:
        d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
        if 1 <= m <= 12 and 1 <= d <= 31 and y > 1900:
            return {"day": d, "month": m, "year": y}
    except ValueError:
        pass
    return None


def _extract_filed_on(diary_text: str) -> str:
    """Extract the date portion from 'Filed on DD-MM-YYYY ...' text."""
    m = re.search(
        r'Filed\s+on\s+(\d{2}-\d{2}-\d{4})(?:\s+\d{1,2}:\d{2}\s+(?:AM|PM))?',
        diary_text,
        re.IGNORECASE,
    )
    return m.group(1).strip() if m else ""


def _extract_next_listing_from_status(status_text: str) -> str:
    """Extract the next-listing date from status text."""
    m = re.search(
        r'List\s+On\s*\(Date\)\s*\(\s*(\d{2}-\d{2}-\d{4})\s*\)',
        status_text,
        re.IGNORECASE,
    )
    if m:
        return m.group(1)

    m = re.search(
        r'List\s+On\s*\(Date\)\s*(\d{2}-\d{2}-\d{4})',
        status_text,
        re.IGNORECASE,
    )
    if m:
        return m.group(1)

    m = re.search(
        r'List\s+On\s*(\d{2}-\d{2}-\d{4})',
        status_text,
        re.IGNORECASE,
    )
    if m:
        return m.group(1)

    return ""


def _extract_first_date(text: str) -> str:
    """Pull the first DD-MM-YYYY occurrence from an arbitrary text string."""
    m = re.search(r'(\d{2}-\d{2}-\d{4})', text)
    return m.group(1) if m else ""


def _clean_parties(raw_html: str) -> str:
    """Parse and deduplicate party/advocate entries from HTML cell content."""
    if not raw_html:
        return ""

    soup = BeautifulSoup(raw_html, "html.parser")

    raw_text = soup.get_text(separator="|", strip=True)
    lines = [line.strip() for line in raw_text.split("|") if line.strip()]

    seen: set[str] = set()
    result: list[str] = []
    for line in lines:
        cleaned = re.sub(r"^\d+[\s\.\-]+", "", line).strip()
        if not cleaned:
            continue
        cleaned = re.sub(r"\s+", " ", cleaned)
        if cleaned not in seen:
            seen.add(cleaned)
            result.append(f'"{cleaned}"')

    return ", ".join(result)


def _normalize_status(raw: str | None) -> str:
    """Map case status to 'Pending' or 'Disposed'."""
    if not raw:
        return "Pending"
    s = str(raw).strip().upper()
    if "DISPOS" in s:
        return "Disposed"
    return "Pending"


_CASE_NUMBER_RE = re.compile(r'\d+\s*/\s*\d{4}')


def _validate_case_number(raw: str) -> str:
    """Return case number only when it contains a numeric case/year pattern."""
    if not raw:
        return ""
    if _CASE_NUMBER_RE.search(raw):
        return raw.strip()
    return ""


def _parse_main_tab(html: str) -> dict[str, dict]:
    """Extract key-value pairs from the main case details table."""
    soup = BeautifulSoup(html, "html.parser")
    details: dict[str, dict] = {}

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 2:
            continue
        key = tds[0].get_text(strip=True).replace(":", "").strip()
        if not key:
            continue
        val_text = tds[1].get_text(separator=" ", strip=True)
        val_text = re.sub(r"\s{2,}", " ", val_text).strip()
        val_html = str(tds[1])
        details[key] = {"text": val_text, "html": val_html}

    return details


class SCIParser(BaseParser):
    """Map Supreme Court detail data to the 19-column schema."""

    SOURCE = "SUPREME_COURT"

    def build_row(
        self,
        detail: dict[str, str],
        fallback: dict[str, Any],
        party_name: str,
        **extra: Any,
    ) -> dict[str, Any]:
        """Merge the multi-tab HTML dictionaries into the final 19-column schema."""
        html_main         = detail.get("case_details") or detail.get("") or ""
        html_listing      = detail.get("listing_dates") or ""
        html_applications = detail.get("interlocutory_application_documents") or ""
        html_judgements   = detail.get("judgement_orders") or ""

        row = {col: "" for col in CSV_COLUMNS}

        row["partyName"]   = party_name
        row["caseNumber"]  = fallback.get("case_number", "")
        row["petitioner"]  = fallback.get("petitioner", "")
        row["respondent"]  = fallback.get("respondent", "")
        row["caseStatus"]  = _normalize_status(fallback.get("status", ""))
        row["courtType"]   = "SUPREME COURT"
        row["location"]    = "India"
        row["benchName"]   = "New Delhi"
        row["uniqueness"]    = None
        row["status"]      = "PUBLISHED"
        row["courtNumber"] = None   # SCI has no court-number concept

        if html_main:
            details = _parse_main_tab(html_main)

            if "Diary Number" in details:
                filed_raw = _extract_filed_on(details["Diary Number"]["text"])
                if filed_raw:
                    row["registrationDate"] = format_date(filed_raw)

            if "Case Number" in details:
                raw_case = details["Case Number"]["text"]
                raw_case = re.split(r'\s*Registered on\b', raw_case, flags=re.IGNORECASE)[0]
                raw_case = re.split(r'\s*Verified\s+On\b',  raw_case, flags=re.IGNORECASE)[0]
                validated = _validate_case_number(raw_case.strip())
                if validated:
                    row["caseNumber"] = validated
                else:
                    row["caseNumber"] = ""

            next_date_raw = ""
            for key in details:
                if "tentatively" in key.lower() or "likely to be listed" in key.lower():
                    next_date_raw = _extract_first_date(details[key]["text"])
                    break

            if not next_date_raw and "Status/Stage" in details:
                next_date_raw = _extract_next_listing_from_status(
                    details["Status/Stage"]["text"]
                )

            if next_date_raw:
                row["nextListingDate"] = format_date(next_date_raw)

            if "Category" in details:
                row["caseType"] = details["Category"]["text"]

            if "Status/Stage" in details:
                row["caseStatus"] = _normalize_status(
                    details["Status/Stage"]["text"]
                )

            if "Petitioner(s)" in details:
                row["petitioner"] = _clean_parties(details["Petitioner(s)"]["html"])
            if "Respondent(s)" in details:
                row["respondent"] = _clean_parties(details["Respondent(s)"]["html"])
            if "Petitioner Advocate(s)" in details:
                row["petitionerAdvocate"] = _clean_parties(
                    details["Petitioner Advocate(s)"]["html"]
                )
            if "Respondent Advocate(s)" in details:
                row["respondentAdvocate"] = _clean_parties(
                    details["Respondent Advocate(s)"]["html"]
                )

        if not row.get("nextListingDate") and html_listing:
            soup_listing = BeautifulSoup(html_listing, "html.parser")
            for tr in soup_listing.find_all("tr"):
                tds = [td.get_text(strip=True) for td in tr.find_all("td")]
                if len(tds) >= 1 and tds[0] and "CL Date" not in tds[0]:
                    candidate = _extract_first_date(tds[0])
                    if candidate:
                        row["nextListingDate"] = format_date(candidate)
                        break

        history: list[dict] = []
        if html_judgements:
            soup_j = BeautifulSoup(html_judgements, "html.parser")
            for tr in soup_j.find_all("tr"):
                tds = tr.find_all("td")
                if not tds:
                    continue
                a_tag = tds[0].find("a")
                if not a_tag:
                    continue
                url      = a_tag.get("href", "") or ""
                date_str = a_tag.get_text(strip=True)
                cell_txt = tds[0].get_text(strip=True)
                purpose  = cell_txt.replace(date_str, "").strip() or None

                history.append({
                    "date":        date_str,
                    "purpose":     purpose,
                    "orderDetail": {"url": url} if url else None,
                    "actionTaken": None,
                    "encPath":     None,
                })

        if history:
            row["listingHistory"] = json.dumps(history)

        apps: list[dict] = []
        if html_applications:
            soup_app = BeautifulSoup(html_applications, "html.parser")
            for tr in soup_app.find_all("tr"):
                tds = [td for td in tr.find_all("td")]
                if len(tds) < 6:
                    continue
                tds_text = [td.get_text(strip=True) for td in tds]

                if "Serial Number" in tds_text[0] or "Filing Number" in tds_text[1]:
                    continue   # skip header rows

                app_no     = tds_text[1]
                app_type   = tds_text[2]
                filed_by   = tds_text[4]
                filing_raw = tds_text[5]
                status_raw = tds_text[6] if len(tds_text) > 6 else ""

                filing_date_obj: dict | None = _parse_date_obj(filing_raw)

                case_status = _normalize_status(status_raw)
                if status_raw and not any(kw in status_raw.upper() for kw in ["PENDING", "DISPOS", "ALLOWED", "DISMISSED"]):
                    if re.search(r'\d+/\d{4}', status_raw):
                        case_status = "Pending"

                if filing_date_obj is None:
                    pass

                apps.append({
                    "filingNumber":      app_no or None,
                    "caseType":          app_type or None,
                    "caseTitle":         filed_by or None,
                    "benchName":         "New Delhi",
                    "caseStatus":        case_status,
                    "courtNumber":       None,
                    "applicationNumber": app_no or None,
                    "registrationDate":  None,
                    "filingDate":        filing_date_obj,
                    "nextListingDate":   None,
                    "disposalDate":      None,
                })

        if apps:
            row["applicationDetails"] = json.dumps(apps)

        return row

    def build_fallback(self, search_result: dict[str, Any]) -> dict[str, Any]:
        row = {col: "" for col in CSV_COLUMNS}
        row["caseNumber"]  = search_result.get("case_number", "")
        row["petitioner"]  = search_result.get("petitioner", "")
        row["respondent"]  = search_result.get("respondent", "")
        row["caseStatus"]  = _normalize_status(search_result.get("status", ""))
        row["courtType"]   = "SUPREME COURT"
        row["location"]    = "India"
        row["benchName"]   = "New Delhi"
        row["uniqueness"]    = None
        row["status"]      = "PUBLISHED"
        row["courtNumber"] = None
        return row
