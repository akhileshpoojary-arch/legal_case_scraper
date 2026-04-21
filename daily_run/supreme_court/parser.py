
from __future__ import annotations

import json
import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from config import CSV_COLUMNS
from utils.date_utils import format_date

logger = logging.getLogger("legal_scraper.daily_run.sc.parser")

# Full display ref as on sci.gov.in, e.g. CRCASE/23/2025, SLP(C)/12/2025
_SC_CASE_REF_RE = re.compile(
    r"\b([A-Za-z()]{2,40})/\s*(\d{1,8})\s*/\s*(\d{4})\b",
)

# API case_type code ? docket prefix (extend as needed; unknown codes use _slug_from_code).
_SCI_TYPE_SLUG_BY_CODE: dict[str, str] = {
    "1": "SLP(C)",
    "2": "SLP(CRL)",
    "3": "CA",
    "4": "CRCASE",
    "5": "WP(C)",
    "6": "WP(CRL)",
    "7": "TP(C)",
    "8": "TP(CRL)",
    "9": "RP(C)",
    "10": "RP(CRL)",
    "11": "TC(C)",
    "12": "TC(CRL)",
    "17": "OS",
    "18": "DRC",
    "19": "CONT(C)",
    "20": "CONT(CRL)",
    "21": "TRC",
    "22": "SRC",
    "23": "EP(C)",
    "24": "ARB.P.",
    "25": "CUR(C)",
    "26": "CUR(CRL)",
    "31": "DIARY",
    "39": "MISC.",
}

def _normalize_sc_case_display(raw: str) -> str:
    """Return TYPE/NO/YEAR when present (e.g. CRCASE/23/2025); else ''."""
    if not raw:
        return ""
    s = str(raw).strip()
    m = _SC_CASE_REF_RE.search(s)
    if m:
        return f"{m.group(1).upper()}/{m.group(2)}/{m.group(3)}"
    return ""

def _slug_from_code(code: str, type_name: str) -> str:
    c = str(code).strip()
    if c in _SCI_TYPE_SLUG_BY_CODE:
        return _SCI_TYPE_SLUG_BY_CODE[c]
    if not type_name:
        return f"T{c}" if c else "SC"
    letters = re.findall(r"[A-Za-z]", type_name.upper())[:10]
    return "".join(letters) if letters else f"T{c}"

def _extract_filed_on(diary_text: str) -> str:
    m = re.search(
        r"Filed\s+on\s+(\d{2}-\d{2}-\d{4})(?:\s+\d{1,2}:\d{2}\s+(?:AM|PM))?",
        diary_text,
        re.IGNORECASE,
    )
    return m.group(1).strip() if m else ""

def _extract_registered_on(case_text: str) -> str:
    m = re.search(
        r"Registered\s+on\s+(\d{2}-\d{2}-\d{4})",
        case_text,
        re.IGNORECASE,
    )
    return m.group(1).strip() if m else ""

def _extract_next_listing_from_status(status_text: str) -> str:
    for pattern in [
        r"List\s+On\s*\(Date\)\s*\(\s*(\d{2}-\d{2}-\d{4})\s*\)",
        r"List\s+On\s*\(Date\)\s*(\d{2}-\d{2}-\d{4})",
        r"List\s+On\s*(\d{2}-\d{2}-\d{4})",
    ]:
        m = re.search(pattern, status_text, re.IGNORECASE)
        if m:
            return m.group(1)
    return ""

def _extract_first_date(text: str) -> str:
    m = re.search(r"(\d{2}-\d{2}-\d{4})", text)
    return m.group(1) if m else ""

def _clean_parties(raw_html: str) -> str:
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
    if not raw:
        return "Pending"
    if "DISPOS" in str(raw).strip().upper():
        return "Disposed"
    return "Pending"

def _parse_date_obj(date_str: str) -> dict | None:
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

def _parse_main_tab(html: str) -> dict[str, dict]:
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

def parse_html(tabs: dict[str, str]) -> dict[str, str]:
    """Passthrough ? tabs dict is already in the right format."""
    return tabs

def build_sc_row(
    detail: dict[str, str],
    fallback: dict[str, Any],
) -> dict[str, Any]:
    """Build a normalized row from multi-tab HTML detail."""
    html_main = detail.get("case_details") or detail.get("") or ""
    html_listing = detail.get("listing_dates") or ""
    html_applications = detail.get("interlocutory_application_documents") or ""
    html_judgements = detail.get("judgement_orders") or ""

    row = {col: "" for col in CSV_COLUMNS}

    row["partyName"] = ""
    # Construct caseNumber from search parameters primarily (TYPE/NO/YEAR).
    slug = _slug_from_code(
        str(fallback.get("sc_type_code", "")).strip(),
        str(fallback.get("searched_case_type", "")).strip(),
    )
    num = str(fallback.get("searched_case_no", "")).strip()
    yr = str(fallback.get("sc_year", "")).strip()
    if slug and num and yr:
        row["caseNumber"] = f"{num}"
    else:
        raw_cn = (fallback.get("case_number") or "").strip()
        row["caseNumber"] = _normalize_sc_case_display(raw_cn) or raw_cn
    row["petitioner"] = fallback.get("petitioner", "")
    row["respondent"] = fallback.get("respondent", "")
    row["caseStatus"] = _normalize_status(fallback.get("status", ""))
    row["courtType"] = "Supreme Court"
    row["location"] = "INDIA"
    row["benchName"] = "New Delhi"
    row["caseType"] = fallback.get("searched_case_type", "")
    dn = str(fallback.get("diary_no", "")).strip()
    dy = str(fallback.get("diary_year", "")).strip()
    if dn and dy:
        row["uniqueness"] = f"SC|{dy}|{dn}"
    else:
        row["uniqueness"] = (
            f"SC|{fallback.get('searched_case_no', '')}|"
            f"{fallback.get('case_number', '')}|{row['caseNumber']}"
        )
    row["status"] = "PUBLISHED"
    row["courtNumber"] = ""

    if html_main:
        details = _parse_main_tab(html_main)

        if "Case Number" in details:
            reg_raw = _extract_registered_on(details["Case Number"]["text"])
            if reg_raw:
                row["registrationDate"] = format_date(reg_raw)
            # User Preference: Case number is constructed from search params (TYPE/NO/YEAR).
            # Do not overwrite with the site's display format (e.g. SLP(C) No. 000003...).
            pass

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

        if "Category" in details and not row["caseType"]:
            row["caseType"] = details["Category"]["text"]

        if "Status/Stage" in details:
            row["caseStatus"] = _normalize_status(details["Status/Stage"]["text"])

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
            url = a_tag.get("href", "") or ""
            date_str = a_tag.get_text(strip=True)
            cell_txt = tds[0].get_text(strip=True)
            purpose = cell_txt.replace(date_str, "").strip() or None

            history.append(
                {
                    "date": date_str,
                    "purpose": purpose,
                    "orderDetail": {"url": url} if url else None,
                    "actionTaken": None,
                    "encPath": None,
                }
            )

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
                continue

            app_no = tds_text[1]
            app_type = tds_text[2]
            filed_by = tds_text[4]
            filing_raw = tds_text[5]
            status_raw = tds_text[6] if len(tds_text) > 6 else ""

            filing_date_obj = _parse_date_obj(filing_raw)
            case_status = _normalize_status(status_raw)
            if status_raw and not any(
                kw in status_raw.upper()
                for kw in ["PENDING", "DISPOS", "ALLOWED", "DISMISSED"]
            ):
                if re.search(r"\d+/\d{4}", status_raw):
                    case_status = "Pending"

            apps.append(
                {
                    "filingNumber": app_no or None,
                    "caseType": app_type or None,
                    "caseTitle": filed_by or None,
                    "benchName": "New Delhi",
                    "caseStatus": case_status,
                    "courtNumber": None,
                    "applicationNumber": app_no or None,
                    "registrationDate": None,
                    "filingDate": filing_date_obj,
                    "nextListingDate": None,
                    "disposalDate": None,
                }
            )

    if apps:
        row["applicationDetails"] = json.dumps(apps)
    return row
