"""Parse High Court detail HTML into the unified output schema."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from scrapers.base import BaseParser, clean_party_name
from utils.date_utils import format_date

logger = logging.getLogger("legal_scraper.hc.parser")

_HC_DETAIL_BASE = "https://hcservices.ecourts.gov.in/hcservices/cases_qry/"

_MONTH_FULL = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

def _parse_ordinal_date(raw: str | None) -> str:
    """Parse ordinal date strings into standard date format."""
    if not raw:
        return ""
    s = str(raw).strip()
    if not s or s.lower() in ("", "na", "null", "none"):
        return ""
    s_clean = re.sub(r"(\d+)\s*(st|nd|rd|th)", r"\1", s, flags=re.IGNORECASE)
    parts = s_clean.strip().split()
    if len(parts) == 3:
        try:
            day = int(parts[0])
            month = _MONTH_FULL.get(parts[1].lower())
            year = int(parts[2])
            if month:
                return format_date(f"{day}-{month}-{year}")
        except (ValueError, AttributeError):
            pass
    return ""


def _parse_any_date(raw: str | None) -> str:
    """Try standard formats first (central), then ordinal format."""
    if not raw:
        return ""
    return format_date(raw) or _parse_ordinal_date(raw)


def _normalize_hc_status(raw: str | None) -> str:
    """Map any HC case-status string to exactly 'Disposed' or 'Pending'."""
    if not raw:
        return "Pending"
    if "dispos" in str(raw).strip().lower():
        return "Disposed"
    return "Pending"


def _format_name_list(names: list[str]) -> str:
    """Deduplicate and format names as quoted comma-separated values."""
    seen: set[str] = set()
    result: list[str] = []
    for raw in names:
        n = re.sub(r"\s+", " ", raw).strip().strip(",").strip()
        if n and n not in seen:
            seen.add(n)
            result.append(f'"{n}"')
    return ", ".join(result)


def _parse_party_block(element) -> tuple[list[str], list[str]]:
    """Parse party and advocate blocks from detail HTML."""
    if not element:
        return [], []

    raw_html = str(element)
    raw_html = re.sub(r"</?br\s*/?>", "\n", raw_html, flags=re.IGNORECASE)
    raw_html = re.sub(r"<[^>]+>", "", raw_html)
    raw_html = (
        raw_html
        .replace("\xa0", " ")
        .replace("&amp;", "&")
        .replace("&nbsp;", " ")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
    )

    lines = [l.strip() for l in raw_html.split("\n") if l.strip()]

    parties: list[str] = []
    advocates: list[str] = []
    seen_parties: set[str] = set()
    seen_advocates: set[str] = set()
    last_was_advocate = False

    for line in lines:
        if re.match(r"^\d+\)", line):
            party = re.sub(r"^\d+\)\s*", "", line).strip().rstrip(",").strip()
            party = re.sub(r"\s+", " ", party)
            if party and party not in seen_parties:
                seen_parties.add(party)
                parties.append(party)
            last_was_advocate = False

        elif re.match(r"^Advocate\s*[-]", line, re.IGNORECASE):
            adv_part = re.sub(
                r"^Advocate\s*[-]\s*", "", line, flags=re.IGNORECASE
            )
            for seg in adv_part.split(","):
                a = seg.strip().rstrip(",").strip()
                if a and a not in seen_advocates:
                    seen_advocates.add(a)
                    advocates.append(a)
            last_was_advocate = True

        elif last_was_advocate:
            a = re.sub(r"\s+", " ", line).strip().rstrip(",").strip()
            if a and a not in seen_advocates:
                seen_advocates.add(a)
                advocates.append(a)
    return parties, advocates


def _direct_rows(table_el) -> list:
    """Return direct child rows, excluding nested-table rows."""
    if not table_el:
        return []
    rows: list = []
    for child in table_el.children:
        if not hasattr(child, "name"):
            continue
        if child.name == "tr":
            rows.append(child)
        elif child.name in ("tbody", "thead", "tfoot"):
            for sub in child.children:
                if hasattr(sub, "name") and sub.name == "tr":
                    rows.append(sub)
    return rows


def parse_detail_html(html: str) -> dict[str, Any]:
    """Parse case detail HTML into normalized fields."""
    # Normalize malformed </br> end tags before BS4 sees them.
    # html.parser silently drops </br> and merges surrounding text,
    # so replace with proper <br/> first.
    html = re.sub(r"</br\s*>", "<br/>", html, flags=re.IGNORECASE)

    soup = BeautifulSoup(html, "html.parser")
    out: dict[str, Any] = {}

    if soup.find(string=re.compile(r"SQL ERROR", re.IGNORECASE)):
        logger.debug("HC detail page returned SQL error — skipping parse")
        out["_parse_error"] = "sql_error"
        return out

    case_table = soup.find("table", class_="case_details_table")
    if case_table:
        for row in _direct_rows(case_table):
            cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
            i = 0
            while i < len(cells) - 1:
                key = re.sub(r"\s+", " ", cells[i]).strip(":").strip()
                val = cells[i + 1].strip() if i + 1 < len(cells) else ""
                if key:
                    out[key] = val
                i += 2

    status_table = soup.find("table", class_="table_r")
    if status_table:
        for row in _direct_rows(status_table):
            cells = [
                td.get_text(" ", strip=True)
                for td in row.find_all(["td", "th"])
            ]
            if len(cells) >= 2:
                key = re.sub(r"\s+", " ", cells[0]).strip(":").strip()
                if key:
                    out[key] = cells[1].strip()

    pet_el = soup.select_one(".Petitioner_Advocate_table")
    pet_names, pet_advs = _parse_party_block(pet_el)
    out["_pet_names"] = pet_names
    out["_pet_advs"] = pet_advs

    res_el = soup.select_one(".Respondent_Advocate_table")
    res_names, res_advs = _parse_party_block(res_el)
    out["_res_names"] = res_names
    out["_res_advs"] = res_advs

    order_pdf: dict[str, str] = {}
    order_table = soup.find("table", class_="order_table")
    if order_table:
        for row in order_table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
            raw_date = cells[3].get_text(strip=True).replace("\xa0", "").strip()
            link = cells[4].find("a", href=True)
            if raw_date and link:
                href = link["href"]
                href = href.replace("cases_qry/", "")
                if href and not href.startswith("http"):
                    href = _HC_DETAIL_BASE + href.lstrip("/")
                if href:
                    order_pdf[raw_date] = href
    out["_order_pdf"] = order_pdf

    hearings: list[dict[str, Any]] = []
    history_table = soup.find("table", class_="history_table")
    if history_table:
        for row in _direct_rows(history_table):
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 4:
                continue

            business_date_raw = cells[2].replace("\xa0", "").strip()
            judge = re.sub(r"\s+", " ", cells[1]).strip()
            purpose = cells[4].strip() if len(cells) > 4 else ""

            date_str = format_date(business_date_raw)
            if not date_str:
                continue

            entry: dict[str, Any] = {
                "date": date_str,
                "purpose": purpose if purpose else None,
                "judge": judge if judge else None,
            }

            pdf_url = order_pdf.get(business_date_raw)
            if pdf_url:
                entry["orderUrl"] = pdf_url

            hearings.append(entry)

    out["_hearings"] = hearings
    return out


def _g(d: dict, *keys: str) -> str:
    """Return the first non-empty, non-null value for any of the given keys."""
    for k in keys:
        v = d.get(k)
        if v not in (None, "", "NA", "null", "None", 0):
            s = str(v).strip()
            if s and s not in ("0", "NA", "null", "None"):
                return s
    return ""


class HCParser(BaseParser):
    """Maps High Court detail + search data to the 19-column schema."""

    SOURCE = "HIGH_COURT"

    def build_row(
        self,
        detail: dict | None,
        fallback: dict,
        party_name: str,
        court: dict[str, Any],
        **extra: Any,
    ) -> dict[str, Any]:
        """Build a normalized row from detail data with fallback fields."""
        search = fallback
        hist = detail or {}

        if hist.get("_parse_error"):
            logger.debug(
                "Detail parse error for '%s': %s", party_name, hist["_parse_error"]
            )

        pet_names: list[str] = hist.get("_pet_names") or []
        res_names: list[str] = hist.get("_res_names") or []
        pet_advs: list[str] = hist.get("_pet_advs") or []
        res_advs: list[str] = hist.get("_res_advs") or []

        if not pet_names:
            raw = _g(search, "pet_name")
            if raw:
                pet_names = [raw]
        if not res_names:
            raw = _g(search, "res_name", "extra_party")
            if raw:
                res_names = [raw]

        petitioner = _format_name_list(pet_names)
        respondent = _format_name_list(res_names)
        petitioner_adv = _format_name_list(pet_advs)
        respondent_adv = _format_name_list(res_advs)

        first_hearing = _parse_any_date(_g(hist, "First Hearing Date"))
        reg_date = format_date(_g(hist, "Registration Date"))
        filing_date = format_date(_g(hist, "Filing Date"))
        next_hearing = format_date(_g(hist, "Next Hearing Date"))

        registration_date = reg_date or filing_date or first_hearing

        hearings: list[dict] = hist.get("_hearings", [])
        listing_history = json.dumps(hearings) if hearings else ""

        raw_status = _g(hist, "Case Status", "Stage of Case")
        if raw_status:
            case_status = _normalize_hc_status(raw_status)
        else:
            decision = _g(search, "date_of_decision") or _g(hist, "Decision Date")
            case_status = "Disposed" if decision else "Pending"

        case_number = _g(hist, "Registration Number")
        if not case_number:
            type_name = _g(search, "type_name")
            case_no2 = _g(search, "case_no2")
            case_year = _g(search, "case_year")
            if type_name and case_no2:
                case_number = f"{type_name}/{case_no2}/{case_year}"
            else:
                case_number = _g(search, "case_no")

        bench_name = extra.get("bench_name", "")
        hc_name = extra.get("hc_name", "")

        return {
            # courtNumber: HC pages have no "Court Number" field.
            # Coram (judges panel) is NOT the court number — always left empty.
            "partyName": party_name,
            "caseNumber": case_number,
            "courtNumber": "",
            "registrationDate": registration_date,
            "nextListingDate": next_hearing,
            "respondent": respondent,
            "otherRespondent": "",
            "respondentAdvocate": respondent_adv,
            "petitioner": petitioner,
            "otherPetitioner": "",
            "petitionerAdvocate": petitioner_adv,
            "location": "INDIA",
            "courtType": f"HIGH_COURT ({hc_name})" if hc_name else "HIGH_COURT",
            "benchName": bench_name,
            "caseType": _g(search, "type_name"),
            "caseStatus": case_status,
            "uniqueness": "",
            "listingHistory": listing_history,
            "applicationDetails": "",
            "status": "PUBLISHED",
        }

    def build_fallback(self, search_result: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in search_result.items() if v is not None}
