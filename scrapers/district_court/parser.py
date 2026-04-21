"""
District Court Parser — transforms case detail HTML into the 19-column schema.

Based on HCParser since the HTML returned by viewHistory for District Court
has exact same structure (case_details_table, Petitioner_Advocate_table).
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from scrapers.base import BaseParser, clean_party_name
from utils.date_utils import format_date
# Import helpers precisely from high_court to avoid duplicating generic date & table parsing logic if possible,
# but it is safer to duplicate or replicate it. We will duplicate the logic since HC parser lacks __all__ exports.
# We will use exactly similar parsing code.

logger = logging.getLogger("legal_scraper.dc.parser")


_MONTH_FULL = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_ordinal_date(raw: str | None) -> str:
    if not raw: return ""
    s = str(raw).strip()
    if not s or s.lower() in ("", "na", "null", "none"): return ""
    s_clean = re.sub(r"(\d+)\s*(st|nd|rd|th)", r"\1", s, flags=re.IGNORECASE)
    parts = s_clean.strip().split()
    if len(parts) == 3:
        try:
            day = int(parts[0])
            month = _MONTH_FULL.get(parts[1].lower())
            year = int(parts[2])
            if month:
                # Use format_date on a generated string to ensure central logic/padding
                return format_date(f"{day}-{month}-{year}")
        except (ValueError, AttributeError):
            pass
    return ""


def _parse_any_date(raw: str | None) -> str:
    """Try standard formats first (central), then ordinal format."""
    if not raw:
        return ""
    return format_date(raw) or _parse_ordinal_date(raw)


def _normalize_dc_status(raw: str | None) -> str:
    if not raw: return "Pending"
    if "dispos" in str(raw).strip().lower(): return "Disposed"
    return "Pending"


def _format_name_list(names: list[str]) -> str:
    seen: set[str] = set()
    result: list[str] = []
    for raw in names:
        n = re.sub(r"\s+", " ", raw).strip().strip(",").strip()
        if n and n not in seen:
            seen.add(n)
            result.append(f'"{n}"')
    return ", ".join(result)


def _parse_party_block(element) -> tuple[list[str], list[str]]:
    if not element: return [], []
    raw_html = str(element)
    raw_html = re.sub(r"</?br\s*/?>", "\n", raw_html, flags=re.IGNORECASE)
    raw_html = re.sub(r"<[^>]+>", "", raw_html)
    raw_html = raw_html.replace("\xa0", " ").replace("&amp;", "&").replace("&nbsp;", " ").replace("&lt;", "<").replace("&gt;", ">")

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
            adv_part = re.sub(r"^Advocate\s*[-]\s*", "", line, flags=re.IGNORECASE)
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
    if not table_el: return []
    rows: list = []
    for child in table_el.children:
        if not hasattr(child, "name"): continue
        if child.name == "tr": rows.append(child)
        elif child.name in ("tbody", "thead", "tfoot"):
            for sub in child.children:
                if hasattr(sub, "name") and sub.name == "tr":
                    rows.append(sub)
    return rows


def parse_detail_html(html: str) -> dict[str, Any]:
    html = re.sub(r"</br\s*>", "<br/>", html, flags=re.IGNORECASE)
    soup = BeautifulSoup(html, "html.parser")
    out: dict[str, Any] = {}

    if soup.find(string=re.compile(r"SQL ERROR", re.IGNORECASE)):
        out["_parse_error"] = "sql_error"
        return out

    case_table = soup.find("table", class_="case_details_table")
    if case_table:
        for row in _direct_rows(case_table):
            cells = [td.get_text(" ", strip=True) for td in row.find_all(["td", "th"])]
            i = 0
            while i < len(cells) - 1:
                key = re.sub(r"\s+", " ", cells[i]).strip(":").strip()
                val = cells[i + 1].strip() if i + 1 < len(cells) else ""
                if key: out[key] = val
                i += 2

    status_table = soup.find("table", class_="case_status_table") or soup.find("table", class_="table_r")
    if status_table:
        for row in _direct_rows(status_table):
            cells = [td.get_text(" ", strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) >= 2:
                key = re.sub(r"\s+", " ", cells[0]).strip(":").strip()
                if key: out[key] = cells[1].strip()

    pet_names, pet_advs = _parse_party_block(soup.select_one(".Petitioner_Advocate_table"))
    out["_pet_names"], out["_pet_advs"] = pet_names, pet_advs

    res_names, res_advs = _parse_party_block(soup.select_one(".Respondent_Advocate_table"))
    out["_res_names"], out["_res_advs"] = res_names, res_advs

    hearings: list[dict[str, Any]] = []
    history_table = soup.find("table", class_="history_table")
    if history_table:
        for row in _direct_rows(history_table):
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 4: continue
            business_date_raw = cells[2].replace("\xa0", "").strip()
            judge = re.sub(r"\s+", " ", cells[1]).strip()
            purpose = cells[4].strip() if len(cells) > 4 else ""

            date_str = format_date(business_date_raw)
            if date_str:
                hearings.append({
                    "date": date_str,
                    "purpose": purpose if purpose else None,
                    "judge": judge if judge else None,
                })
    out["_hearings"] = hearings
    return out


def _g(d: dict, *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if v not in (None, "", "NA", "null", "None", 0):
            s = str(v).strip()
            if s and s not in ("0", "NA", "null", "None"):
                return s
    return ""


class DCParser(BaseParser):
    SOURCE = "DISTRICT_COURT"

    def build_row(
        self,
        detail: dict | None,
        fallback: dict,
        party_name: str,
        court: dict[str, Any],
        **extra: Any,
    ) -> dict[str, Any]:
        search = fallback
        hist = detail or {}

        pet_names: list[str] = hist.get("_pet_names") or []
        res_names: list[str] = hist.get("_res_names") or []
        pet_advs: list[str] = hist.get("_pet_advs") or []
        res_advs: list[str] = hist.get("_res_advs") or []

        # From fallback label
        # e.g aria-label: 'view case C.C./15887/2025, Excise P.S Peenya<br>Vs</br>Govindapa'
        if not pet_names and not res_names:
            label = search.get("case_label", "")
            if "Vs" in label:
                parts = label.split("Vs")
                pet = re.sub(r"^view case.*?,", "", parts[0], flags=re.IGNORECASE).replace("<br>", "").strip()
                res = parts[1].replace("</br>", "").strip()
                if pet: pet_names = [pet]
                if res: res_names = [res]

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
            case_status = _normalize_dc_status(raw_status)
        else:
            decision = _g(search, "date_of_decision") or _g(hist, "Decision Date")
            case_status = "Disposed" if decision else "Pending"

        case_number = _g(hist, "Registration Number")
        if not case_number:
            case_number = search.get("case_no", "") or search.get("cino", "")

        court_num = _g(hist, "Court Number and Judge")

        return {
            "partyName": party_name,
            "caseNumber": case_number,
            "courtNumber": court_num,
            "registrationDate": registration_date,
            "nextListingDate": next_hearing,
            "respondent": respondent,
            "otherRespondent": "",
            "respondentAdvocate": respondent_adv,
            "petitioner": petitioner,
            "otherPetitioner": "",
            "petitionerAdvocate": petitioner_adv,
            "location": court.get("state_name", "INDIA"),
            "courtType": f"DISTRICT_COURT ({court.get('dist_name', '')})",
            "benchName": court.get("est_name", ""),
            "caseType": _g(hist, "Case Type"),
            "caseStatus": case_status,
            "uniqueness": "",
            "listingHistory": listing_history,
            "applicationDetails": "",
            "status": "PUBLISHED",
        }

    def build_fallback(self, search_result: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in search_result.items() if v is not None}
