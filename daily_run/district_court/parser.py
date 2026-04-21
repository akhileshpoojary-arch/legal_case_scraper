
from __future__ import annotations

import json
import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from utils.date_utils import format_date

logger = logging.getLogger("legal_scraper.daily_run.dc.parser")

_MONTH_FULL = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

def _parse_ordinal_date(raw: str | None) -> str:
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
    if not raw:
        return ""
    return format_date(raw) or _parse_ordinal_date(raw)

def _normalize_dc_status(raw: str | None) -> str:
    if not raw:
        return "Pending"
    if "dispos" in str(raw).strip().lower():
        return "Disposed"
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
    if not element:
        return [], []
    raw_html = str(element)
    raw_html = re.sub(r"</?br\s*/?>", "\n", raw_html, flags=re.IGNORECASE)
    raw_html = re.sub(r"<[^>]+>", "", raw_html)
    raw_html = (
        raw_html.replace("\xa0", " ")
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
    html = re.sub(r"</br\s*>", "<br/>", html, flags=re.IGNORECASE)
    soup = BeautifulSoup(html, "html.parser")
    out: dict[str, Any] = {}

    if soup.find(string=re.compile(r"SQL ERROR", re.IGNORECASE)):
        out["_parse_error"] = "sql_error"
        return out

    case_table = soup.find(
        "table",
        class_=lambda c: c and "case_details_table" in c,
    )
    if case_table:
        for row in _direct_rows(case_table):
            cells = [
                td.get_text(" ", strip=True) for td in row.find_all(["td", "th"])
            ]
            i = 0
            while i < len(cells) - 1:
                key = re.sub(r"\s+", " ", cells[i]).strip(":").strip()
                val = cells[i + 1].strip() if i + 1 < len(cells) else ""
                if key:
                    out[key] = val
                i += 2

    status_table = soup.find("table", class_="case_status_table") or soup.find(
        "table", class_="table_r"
    )
    if status_table:
        for row in _direct_rows(status_table):
            cells = [
                td.get_text(" ", strip=True) for td in row.find_all(["td", "th"])
            ]
            if len(cells) >= 2:
                key = re.sub(r"\s+", " ", cells[0]).strip(":").strip()
                if key:
                    out[key] = cells[1].strip()

    pet_names, pet_advs = _parse_party_block(
        soup.select_one(".Petitioner_Advocate_table")
    )
    out["_pet_names"], out["_pet_advs"] = pet_names, pet_advs

    res_names, res_advs = _parse_party_block(
        soup.select_one(".Respondent_Advocate_table")
    )
    out["_res_names"], out["_res_advs"] = res_names, res_advs

    hearings: list[dict[str, Any]] = []
    history_table = soup.find("table", class_=lambda c: c and "history_table" in c)
    if history_table:
        header_cells: list[str] = []
        thead = history_table.find("thead")
        if thead:
            hr = thead.find("tr")
            if hr:
                header_cells = [
                    re.sub(r"\s+", " ", th.get_text(strip=True)).lower()
                    for th in hr.find_all(["th", "td"])
                ]
        idx_hearing = 2
        idx_purpose = 3
        idx_judge = 0
        idx_business = 1
        for i, h in enumerate(header_cells):
            if "hearing" in h and "date" in h:
                idx_hearing = i
            elif "purpose" in h:
                idx_purpose = i
            elif "judge" in h:
                idx_judge = i
            elif "business" in h:
                idx_business = i

        for row in _direct_rows(history_table):
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if not cells:
                continue
            if len(cells) >= 4:
                judge = re.sub(r"\s+", " ", cells[idx_judge]).strip()
                hearing_date_raw = cells[idx_hearing].replace("\xa0", "").strip()
                purpose = (
                    cells[idx_purpose].strip()
                    if len(cells) > idx_purpose
                    else ""
                )
            elif len(cells) == 3:
                judge = re.sub(r"\s+", " ", cells[0]).strip()
                hearing_date_raw = cells[1].replace("\xa0", "").strip()
                purpose = cells[2].strip()
            else:
                continue

            date_str = format_date(hearing_date_raw) or _parse_any_date(hearing_date_raw)
            if date_str:
                hearings.append(
                    {
                        "date": date_str,
                        "purpose": purpose if purpose else None,
                        "judge": judge if judge else None,
                    }
                )
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

def _extract_dc_court_number(raw: str) -> str:
    if not raw:
        return ""
    s = re.sub(r"<[^>]+>", "", str(raw))
    s = re.sub(r"\s+", " ", s).strip()
    m = re.match(r"^\s*(\d+)\s*[-–—]", s)
    if m:
        return m.group(1)
    m2 = re.match(r"^\s*(\d+)\s*$", s)
    if m2:
        return m2.group(1)
    m3 = re.search(r"\b(\d{1,3})\s*[-–—]\s*[A-Za-z]", s)
    if m3:
        return m3.group(1)
    return ""

def _case_type_abbrev(case_type_cell: str) -> str:
    if not case_type_cell:
        return ""
    left = case_type_cell.strip().split("-", 1)[0].strip()
    left = re.sub(r"\s+", "", left)
    return left.upper()[:16]

def _normalize_list_case_ref(raw: str) -> str:
    """CHA/534/2025 style from results table or aria-label."""
    if not raw:
        return ""
    s = re.sub(r"\s+", "", str(raw).strip())
    m = re.search(r"([A-Za-z]{1,12}/\d{1,8}/\d{4})", s)
    if m:
        return m.group(1).upper()
    return ""

def _looks_like_cnr_token(s: str) -> bool:
    """Long alphanumeric case id (e.g. CNR) — not a display TYPE/NO/YEAR ref."""
    t = re.sub(r"\s+", "", str(s))
    return bool(t) and "/" not in t and len(t) >= 10 and t.isalnum()

def _finalize_dc_case_number(
    case_number: str,
    abbr: str,
    reg_num_raw: str,
) -> str:
    """
    District Court display ref: extracts ONLY the numeric portion from a string
    that looks like TYPE/NUMBER/YEAR or NUMBER/YEAR.
    """
    s = (case_number or "").strip()
    if _looks_like_cnr_token(s):
        s = ""

    ref = _normalize_list_case_ref(s)
    if ref:
        res = ref
    else:
        rn = re.sub(r"\s+", "", str(reg_num_raw or ""))
        if abbr and rn and re.fullmatch(r"\d{1,12}/\d{4}", rn):
            res = f"{abbr}/{rn}"
        elif abbr and rn and "/" in rn and not re.match(r"^[A-Za-z]", rn):
            res = f"{abbr}/{rn}"
        else:
            res = s if s else (f"{abbr}/{rn}" if abbr and rn else "")

    # Extract numeric part: typically the part before the year in TYPE/NUMBER/YEAR
    if "/" in res:
        parts = res.split("/")
        # We look for the last part that contains digits (excluding the year at the end)
        for p in reversed(parts[:-1]):
            if any(c.isdigit() for c in p):
                return p
        # Fallback if no numeric part found before last slash
        return parts[0]

    return res

def build_dc_row(
    detail: dict | None,
    fallback: dict,
    court: dict[str, Any],
) -> dict[str, Any]:
    """Build a normalized row from parsed detail + search fallback."""
    search = fallback
    hist = detail or {}

    pet_names: list[str] = hist.get("_pet_names") or []
    res_names: list[str] = hist.get("_res_names") or []
    pet_advs: list[str] = hist.get("_pet_advs") or []
    res_advs: list[str] = hist.get("_res_advs") or []

    # Fallback from label
    if not pet_names and not res_names:
        label = search.get("case_label", "")
        if "Vs" in label:
            parts = label.split("Vs")
            pet = re.sub(
                r"^view case.*?,", "", parts[0], flags=re.IGNORECASE
            ).replace("<br>", "").strip()
            res = parts[1].replace("</br>", "").strip()
            if pet:
                pet_names = [pet]
            if res:
                res_names = [res]

    petitioner = _format_name_list(pet_names)
    respondent = _format_name_list(res_names)
    petitioner_adv = _format_name_list(pet_advs)
    respondent_adv = _format_name_list(res_advs)

    first_hearing = _parse_any_date(_g(hist, "First Hearing Date"))
    reg_date_raw = _g(hist, "Registration Date")
    reg_date = format_date(reg_date_raw) or _parse_any_date(reg_date_raw)
    filing_raw = _g(hist, "Filing Date")
    filing_date = format_date(filing_raw) or _parse_any_date(filing_raw)
    next_raw = _g(hist, "Next Hearing Date")
    next_hearing = format_date(next_raw) or _parse_any_date(next_raw)
    registration_date = reg_date or filing_date or first_hearing

    hearings: list[dict] = hist.get("_hearings", [])
    listing_history = json.dumps(hearings) if hearings else ""

    raw_status = _g(hist, "Case Status", "Stage of Case")
    if raw_status:
        case_status = _normalize_dc_status(raw_status)
    else:
        decision = _g(search, "date_of_decision") or _g(hist, "Decision Date")
        case_status = "Disposed" if decision else "Pending"

    reg_num_raw = _g(hist, "Registration Number")
    if not reg_num_raw:
        label = search.get("case_label", "")
        m = re.search(
            r"Registration Number\s+(.*?)\s*,", label, flags=re.IGNORECASE
        )
        if m:
            reg_num_raw = m.group(1).strip()

    ct_line = _g(hist, "Case Type")
    if not ct_line:
        ct_line = str(court.get("selected_case_type") or "").strip()
    abbr = _case_type_abbrev(ct_line)

    list_ref = _normalize_list_case_ref(search.get("list_case_ref", ""))
    if not list_ref:
        list_ref = _normalize_list_case_ref(
            search.get("case_label", "").replace("<br>", " ").replace("</br>", " ")
        )

    if list_ref:
        case_number = list_ref
    elif reg_num_raw and abbr:
        rn = re.sub(r"\s+", "", reg_num_raw)
        if re.fullmatch(r"\d{1,12}/\d{4}", rn):
            case_number = f"{abbr}/{rn}"
        elif re.match(r"^[A-Za-z]{1,12}/\d{1,8}/\d{4}", rn):
            case_number = _normalize_list_case_ref(rn) or rn.upper()
        else:
            case_number = f"{abbr}/{rn}" if not rn.upper().startswith(abbr + "/") else rn
    elif reg_num_raw:
        rn = re.sub(r"\s+", "", reg_num_raw)
        case_number = f"{abbr}/{rn}" if abbr and re.fullmatch(r"\d{1,12}/\d{4}", rn) else rn
    else:
        case_number = ""

    case_number = _finalize_dc_case_number(case_number, abbr, reg_num_raw)

    cino = str(search.get("cino", "")).strip()
    internal_no = str(search.get("case_no", "")).strip()
    case_file = f"DC|{cino}" if cino else f"DC|NO_CINO|{internal_no}"

    court_num = _g(hist, "Court Number and Judge")
    court_num = _extract_dc_court_number(court_num)
    selected_case_type = _g(court, "selected_case_type")
    final_case_type = selected_case_type or _g(hist, "Case Type")

    state_name = court.get("state_name", "INDIA")
    dist_name = court.get("dist_name", "")
    complex_name = court.get("complex_name", "")
    est_name = court.get("est_name")
    est_str = est_name if est_name else "null"
    bench_val = f"{state_name} (dis: {dist_name}, comp:{complex_name} est:{est_str})"

    return {
        "partyName": "",
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
        "location": "INDIA",
        "courtType": "District Court",
        "benchName": bench_val,
        "caseType": final_case_type,
        "caseStatus": case_status,
        "uniqueness": case_file,
        "listingHistory": listing_history,
        "applicationDetails": "",
        "status": "PUBLISHED",
    }
