"""Normalization helpers for legal case data."""

from __future__ import annotations

import html
import logging
import re
from typing import Any

logger = logging.getLogger("legal_scraper.normalize")

_DISPOSED_KEYWORDS = frozenset({
    "disposed", "disposed off", "disposed off/compliance",
    "dismissed", "dismissed in default",
    "allowed", "consigned to recordroom",
    "decree", "decreed", "withdrawn", "settled",
    "transferred", "converted", "rejected",
    "abated", "not pressed", "compromised",
})



_PERSONAL_TITLE_RE = re.compile(
    r"^(?:"
    r"SHRIMATI|SHRI\.+|SHRI|SRIMATI|SRI\.+|SRI"
    r"|SMT\.+|SMT|SH\.+|SH"
    r"|MRS\.+|MRS|MR\.+|MR"
    r"|MS\.+|MS"
    r"|DR\.+|DR"
    r"|PROF\.+|PROF"
    r"|ADVOCATE|ADV\.+|ADV"
    r"|CA\.+|CS\.+"
    r"|LATE\s+(?:MR\.+|MRS\.+|SH\.+|SMT\.+|DR\.+|SHRI\.+?)?\s*"
    r")\s+",
    re.IGNORECASE,
)

_RP_PATTERN = re.compile(
    r"\bRESOLUTION\s+PROFESSIONAL\s+OF\b\s*(.+)$",
    re.IGNORECASE,
)

_TRAILING_NOISE_RE = re.compile(
    r"\s*[\(\-]\s*(?:BORROWER|CO[\s\-]?BORROWER|GUARANTOR|MORTGAGER|"
    r"PROPRIETOR|MORTGAGER|CO[\s\-]APPLICANT)\s*[\)\-]?\s*$",
    re.IGNORECASE,
)

_REP_BY_RE = re.compile(
    r"\s+(?:REP(?:RESENTED)?\.?\s+BY\s+ITS?|REPRESENTED\s+BY)\b.*$",
    re.IGNORECASE,
)
_THROUGH_ITS_RE = re.compile(
    r"\s+THROUGH\s+ITS?\b.*$",
    re.IGNORECASE,
)

_ROLE_ONLY_RE = re.compile(
    r"^(?:THE\s+)?(?:A\s+)?(?:"
    r"ASSISTANT\s+COMMISSIONER(?:\s+\(ST\))?"
    r"|BRANCH\s+MANAGER|BRANCH\s+HEAD|RELATIONSHIP\s+MANAGER"
    r"|RECOVERY\s+OFFICER|NODAL\s+OFFICER|AREA\s+MANAGER"
    r"|ZONAL\s+MANAGER|REGIONAL\s+MANAGER"
    r"|MANAGER|OFFICER|COMMISSIONER|REGISTRAR|INSPECTOR"
    r"|MAGISTRATE|JUDGE|ENGINEER|SECRETARY|CHAIRMAN|PRESIDENT|TREASURER|CLERK"
    r"|DIRECTOR|SUPERINTENDENT|PRINCIPAL|CONSERVATOR|MUNSIF|TAHSILDAR|SARPANCH"
    r"|COLLECTOR|AUTHORIZED\s+OFFICER|AUTHORISED\s+OFFICER"
    r"|INSPECTOR\s+OF\s+POLICE|SUPERINTENDENT\s+OF\s+POLICE"
    r"|SUB\s+INSPECTOR|DISTRICT\s+COLLECTOR|TEHSILDAR"
    r"|FINANCE\s+DEPARTMENT|DEPARTMENT"
    r")(?:\s+.*)?$",
    re.IGNORECASE,
)

_ENTITY_KEYWORDS_RE = re.compile(
    r"\b(?:LIMITED|LTD|LLP|BANK|INSURANCE|FINANCE|CORPORATION|ENTERPRISE|"
    r"INDUSTRY|GROUP|TRUST|SOCIETY|UNIVERSITY|COLLEGE|HOSPITAL|"
    r"AUTHORITY|BOARD|COMMISSION|TRIBUNAL)\b",
    re.IGNORECASE,
)


def _is_role_only(name: str) -> bool:
    """True when the name is just a role/title with no entity component."""
    if not name:
        return True
    if not _ROLE_ONLY_RE.match(name):
        return False
    return not _ENTITY_KEYWORDS_RE.search(name)


_COMPOUND_PATTERNS: list[re.Pattern] = [
    re.compile(
        r"^THE\s+(?:\w+\s+)?(?:HEAD\s*(?:/\s*)?)?(?:MANAGER|OFFICER|COMMISSIONER|"
        r"REGISTRAR|INSPECTOR|MAGISTRATE|JUDGE|ENGINEER|SECRETARY|CHAIRMAN|"
        r"PRESIDENT|TREASURER|CLERK|DIRECTOR|SUPERINTENDENT|PRINCIPAL|"
        r"CONSERVATOR|DEPARTMENT|DIVISION|OFFICE|BUREAU|WING|UNIT|SECTION)"
        r"(?:\s+OF)?\s+(?:M\s*/\s*S\.?\s*)?(.+)$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^.+?\s+(?:THE\s+)?(?:LIQUIDATOR|ADMINISTRATOR|INSOLVENCY\s+RESOLUTION\s+PROFESSIONAL|"
        r"IRP|RESOLUTION\s+PROFESSIONAL|INTERIM\s+RESOLUTION\s+PROFESSIONAL|"
        r"AUTHORIZED\s+REPRESENTATIVE|AUTHORISED\s+REPRESENTATIVE|"
        r"SPECIAL\s+ADMINISTRATOR)\s+(?:FOR|OF)\s+(?:M\s*/\s*S\.?\s*)?(.+)$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^.+?\s+(?:MANAGER|OFFICER|HEAD|DIRECTOR|SECRETARY|CHAIRMAN|COMMISSIONER)"
        r"\s+(?:OF|AT)\s+(?:M\s*/\s*S\.?\s*)?(.+)$",
        re.IGNORECASE,
    ),
]


def _extract_entity_from_compound(name: str) -> str | None:
    """Extract entity text from role+entity compound names."""
    upper = name.upper().strip()
    for pat in _COMPOUND_PATTERNS:
        m = pat.match(upper)
        if m:
            entity = m.group(1).strip()
            if entity and any(c.isalpha() for c in entity):
                return entity
    return None


def normalize_party_name(name: str | None) -> str:
    """Apply all normalization rules to a single party name."""
    if not name:
        return ""

    text = str(name).strip()
    if not text:
        return ""

    text = html.unescape(text)
    text = text.replace("&AMP;", "&").replace("&amp;", "&")

    text = re.sub(r"\(Not Applicable\)", "", text, flags=re.IGNORECASE)

    text = re.sub(r"\[.*?\]", "", text).strip()

    text = re.sub(r"^(\d+)\.\s*", "", text)
    text = re.sub(r"^(\d+)\)\s*", "", text)
    text = re.sub(r"^(\d+)\s+(?=[A-Z])", _strip_leading_number, text)

    text = text.upper().strip()

    text = re.sub(r"(\.+)([A-Z])", r"\1 \2", text)

    for _ in range(3):
        new = _PERSONAL_TITLE_RE.sub("", text).strip()
        if new == text:
            break
        text = new

    for prefix in ["M/S.", "M/S ", "M/S", "MS.", "MS "]:
        if text.startswith(prefix):
            text = text[len(prefix):].strip()
            break

    m_rp = _RP_PATTERN.search(text)
    if m_rp:
        text = m_rp.group(1).strip()

    text = re.sub(r"\(I\)", "(INDIA)", text)

    text = re.sub(
        r"[,\s]+(?:S/O|D/O|W/O|F/O|C/O|SON\s+OF|WIFE\s+OF|DAUGHTER\s+OF|FATHER\s+OF)\.?\s+.*$",
        "", text, flags=re.IGNORECASE,
    )

    text = _TRAILING_NOISE_RE.sub("", text)

    text = _REP_BY_RE.sub("", text).strip()

    text = _THROUGH_ITS_RE.sub("", text).strip()

    text = re.sub(
        r"\s*&\s*(ORS|OTHERS?|ANR|ANOTHERS?)\.?\s*$", "", text, flags=re.IGNORECASE
    )
    text = re.sub(
        r"\s+AND\s+\d*\s*(ORS|OTHERS?|ANR|ANOTHERS?)\.?\s*$",
        "", text, flags=re.IGNORECASE,
    )
    text = re.sub(
        r"AND\s+(ORS|OTHERS?|ANR|ANOTHERS?)\.?\s*$", "", text, flags=re.IGNORECASE
    )

    text = re.sub(r"\s+(AND|ETC\.?)\s+\d+\s*$", "", text, flags=re.IGNORECASE)

    suffix_order = [
        "PRIVATE LIMITED", "PVT LIMITED", "PVT LTD",
        "PRIVATE LTD", "LIMITED", "LTD", "LLP",
    ]
    for suf in suffix_order:
        idx = text.find(suf)
        if idx != -1:
            end = idx + len(suf)
            if end < len(text):
                text = text[:end].strip()
            break

    text = re.sub(r"\s+UNIT\s+\d+\s*$", "", text, flags=re.IGNORECASE).strip()

    text = text.rstrip(".,;: ")

    text = re.sub(r"\s+", " ", text).strip()

    if not any(c.isalpha() for c in text):
        return ""

    if _is_role_only(text):
        return ""

    return text


def _strip_leading_number(match: re.Match) -> str:
    num = match.group(1)
    if len(num) >= 2:
        return match.group(0)
    return ""


def normalize_multi_party(raw: str | None) -> str:
    """Normalize a multi-party field into quoted comma-separated names."""
    if not raw:
        return ""

    text = str(raw).strip()
    if not text:
        return ""

    names: list[str] = []

    if '"' in text:
        quoted_segments = re.findall(r'"([^"]+)"', text)
        if quoted_segments:
            for seg in quoted_segments:
                names.extend(_split_unquoted_parties(seg.strip()))
        else:
            names = _split_unquoted_parties(text)
    else:
        names = _split_unquoted_parties(text)

    seen: set[str] = set()
    result: list[str] = []
    for n in names:
        entity = _extract_entity_from_compound(n)
        targets = [entity] if entity else [n]

        for target in targets:
            cleaned = normalize_party_name(target)
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                result.append(f'"{cleaned}"')

    return ", ".join(result)


def _split_unquoted_parties(text: str) -> list[str]:
    """Split unquoted party text into individual names."""
    upper = text.upper()

    if re.search(r"\bRESOLUTION\s+PROFESSIONAL\s+OF\b", upper):
        return [text]

    if re.search(
        r"\b(?:LIQUIDATOR|ADMINISTRATOR|IRP|INSOLVENCY\s+RESOLUTION\s+PROFESSIONAL)"
        r"\s+(?:FOR|OF)\b", upper,
    ):
        return [text]
    if re.search(r"\bPERSONAL\s+GUARANTOR\s+OF\b", upper):
        return [text]

    if re.search(r",\s*(S/O|D/O|W/O|F/O|PROP|SOLE|SON|DAUGHTER|WIFE|FATHER|MOTHER|WIDOW)\b", upper):
        return [text]

    if re.search(r"\b(REPRESENTED\s+BY|REP\s+BY|THROUGH|PROPRIETOR\s+OF)\b", upper):
        parts = _smart_split(text)
        return parts if len(parts) > 1 else [text]

    return _smart_split(text)


def _smart_split(text: str) -> list[str]:
    """Split on commas that likely separate distinct parties."""
    raw_parts = [p.strip() for p in text.split(",") if p.strip()]
    if len(raw_parts) <= 1:
        return raw_parts

    merged: list[str] = []
    current = raw_parts[0]

    continuation_re = re.compile(
        r"^(S/O|D/O|W/O|F/O|C/O|REP\.?\s|REP\s|THROUGH|PROP\.?|PARTNER|"
        r"AGED|OCC|R/O|AT|AND\s+\d|ETC)\b",
        re.IGNORECASE,
    )

    address_re = re.compile(
        r"^(SECTOR|SEC\-|SEC\.|PHASE|PIN\b|P\.S\.|P\.O\.|NEAR\b|OPP\.|BEHIND\b|HOUSE|H\.NO|FLAT\b|ROOM\b|SHOP\b|BLOCK\b|BUILDING\b)|"
        r"\b(COLONY|NAGAR|ENCLAVE|VIHAR|COMPLEX|FLOOR|APARTMENT|TOWER|CHAMBER|MARG|STREET|ROAD)$|"
        r"^(MUMBAI|DELHI|NEW\s+DELHI|NOIDA|GURUGRAM|BENGALURU|CHENNAI|KOLKATA|PUNE|HYDERABAD|AHMEDABAD|JAIPUR|CHANDIGARH|LUCKNOW|KANPUR|AGRA|VARANASI|ALLAHABAD)$",
        re.IGNORECASE
    )

    for part in raw_parts[1:]:
        if continuation_re.match(part) or address_re.search(part):
            current += ", " + part
        else:
            merged.append(current)
            current = part

    merged.append(current)
    return merged


def normalize_case_status(raw: str | None) -> str:
    """Map any case status to 'Pending' or 'Disposed'.
    
    Returns "" for empty strings (important for continuation rows).
    """
    if not raw or not str(raw).strip():
        return ""
    status = str(raw).strip().lower()
    if status in _DISPOSED_KEYWORDS:
        return "Disposed"
    if "dispos" in status or "dismiss" in status or "allowed" in status:
        return "Disposed"
    return "Pending"


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Apply all normalization rules to a scraped row dict. Mutates in-place."""

    party_fields = (
        "respondent", "petitioner", "otherRespondent", "otherPetitioner"
    )
    has_party = any(row.get(f) and str(row.get(f)).strip() for f in party_fields)
    
    if not has_party:
        if not row.get("caseStatus"):
            row["caseStatus"] = ""
        return row

    advocate_fields = ("respondentAdvocate", "petitionerAdvocate")
    all_name_fields = party_fields + advocate_fields
    
    for field in all_name_fields:
        row[field] = normalize_multi_party(row.get(field))

    row["caseStatus"] = normalize_case_status(row.get("caseStatus"))
    if not row["caseStatus"]:
        row["caseStatus"] = "Pending"
        
    return row
