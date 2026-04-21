"""
Stable row keys for Google Sheets deduplication across all tabs of DC / HC / SC.

New rows should set uniqueness to PREFIX|unique_id (see parsers). Legacy rows
without uniqueness use a composite LEGACY|… key so duplicates are still avoided.
"""

from __future__ import annotations

import re
from typing import Any


def _norm(s: Any) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).strip()).upper()


def row_dedup_key(case: dict[str, Any]) -> str:
    """
    Return a stable uppercase key for this row.

    Prefer uniqueness when set (e.g. DC|HRHS030000552025).
    Otherwise LEGACY|courtType|benchName|caseNumber|registrationDate.
    """
    cf = str(case.get("uniqueness", "")).strip()
    if cf and "|" in cf:
        return cf.upper()

    ct = _norm(case.get("courtType"))
    bn = _norm(case.get("benchName"))
    cn = _norm(case.get("caseNumber"))
    rd = _norm(case.get("registrationDate"))
    return f"LEGACY|{ct}|{bn}|{cn}|{rd}"
