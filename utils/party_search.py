"""
Flexible party-name matching for sheet search (company / advocate style names).

Matches query variants like:
  PRIVATE LIMITED ↔ PVT LTD, LIMITED ↔ LTD / LIM, INDUSTRIES ↔ IND, etc.
"""

from __future__ import annotations

import re
from typing import Pattern


def _token_pattern(token: str) -> str:
    t = token.upper().strip().rstrip(".,;:")
    if not t:
        return ""
    if t in ("PRIVATE", "PRIV"):
        return r"(?:PRIVATE|PRIV(?:ATE)?\.?|PVT\.?|PRV\.?|PRT\.?)"
    if t in ("LIMITED", "LTD"):
        return r"(?:LIMITED|LTD\.?|LIM\.?)"
    if t in ("INDUSTRIES", "INDUSTRY", "IND"):
        return r"(?:INDUSTRIES|INDUSTRY|IND\.?)"
    if t in ("AND", "&"):
        return r"(?:AND|&)"
    if t in ("COMPANY", "CO"):
        return r"(?:COMPANY|CO\.?)"
    return re.escape(t)


def compile_party_search_pattern(query: str) -> Pattern[str]:
    """
    Build a regex that matches the query with common legal-name abbreviations.

    Uses re.search (substring OK) with IGNORECASE.
    """
    raw = (query or "").strip()
    if not raw:
        return re.compile(r"$^")

    tokens = re.findall(r"\S+", raw.upper())
    parts = [_token_pattern(t) for t in tokens if t]
    parts = [p for p in parts if p]
    if not parts:
        return re.compile(re.escape(raw), re.IGNORECASE)

    # Allow flexible whitespace between tokens
    body = r"\s+".join(parts)
    return re.compile(body, re.IGNORECASE | re.UNICODE)


def compile_party_fallback_patterns(query: str) -> list[Pattern[str]]:
    """
    Extra patterns: first two words (e.g. SARAVI INDUSTRIES) and PVT LTD phrasing.
    """
    q = re.sub(r"\s+", " ", (query or "").strip().upper())
    if len(q) < 4:
        return []

    out: list[Pattern[str]] = []
    words = q.split()
    if len(words) >= 2:
        stem = " ".join(words[:2])
        out.append(re.compile(re.escape(stem), re.IGNORECASE))

    collapsed = q.replace("PRIVATE LIMITED", "PVT LTD")
    if collapsed != q:
        out.append(compile_party_search_pattern(collapsed))

    return out


def cell_matches_party_query(cell: str, query: str) -> bool:
    if not cell or not query:
        return False
    primary = compile_party_search_pattern(query)
    if primary.search(cell):
        return True
    for p in compile_party_fallback_patterns(query):
        if p.search(cell):
            return True
    return False
