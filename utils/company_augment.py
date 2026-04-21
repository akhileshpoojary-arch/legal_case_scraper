"""
Company name augmentation for sheet search.

The name in B1 (Search Console) is the proper/full form, but names stored
in Google Sheets may use short forms (PVT instead of PRIVATE, etc.).
This module generates all plausible search variants so the sheet-search
scraper can find matches regardless of abbreviation style.

Pipeline:
  1. Normalize  → strip numbering, M/S prefix, punctuation, designations
  2. Tokenize   → split into non-suffix tokens
  3. Expand     → generate acronym variants (joined, split, dotted)
  4. Suffix     → legal suffix expansion table
"""

from __future__ import annotations

import re
from typing import Any


# ═══════════════════════════════════════════════════════════════
#  LEGAL SUFFIX EXPANSION TABLE
# ═══════════════════════════════════════════════════════════════
# Each key maps to a tuple of alternative suffixes that mean the same thing.
_SUFFIX_EXPANSION: dict[str, tuple[str, ...]] = {
    "PRIVATE LIMITED": ("PVT LTD", "PVT LIMITED", "PRIVATE LTD", "LIMITED", "LTD"),
    "PVT LTD": ("PRIVATE LIMITED", "PRIVATE LTD", "PVT LIMITED", "LIMITED", "LTD"),
    "PVT LIMITED": ("PRIVATE LIMITED", "PRIVATE LTD", "PVT LTD", "LIMITED", "LTD"),
    "PRIVATE LTD": ("PRIVATE LIMITED", "PVT LTD", "PVT LIMITED", "LIMITED", "LTD"),
    "LIMITED": ("LTD", "PRIVATE LIMITED", "PVT LTD"),
    "LTD": ("LIMITED", "PRIVATE LIMITED", "PVT LTD"),
    "LLP": ("LIMITED LIABILITY PARTNERSHIP",),
    "LIMITED LIABILITY PARTNERSHIP": ("LLP",),
    "INC": ("INCORPORATED",),
    "INCORPORATED": ("INC",),
    "CORPORATION": ("CORP",),
    "CORP": ("CORPORATION",),
    "COMPANY": ("CO",),
    "CO": ("COMPANY",),
}

# Ordered longest-first so greedy matching removes the right suffix
_ALL_SUFFIXES = sorted(_SUFFIX_EXPANSION.keys(), key=len, reverse=True)

# Designation prefixes to strip before the legal entity name
_DESIGNATION_RE = re.compile(
    r"^(?:THE\s+)?"
    r"(?:CEO|CFO|CTO|COO|CMD|MD|MANAGING\s+DIRECTOR|DIRECTOR|CHAIRMAN"
    r"|SECRETARY|MANAGER|BRANCH\s+MANAGER|AUTHORIZED\s+OFFICER"
    r"|AUTHORISED\s+OFFICER|RECOVERY\s+OFFICER|NODAL\s+OFFICER"
    r"|REGISTRAR|PRINCIPAL|PRESIDENT|SUPERINTENDENT)\s+(?:OF\s+)?",
    re.IGNORECASE,
)

# M/S prefix variants
_MS_PREFIX_RE = re.compile(r"^M\s*/?\s*S\.?\s*", re.IGNORECASE)


# ═══════════════════════════════════════════════════════════════
#  STEP 0: NORMALIZATION
# ═══════════════════════════════════════════════════════════════

def normalize_company_name(raw: str) -> str:
    """Apply all normalization rules to make the name search-ready.

    1. Remove leading numbering
    2. Remove M/S prefix
    3. Replace (I) → (INDIA)
    4. Remove punctuation (keep alphanumeric, spaces, parentheses)
    5. Remove designation prior to LE name
    6. Remove words after legal suffix
    """
    if not raw:
        return ""

    text = str(raw).strip().upper()
    if not text:
        return ""

    # 1. Remove leading numbering: "1 ABC PVT LTD" → "ABC PVT LTD"
    text = re.sub(r"^\d+[\s.\-)]+\s*", "", text).strip()

    # 2. Remove M/S prefix
    text = _MS_PREFIX_RE.sub("", text).strip()

    # 3. Replace (I) → (INDIA)
    text = re.sub(r"\(I\)", "(INDIA)", text)

    # 4. Remove punctuation (keep letters, digits, spaces, parentheses, &)
    text = re.sub(r"[^\w\s()&]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    # 5. Remove designation prior to LE name
    text = _DESIGNATION_RE.sub("", text).strip()

    # 6. Remove words after legal suffix
    for suf in _ALL_SUFFIXES:
        idx = text.find(suf)
        if idx != -1:
            end = idx + len(suf)
            if end < len(text):
                text = text[:end].strip()
            break

    return text.strip()


# ═══════════════════════════════════════════════════════════════
#  STEP 1: REMOVE LEGAL SUFFIX → GET CORE NAME
# ═══════════════════════════════════════════════════════════════

def _strip_legal_suffix(name: str) -> tuple[str, str]:
    """Return (core_name, detected_suffix). Suffix is '' if none found.

    Also strips a trailing 'P' if it sits right before the suffix,
    e.g. 'ABC P LIMITED' → core='ABC', suffix='LIMITED'.
    """
    upper = name.upper().strip()
    for suf in _ALL_SUFFIXES:
        if upper.endswith(suf):
            core = upper[: -len(suf)].strip()
            # Strip trailing lone 'P' before suffix (e.g. "ABC P")
            if core.endswith(" P"):
                core = core[:-2].strip()
            return core, suf
    return upper, ""


# ═══════════════════════════════════════════════════════════════
#  STEP 2: TOKENIZE CORE NAME
# ═══════════════════════════════════════════════════════════════

def _tokenize(core: str) -> list[str]:
    """Split core name into individual tokens."""
    return [t for t in core.split() if t]


# ═══════════════════════════════════════════════════════════════
#  STEP 3: GENERATE SEARCH VARIANTS (acronym expansion)
# ═══════════════════════════════════════════════════════════════

def _is_short_alpha(token: str) -> bool:
    """True for short alphabetic tokens that could be acronyms (≤3 chars)."""
    return len(token) <= 3 and token.isalpha()


def _acronym_variants(token: str) -> list[str]:
    """Generate variants for a short alphabetic token.

    E.g. "MK" → ["MK", "M K", "M.K."]
    """
    if len(token) <= 1:
        return [token]

    joined = token  # MK
    spaced = " ".join(token)  # M K
    dotted = ".".join(token) + "."  # M.K.
    return [joined, spaced, dotted]


def _build_token_search_queries(tokens: list[str]) -> list[list[str]]:
    """Generate all variant combinations for the token list.

    Short alphabetic tokens → multiple forms (joined, split, dotted).
    Each variant is a list of tokens to AND together.
    """
    if not tokens:
        return []

    # Find indices of short tokens that need expansion
    expansion_slots: list[tuple[int, list[str]]] = []
    for i, token in enumerate(tokens):
        if _is_short_alpha(token):
            expansion_slots.append((i, _acronym_variants(token)))

    if not expansion_slots:
        return [tokens]

    # Generate all combinations
    # For simplicity, expand one slot at a time (keeps count manageable)
    base = list(tokens)
    variants: list[list[str]] = [base[:]]

    for slot_idx, slot_variants in expansion_slots:
        new_variants: list[list[str]] = []
        for existing in variants:
            for sv in slot_variants:
                copy = list(existing)
                copy[slot_idx] = sv
                new_variants.append(copy)
        variants = new_variants

    # Deduplicate
    seen: set[str] = set()
    unique: list[list[str]] = []
    for v in variants:
        key = " ".join(v)
        if key not in seen:
            seen.add(key)
            unique.append(v)

    return unique


# ═══════════════════════════════════════════════════════════════
#  STEP 4: GENERATE FULL SEARCH VARIANTS WITH SUFFIX EXPANSION
# ═══════════════════════════════════════════════════════════════

def generate_search_variants(raw_name: str) -> list[str]:
    """Generate all search name variants for a company name.

    Returns a list of full names to search for in sheets.
    The input B1 name is proper/full but sheet data may use abbreviations.

    Example:
        "M/S ABC PRIVATE LIMITED" →
        ["ABC PRIVATE LIMITED", "ABC PVT LTD", "ABC PVT LIMITED",
         "ABC PRIVATE LTD", "ABC LIMITED", "ABC LTD", "ABC"]
    """
    normalized = normalize_company_name(raw_name)
    if not normalized:
        return [raw_name.strip().upper()] if raw_name else []

    core, detected_suffix = _strip_legal_suffix(normalized)
    tokens = _tokenize(core)

    if not tokens:
        return [normalized]

    # Generate token-level variants (acronym expansion)
    token_variants = _build_token_search_queries(tokens)

    results: list[str] = []
    seen: set[str] = set()

    def _add(name: str) -> None:
        key = re.sub(r"\s+", " ", name.strip().upper())
        if key and key not in seen:
            seen.add(key)
            results.append(key)

    # Always include the normalized full name first
    _add(normalized)

    # For each token variant, combine with detected suffix + all alternatives
    suffixes_to_try: list[str] = []
    if detected_suffix:
        suffixes_to_try.append(detected_suffix)
        for alt in _SUFFIX_EXPANSION.get(detected_suffix, ()):
            suffixes_to_try.append(alt)

    for tv in token_variants:
        core_str = " ".join(tv)

        # With each suffix variant
        for suf in suffixes_to_try:
            _add(f"{core_str} {suf}")

        # Without suffix (core only)
        _add(core_str)

    return results


def generate_search_patterns(raw_name: str) -> list[re.Pattern[str]]:
    """Compile regex patterns for all search variants.

    Each pattern matches the variant as a substring (case-insensitive),
    with flexible whitespace between tokens.
    """
    variants = generate_search_variants(raw_name)
    patterns: list[re.Pattern[str]] = []
    seen_pat: set[str] = set()

    for variant in variants:
        tokens = variant.split()
        if not tokens:
            continue
        parts = [re.escape(t) for t in tokens]
        body = r"\s+".join(parts)
        if body not in seen_pat:
            seen_pat.add(body)
            patterns.append(re.compile(body, re.IGNORECASE))

    return patterns
