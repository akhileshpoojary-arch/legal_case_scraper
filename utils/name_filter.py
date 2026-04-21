"""
Post-scrape name filtering for individual entity searches.

When entity_type is 'individual', court APIs often return partial matches
(e.g. searching "PRAKASH RANJAN NAYAK" returns "DR..PRAKASH RANJAN" —
a different person). This module discards false-positive rows by checking
that the exact queried name appears in at least one party column.

For 'company' entity, no filtering is applied — the existing augmentation
logic handles abbreviation matching.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from utils.normalize import normalize_party_name

logger = logging.getLogger("legal_scraper.name_filter")

_PARTY_COLUMNS = ("respondent", "otherRespondent", "petitioner", "otherPetitioner")


def _normalize_query(name: str) -> str:
    """Normalize the query name using the same pipeline as party names."""
    return normalize_party_name(name)


def _extract_names_from_quoted_csv(cell: str) -> list[str]:
    """Parse '"NAME1", "NAME2"' format and return individual names.

    Also handles plain (unquoted) single-name cells.
    """
    if not cell:
        return []

    if '"' in cell:
        return re.findall(r'"([^"]+)"', cell)

    stripped = cell.strip()
    return [stripped] if stripped else []


def _name_matches(normalized_query: str, cell_value: str) -> bool:
    """Check if the normalized query name matches any name in the cell.

    Each name extracted from the cell is normalized (title prefixes stripped,
    uppercased, whitespace collapsed) before comparison.
    """
    names = _extract_names_from_quoted_csv(cell_value)
    for raw_name in names:
        normalized = normalize_party_name(raw_name)
        if normalized == normalized_query:
            return True
    return False


def filter_individual_matches(
    rows: list[dict[str, Any]],
    party_name: str,
    entity_type: str,
) -> list[dict[str, Any]]:
    """Discard rows where the searched individual name doesn't appear.

    Args:
        rows: Scraped and normalized result rows.
        party_name: The original query party name.
        entity_type: 'individual' or 'company'.

    Returns:
        Filtered list — unchanged for company, strict-matched for individual.
    """
    if entity_type != "individual":
        return rows

    normalized_query = _normalize_query(party_name)
    if not normalized_query:
        return rows

    kept: list[dict[str, Any]] = []
    dropped = 0
    is_previous_row_kept = False

    for row in rows:
        match_found = False
        has_any_party = False

        for col in _PARTY_COLUMNS:
            cell = row.get(col, "")
            if cell and str(cell).strip():
                has_any_party = True
                if _name_matches(normalized_query, cell):
                    match_found = True
                    break

        # If the row has no party info, it's likely a continuation row (e.g. spilled JSON data).
        # We tie its preservation to whether the immediately preceding main row was kept.
        if not has_any_party:
            if is_previous_row_kept:
                kept.append(row)
            else:
                dropped += 1
        else:
            if match_found:
                kept.append(row)
                is_previous_row_kept = True
            else:
                dropped += 1
                is_previous_row_kept = False
                logger.debug(
                    "Filtered out case %s — no exact match for '%s'",
                    row.get("caseNumber", "?"),
                    party_name,
                )

    if dropped:
        logger.info(
            "Name filter: kept %d, dropped %d rows (individual match for '%s')",
            len(kept),
            dropped,
            party_name,
        )

    return kept
