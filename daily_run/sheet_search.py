
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any
from config import CSV_COLUMNS
import config
from config import SERVICE_ACCOUNT_FILE
from daily_run.config import INDEX_SHEET_ID
from scrapers.base import BaseScraper
from utils.company_augment import generate_search_patterns
from utils.party_search import cell_matches_party_query

logger = logging.getLogger("legal_scraper.daily_run.sheet_search")

_COURT_COL_MAP = {"dc": "A", "hc": "B", "sc": "C"}

# Fields to search for party name matches
_SEARCH_FIELDS = [
    "respondent",
    "otherRespondent",
    "petitioner",
    "otherPetitioner",
]

class BaseSheetSearchScraper(BaseScraper):
    """Scraper that queries Google Sheets instead of live websites."""

    COURT_TYPE = ""
    SOURCE = ""

    def __init__(self) -> None:
        import gspread

        self._gc = gspread.service_account(filename=str(SERVICE_ACCOUNT_FILE))
        self._index_ws = self._gc.open_by_key(INDEX_SHEET_ID).worksheet(
            "All court"
        )

    async def close(self) -> None:
        pass

    async def run(self, party_name: str) -> list[dict[str, Any]]:
        col = _COURT_COL_MAP.get(self.COURT_TYPE)
        if not col:
            return []

        loop = asyncio.get_running_loop()
        entity_type = getattr(config, "ENTITY_TYPE", "individual")

        augmented_patterns: list[re.Pattern[str]] | None = None
        if entity_type == "company":
            augmented_patterns = generate_search_patterns(party_name)
            logger.info(
                "[%s] Company mode: %d search variants for '%s'",
                self.COURT_TYPE.upper(),
                len(augmented_patterns),
                party_name,
            )

        def get_sheet_ids() -> list[str]:
            try:
                values = self._index_ws.get(
                    f"{col}2:{col}", value_render_option="FORMULA"
                )
                ids: list[str] = []
                for row in (values or []):
                    if not row:
                        continue
                    match = re.search(r"/d/([a-zA-Z0-9-_]+)", str(row[0]))
                    if match:
                        ids.append(match.group(1))
                    else:
                        raw = str(row[0]).strip()
                        if raw:
                            ids.append(raw)
                return ids
            except Exception as e:
                logger.error("Failed to read index for %s: %s", self.COURT_TYPE, e)
                return []

        sheet_ids = await loop.run_in_executor(None, get_sheet_ids)
        if not sheet_ids:
            return []

        all_matches: list[dict[str, Any]] = []

        def _cell_matches(cell_val: str) -> bool:
            """Check if a cell value matches the party query (augmented or basic)."""
            if augmented_patterns:
                for pat in augmented_patterns:
                    if pat.search(cell_val):
                        return True
                return False
            return cell_matches_party_query(cell_val, party_name)

        def search_single_sheet(sh_id: str) -> list[dict[str, Any]]:
            """Search a single sheet, retrying indefinitely on quota errors."""
            attempt = 0
            while True:
                try:
                    sh = self._gc.open_by_key(sh_id)
                    ws = sh.get_worksheet(0)
                    header = ws.row_values(1)

                    if not header:
                        return []

                    search_col_indices: dict[str, int] = {}
                    for i, h in enumerate(header):
                        h_clean = h.strip()
                        if h_clean in _SEARCH_FIELDS:
                            search_col_indices[h_clean] = i + 1

                    if not search_col_indices:
                        return []

                    all_values = ws.get_all_values()
                    if len(all_values) <= 1:
                        return []

                    matching_rows: list[dict[str, Any]] = []
                    skip_until = 0

                    for row_idx in range(1, len(all_values)):
                        if row_idx < skip_until:
                            continue

                        row_data = all_values[row_idx]
                        found = False
                        for col_idx_0 in search_col_indices.values():
                            if col_idx_0 - 1 < len(row_data):
                                cell_val = row_data[col_idx_0 - 1]
                                if cell_val and _cell_matches(str(cell_val)):
                                    found = True
                                    break

                        if found:
                            row_dict: dict[str, Any] = {}
                            for i, h in enumerate(header):
                                if i < len(row_data):
                                    row_dict[h.strip()] = row_data[i]
                                else:
                                    row_dict[h.strip()] = ""
                            row_dict["partyName"] = party_name
                            matching_rows.append(row_dict)

                            # Fetch continuation rows (spillover listing/application data)
                            peek_idx = row_idx + 1
                            while peek_idx < len(all_values):
                                next_row_data = all_values[peek_idx]
                                has_r_or_s = False
                                has_primary = False

                                for i, cell in enumerate(next_row_data):
                                    val = str(cell).strip()
                                    if not val:
                                        continue

                                    h_name = header[i].strip() if i < len(header) else ""
                                    if h_name in ("listingHistory", "applicationDetails", "orderHistory", "applicationHistory"):
                                        has_r_or_s = True
                                    elif h_name in ("caseNumber", "respondent", "petitioner", "caseType", "registrationDate"):
                                        has_primary = True

                                # Stop if we hit a new distinct case or a totally empty row
                                if has_primary or not has_r_or_s:
                                    break

                                # It's a continuation row, add it
                                next_row_dict: dict[str, Any] = {}
                                for i, h in enumerate(header):
                                    key = h.strip()
                                    if i < len(next_row_data):
                                        next_row_dict[key] = next_row_data[i]
                                    else:
                                        next_row_dict[key] = ""

                                # Don't propagate metadata/party data for the continuation row.
                                # Aggressively clear ALL columns except JSON history columns.
                                history_cols = {"listingHistory", "applicationDetails", "orderHistory", "applicationHistory"}
                                for col in CSV_COLUMNS:
                                    if col not in history_cols:
                                        next_row_dict[col] = ""

                                next_row_dict["_is_continuation"] = True
                                matching_rows.append(next_row_dict)

                                peek_idx += 1

                            skip_until = peek_idx

                    return matching_rows
                except Exception as e:
                    err_str = str(e)
                    if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                        attempt += 1
                        logger.info(
                            "Sheet %s: quota hit, waiting 60s then retrying (attempt %d)",
                            sh_id[:8], attempt,
                        )
                        import time
                        time.sleep(60)
                        continue
                    logger.warning("Error searching sheet %s: %s", sh_id, e)
                    return []

        # Each sheet uses ~3 API calls; 3s gap keeps us at ~20 sheets/min (~60 reads)
        import time as _time

        for idx, sid in enumerate(sheet_ids):
            if idx > 0:
                _time.sleep(3)
            try:
                result = await loop.run_in_executor(None, search_single_sheet, sid)
                all_matches.extend(result)
            except Exception as e:
                logger.warning("Error processing sheet %s: %s", sid, e)

        primary_count = sum(1 for r in all_matches if not r.get("_is_continuation"))
        logger.info(
            "[%s] Sheet search for '%s' found %d cases across %d sheets [entity=%s]",
            self.COURT_TYPE.upper(),
            party_name,
            primary_count,
            len(sheet_ids),
            entity_type,
        )
        return all_matches

class DCSheetScraper(BaseSheetSearchScraper):
    NAME = "district_court"
    COURT_TYPE = "dc"
    SOURCE = "DISTRICT_COURT"

class HCSheetScraper(BaseSheetSearchScraper):
    NAME = "high_court"
    COURT_TYPE = "hc"
    SOURCE = "HIGH_COURT"

class SCSheetScraper(BaseSheetSearchScraper):
    NAME = "supreme_court"
    COURT_TYPE = "sc"
    SOURCE = "SUPREME_COURT"
