
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from typing import Any
from config import CSV_COLUMNS
import config
from config import SERVICE_ACCOUNT_FILE
from daily_run.config import INDEX_SHEET_ID
from scrapers.base import BaseScraper
from utils.company_augment import CompanySearchMatcher, build_company_search_plan
from utils.party_search import cell_matches_party_query

logger = logging.getLogger("legal_scraper.daily_run.sheet_search")

_COURT_COL_MAP = {"dc": "A", "hc": "B", "sc": "C"}
_SHEET_SEARCH_DELAY_SECONDS = float(os.environ.get("SHEET_SEARCH_DELAY_SECONDS", "2.0"))
_SHEET_SEARCH_MAX_QUOTA_RETRIES = int(os.environ.get("SHEET_SEARCH_MAX_QUOTA_RETRIES", "8"))

# Fields to search for party name matches
_SEARCH_FIELDS = [
    "respondent",
    "otherRespondent",
    "petitioner",
    "otherPetitioner",
]
_HISTORY_FIELDS = {
    "listingHistory",
    "applicationDetails",
    "orderHistory",
    "applicationHistory",
}
_PRIMARY_FIELDS = {
    "caseNumber",
    "respondent",
    "petitioner",
    "caseType",
    "registrationDate",
}

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

        company_matcher: CompanySearchMatcher | None = None
        if entity_type == "company":
            search_plan = build_company_search_plan(party_name)
            company_matcher = search_plan.matcher()
            logger.info(
                "[%s] Company search plan:\n"
                "  Query      : %s\n"
                "  Normalized : %s\n"
                "  Core       : %s\n"
                "  Variants   : %d\n"
                "  Keywords   : %s\n"
                "  Score min  : %d",
                self.COURT_TYPE.upper(),
                party_name,
                search_plan.normalized_name or "-",
                search_plan.core_name or "-",
                len(search_plan.variants),
                "; ".join(search_plan.keyword_queries[:5]) or "-",
                search_plan.score_threshold,
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
            if company_matcher:
                return company_matcher.matches(cell_val)
            return cell_matches_party_query(cell_val, party_name)

        def _col_letter(col_idx: int) -> str:
            from gspread.utils import rowcol_to_a1

            return "".join(c for c in rowcol_to_a1(1, col_idx) if c.isalpha())

        def _cell_from_block(block: list[list[str]], data_idx: int) -> str:
            if data_idx < len(block) and block[data_idx]:
                return str(block[data_idx][0]).strip()
            return ""

        def _row_dict_from_values(header: list[str], row_values: list[str]) -> dict[str, Any]:
            return {
                h.strip(): row_values[i] if i < len(row_values) else ""
                for i, h in enumerate(header)
            }

        def search_single_sheet(sh_id: str) -> list[dict[str, Any]]:
            """Search a single sheet without loading the whole spreadsheet grid."""
            attempt = 0
            while attempt <= _SHEET_SEARCH_MAX_QUOTA_RETRIES:
                try:
                    started = time.monotonic()
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

                    watched_fields = set(_SEARCH_FIELDS) | _HISTORY_FIELDS | _PRIMARY_FIELDS
                    watched_indices: dict[str, int] = {}
                    for i, h in enumerate(header):
                        h_clean = h.strip()
                        if h_clean in watched_fields:
                            watched_indices[h_clean] = i + 1

                    if not watched_indices:
                        return []

                    ordered_cols = sorted(set(watched_indices.values()))
                    col_blocks = ws.batch_get(
                        [f"{_col_letter(col)}2:{_col_letter(col)}" for col in ordered_cols]
                    )
                    block_by_col = {
                        col: col_blocks[i] if i < len(col_blocks) else []
                        for i, col in enumerate(ordered_cols)
                    }
                    data_row_count = max((len(block) for block in col_blocks), default=0)
                    if data_row_count <= 0:
                        return []

                    def value_for(field: str, data_idx: int) -> str:
                        col_idx = watched_indices.get(field)
                        if not col_idx:
                            return ""
                        return _cell_from_block(block_by_col.get(col_idx, []), data_idx)

                    groups: list[tuple[int, int]] = []
                    skip_until = -1

                    for data_idx in range(data_row_count):
                        if data_idx <= skip_until:
                            continue

                        found = False
                        for col_idx_0 in search_col_indices.values():
                            cell_val = _cell_from_block(
                                block_by_col.get(col_idx_0, []), data_idx
                            )
                            if cell_val and _cell_matches(cell_val):
                                found = True
                                break

                        if found:
                            end_idx = data_idx

                            peek_idx = data_idx + 1
                            while peek_idx < data_row_count:
                                has_history = any(
                                    value_for(field, peek_idx) for field in _HISTORY_FIELDS
                                )
                                has_primary = any(
                                    value_for(field, peek_idx) for field in _PRIMARY_FIELDS
                                )
                                if has_primary or not has_history:
                                    break

                                end_idx = peek_idx
                                peek_idx += 1

                            groups.append((data_idx + 2, end_idx + 2))
                            skip_until = end_idx

                    if not groups:
                        logger.debug(
                            "[%s] Sheet %s scanned %d rows: 0 matches",
                            self.COURT_TYPE.upper(),
                            sh_id[:8],
                            data_row_count,
                        )
                        return []

                    last_col = _col_letter(len(header))
                    row_ranges = [
                        f"A{start}:{last_col}{end}" for start, end in groups
                    ]
                    row_blocks = ws.batch_get(row_ranges)
                    matching_rows: list[dict[str, Any]] = []

                    for group_idx, block in enumerate(row_blocks):
                        for row_offset, row_data in enumerate(block or []):
                            row_dict = _row_dict_from_values(header, row_data)
                            if row_offset == 0:
                                row_dict["partyName"] = party_name
                            else:
                                for col in CSV_COLUMNS:
                                    if col not in _HISTORY_FIELDS:
                                        row_dict[col] = ""
                                row_dict["_is_continuation"] = True
                            matching_rows.append(row_dict)

                    primary_count = len(groups)
                    logger.info(
                        "[%s] Sheet %s scanned %d rows in %.1fs: matches=%d sheet_rows=%d",
                        self.COURT_TYPE.upper(),
                        sh_id[:8],
                        data_row_count,
                        time.monotonic() - started,
                        primary_count,
                        len(matching_rows),
                    )

                    return matching_rows
                except Exception as e:
                    err_str = str(e)
                    if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                        attempt += 1
                        logger.info(
                            "Sheet %s: quota hit, waiting 60s then retrying (attempt %d)",
                            sh_id[:8], attempt,
                        )
                        time.sleep(60)
                        continue
                    logger.warning("Error searching sheet %s: %s", sh_id, e)
                    return []
            logger.warning(
                "Sheet %s: quota retry limit reached after %d attempts",
                sh_id[:8],
                _SHEET_SEARCH_MAX_QUOTA_RETRIES,
            )
            return []

        for idx, sid in enumerate(sheet_ids):
            if idx > 0:
                await asyncio.sleep(max(0.0, _SHEET_SEARCH_DELAY_SECONDS))
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
