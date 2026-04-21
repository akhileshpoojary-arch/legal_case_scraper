
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from pathlib import Path
from typing import Any

from config import SERVICE_ACCOUNT_FILE
from utils.sheet_dedup import row_dedup_key
from daily_run.cluster import acquire_write_lock, release_write_lock
from daily_run.config import (
    CLUSTER_WORKER_ID,
    CONFIG_WORKSHEET_NAME,
    ENFORCE_SHARED_DRIVE_DESTINATION,
    INDEX_SHEET_ID,
    MAX_ROWS_PER_SHEET,
    SHARED_DRIVE_FOLDER_ID,
    TEMPLATE_SHEET_ID,
    WRITE_BATCH_SIZE,
    WRITE_LOCK_POLL_SECONDS,
)

logger = logging.getLogger("legal_scraper.daily_run.sheets")

_COURT_COL_MAP = {"dc": "A", "hc": "B", "sc": "C"}
_COURT_NAME_MAP = {"dc": "district court", "hc": "high court", "sc": "supreme court"}

# Google Sheets hard limit per cell is 50,000 characters
_MAX_CELL_CHARS = 49_000
_RETRY_BASE_DELAY = 2
_RATE_LIMIT_MIN_DELAY = 65
_NON_RETRYABLE_REASONS = {
    "storageQuotaExceeded",
    "insufficientFilePermissions",
    "notFound",
    "forbidden",
}
_DEFAULT_DOMAIN = "tracxn.com"
_DEDUP_KEY_FIELDS = (
    "uniqueness",
    "courtType",
    "benchName",
    "caseNumber",
    "registrationDate",
)

class DailyRunSheetsManager:

    def __init__(self) -> None:
        import gspread

        attempt = 0
        while True:
            attempt += 1
            try:
                self._gc = gspread.service_account(filename=str(SERVICE_ACCOUNT_FILE))
                self._index_sh = self._gc.open_by_key(INDEX_SHEET_ID)
                self._index_ws = self._index_sh.worksheet("All court")
                break
            except Exception as e:
                reason = DailyRunSheetsManager._extract_error_reason(e)
                if reason in _NON_RETRYABLE_REASONS:
                    logger.error("Sheets startup failed (non-retryable): %s", e)
                    raise
                delay = min(_RETRY_BASE_DELAY * (2 ** min(attempt - 1, 6)), 300)
                if DailyRunSheetsManager._is_rate_limit_error(e):
                    delay = max(delay, _RATE_LIMIT_MIN_DELAY + random.uniform(0.0, 5.0))
                logger.warning(
                    "Sheets startup attempt %d failed: %s. Retrying in %ds.",
                    attempt,
                    e,
                    int(delay),
                )
                time.sleep(delay)

        self._locks = {k: asyncio.Lock() for k in _COURT_COL_MAP}
        self._header_cache: dict[str, list[str]] = {}
        self._case_col_cache: dict[str, int | None] = {}
        self._existing_case_cache: dict[str, set[str]] = {}
        # All dedup keys across every paginated file for a court (DC / HC / SC).
        self._court_wide_dedup: dict[str, set[str]] = {}
        self._shared_ok_cache: set[str] = set()
        # Cache active sheet ID and row counts to avoid redundant API reads
        self._active_sheet_info: dict[str, dict[str, Any]] = {}
        # Cache cluster config to avoid extra config-tab reads on every batch write.
        self._cluster_total_systems: int | None = None
        self._cluster_total_systems_at: float = 0.0

    @staticmethod
    def _col_index_to_letters(col_idx: int) -> str:
        """1-based column index → A1 letters (no row)."""
        from gspread.utils import rowcol_to_a1

        return "".join(c for c in rowcol_to_a1(1, col_idx) if c.isalpha())

    @staticmethod
    def _physical_used_row_count(ws: Any) -> int:
        """
        Approximate last used row: max depth across the first several columns.
        Uses a single values.batchGet (via batch_get) instead of four col_values
        calls so parallel DC/HC/SC runs stay under Sheets read quotas.
        """
        m = 0
        try:
            letters = [
                DailyRunSheetsManager._col_index_to_letters(c) for c in (1, 2, 3, 18)
            ]
            vrs = ws.batch_get([f"{L}:{L}" for L in letters])
            for block in vrs:
                m = max(m, len(block) if block else 0)
        except Exception:
            try:
                for col in (1, 2, 3, 18):
                    m = max(m, len(ws.col_values(col)))
            except Exception:
                pass
        return m

    @staticmethod
    def _extract_error_reason(exc: Exception) -> str:
        try:
            text = str(exc)
            match = re.search(r"'reason':\s*'([^']+)'", text)
            if match:
                return match.group(1)
        except Exception:
            return ""
        return ""

    def _is_retryable_error(self, exc: Exception) -> bool:
        reason = self._extract_error_reason(exc)
        if reason in _NON_RETRYABLE_REASONS:
            return False
        text = str(exc)
        status_match = re.search(r"'code':\s*(\d+)", text)
        if status_match:
            status = int(status_match.group(1))
            if status >= 500 or status in {408, 429}:
                return True
        return True

    @staticmethod
    def _is_rate_limit_error(exc: Exception) -> bool:
        text = str(exc)
        return ("RATE_LIMIT_EXCEEDED" in text) or ("'code': 429" in text)

    def _ensure_in_shared_drive(self, _file_id: str) -> None:
        if ENFORCE_SHARED_DRIVE_DESTINATION and not SHARED_DRIVE_FOLDER_ID:
            raise RuntimeError(
                "ENFORCE_SHARED_DRIVE_DESTINATION is True but SHARED_DRIVE_FOLDER_ID is not set."
            )

    def _ensure_domain_editor_access(self, file_id: str) -> None:
        """
        Ensure organization-wide editor access on the spreadsheet.
        Matches Drive UI: General access -> Tracxn Technologies Limited -> Editor.
        """
        if file_id in self._shared_ok_cache:
            return
        sh = self._gc.open_by_key(file_id)
        try:
            sh.share(
                _DEFAULT_DOMAIN,
                perm_type="domain",
                role="writer",
                notify=False,
            )
            logger.info(
                "Ensured domain editor access for %s on file %s",
                _DEFAULT_DOMAIN,
                file_id,
            )
        except Exception as e:
            reason = self._extract_error_reason(e)
            # Already exists / restricted policies can raise API errors.
            if reason in {"alreadyExists", "cannotShareAcrossDomains", "forbidden"}:
                logger.debug(
                    "Domain share check skipped for %s (%s): %s",
                    file_id,
                    reason or "policy",
                    e,
                )
            else:
                logger.warning("Could not ensure domain editor access: %s", e)
        self._shared_ok_cache.add(file_id)

    def _get_sheet_ids_for_court(self, court_type: str) -> list[str]:
        """Read all sheet URLs/IDs from the index column for a court type."""
        col = _COURT_COL_MAP[court_type]
        values = self._index_ws.get(
            f"{col}2:{col}", value_render_option="FORMULA"
        )
        ids: list[str] = []
        if not values:
            return ids
        for row in values:
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

    async def get_active_sheet(self, court_type: str) -> str:
        """Get or create the active sheet ID for a court type with row-count caching."""
        async with self._locks[court_type]:
            now = time.monotonic()
            cached = self._active_sheet_info.get(court_type)
            
            # Force refresh if nothing cached or cache older than 10 mins
            if not cached or (now - cached.get("at", 0) > 600):
                loop = asyncio.get_event_loop()
                sheet_ids = await loop.run_in_executor(
                    None, self._get_sheet_ids_for_court, court_type
                )

                if not sheet_ids:
                    active_id = await self._create_new_sheet(court_type, 1)
                    row_count = 0
                else:
                    active_id = sheet_ids[-1]
                    def check_full() -> int:
                        try:
                            sh = self._gc.open_by_key(active_id)
                            ws = sh.get_worksheet(0)
                            return DailyRunSheetsManager._physical_used_row_count(ws)
                        except Exception:
                            return 0
                    row_count = await loop.run_in_executor(None, check_full)
                
                self._active_sheet_info[court_type] = {
                    "id": active_id,
                    "row_count": row_count,
                    "at": now
                }

            active_info = self._active_sheet_info[court_type]
            if active_info["row_count"] >= MAX_ROWS_PER_SHEET:
                logger.info(
                    "[%s] Sheet %s full (%d rows). Creating next.",
                    court_type.upper(), active_info["id"], active_info["row_count"],
                )
                sheet_ids = await asyncio.get_event_loop().run_in_executor(
                    None, self._get_sheet_ids_for_court, court_type
                )
                new_id = await self._create_new_sheet(court_type, len(sheet_ids) + 1)
                self._active_sheet_info[court_type] = {
                    "id": new_id,
                    "row_count": 0,
                    "at": time.monotonic()
                }
                return new_id

            return active_info["id"]

    async def _create_new_sheet(self, court_type: str, index: int) -> str:
        """Duplicate template, name it, register in the index sheet.
        Retries retryable failures until success.
        """
        loop = asyncio.get_event_loop()
        name = f"{_COURT_NAME_MAP[court_type]} {index}"

        attempt = 0
        while True:
            attempt += 1
            try:
                def clone():
                    new_sh = self._gc.copy(
                        TEMPLATE_SHEET_ID,
                        title=name,
                        copy_permissions=False,
                        folder_id=SHARED_DRIVE_FOLDER_ID,
                    )
                    self._ensure_in_shared_drive(new_sh.id)
                    self._ensure_domain_editor_access(new_sh.id)
                    return new_sh

                new_sh = await loop.run_in_executor(None, clone)
                url = new_sh.url

                def update_index():
                    col = _COURT_COL_MAP[court_type]
                    col_num = ord(col) - ord("A") + 1
                    values = self._index_ws.col_values(col_num)
                    next_row = max(len(values) + 1, 2)
                    self._index_ws.update_acell(f"{col}{next_row}", url)

                await loop.run_in_executor(None, update_index)
                logger.info("[%s] Created sheet: %s", court_type.upper(), url)
                return new_sh.id

            except Exception as e:
                if not self._is_retryable_error(e):
                    logger.error(
                        "[%s] Non-retryable sheet creation error: %s",
                        court_type.upper(),
                        e,
                    )
                    raise
                delay = min(_RETRY_BASE_DELAY * (2 ** min(attempt - 1, 6)), 300)
                logger.error(
                    "[%s] Sheet creation failed (attempt %d): %s. Retrying in %ds.",
                    court_type.upper(), attempt, e, delay,
                )
                await asyncio.sleep(delay)

    def _get_existing_case_numbers(self, sheet_id: str, force_refresh: bool = False) -> set[str]:
        """Read caseNumber column from a sheet for dedup with memoization."""
        if not force_refresh and sheet_id in self._existing_case_cache:
            return set(self._existing_case_cache[sheet_id])
        try:
            sh = self._gc.open_by_key(sheet_id)
            ws = sh.get_worksheet(0)
            header = self._header_cache.get(sheet_id)
            if header is None or force_refresh:
                header = ws.row_values(1)
                self._header_cache[sheet_id] = header

            case_num_col = self._case_col_cache.get(sheet_id)
            if case_num_col is None:
                for i, h in enumerate(header):
                    if h.strip() == "caseNumber":
                        case_num_col = i + 1
                        break
                self._case_col_cache[sheet_id] = case_num_col

            if case_num_col is None:
                return set()

            values = ws.col_values(case_num_col)
            existing = {v.strip() for v in values[1:] if v.strip()}
            self._existing_case_cache[sheet_id] = existing
            return set(existing)
        except Exception as e:
            logger.warning("Could not read case numbers for dedup: %s", e)
            return set()

    def _read_dedup_field_blocks(
        self,
        sheet_id: str,
    ) -> tuple[Any, list[str], list[Any], int]:
        """Fetch the columns needed for row-level dedup / cleanup decisions."""
        sh = self._gc.open_by_key(sheet_id)
        ws = sh.get_worksheet(0)
        header = self._header_cache.get(sheet_id)
        if header is None:
            header = ws.row_values(1)
            self._header_cache[sheet_id] = header
        if not header:
            return ws, [], [], 0

        col_indices: list[int] = []
        names_in_order: list[str] = []
        for name in _DEDUP_KEY_FIELDS:
            pos: int | None = None
            for i, h in enumerate(header):
                if h.strip() == name:
                    pos = i + 1
                    break
            if pos is not None:
                col_indices.append(pos)
                names_in_order.append(name)

        if not col_indices:
            return ws, [], [], 0

        letters = [self._col_index_to_letters(c) for c in col_indices]
        vrs = ws.batch_get([f"{L}:{L}" for L in letters])
        max_len = DailyRunSheetsManager._physical_used_row_count(ws)
        return ws, names_in_order, vrs, max_len

    def _primary_row_groups_from_sheet(self, sheet_id: str) -> list[dict[str, Any]]:
        """
        Return logical row groups keyed by the primary dedup columns.

        Continuation rows created by overflow splitting have blank key fields, so
        they are attached to the previous primary row group.
        """
        groups: list[dict[str, Any]] = []
        try:
            _ws, names_in_order, vrs, max_len = self._read_dedup_field_blocks(sheet_id)
            if not names_in_order or max_len <= 1:
                return groups

            current_group: dict[str, Any] | None = None
            for row_i in range(1, max_len):
                row_num = row_i + 1
                row_dict: dict[str, Any] = {}
                has_anchor = False
                for j, name in enumerate(names_in_order):
                    block = vrs[j] if j < len(vrs) else []
                    cell = ""
                    if row_i < len(block) and block[row_i]:
                        cell = str(block[row_i][0]).strip()
                    row_dict[name] = cell
                    if cell:
                        has_anchor = True

                if has_anchor:
                    if current_group is not None:
                        groups.append(current_group)
                    current_group = {
                        "sheet_id": sheet_id,
                        "start_row": row_num,
                        "end_row": row_num,
                        "row_dict": row_dict,
                        "dedup_key": row_dedup_key(row_dict),
                    }
                    continue

                if current_group is not None:
                    current_group["end_row"] = row_num

            if current_group is not None:
                groups.append(current_group)
            return groups
        except Exception as e:
            logger.warning("Primary-row scan failed for sheet %s: %s", sheet_id, e)
            return groups

    def _dedup_keys_from_sheet_rows(self, sheet_id: str) -> set[str]:
        """
        Build row_dedup_key for every data row (cross-tab dedup).

        Reads only the columns needed for row_dedup_key in one batch_get
        instead of get_all_values() (same quota weight per sheet, far less
        payload than full grid when many columns exist).
        """
        keys: set[str] = set()
        try:
            for group in self._primary_row_groups_from_sheet(sheet_id):
                k = str(group.get("dedup_key", "")).strip()
                if k:
                    keys.add(k)
            return keys
        except Exception as e:
            logger.warning("Dedup scan failed for sheet %s: %s", sheet_id, e)
            return keys

    def _load_court_wide_dedup(self, court_type: str) -> set[str]:
        ids = self._get_sheet_ids_for_court(court_type)
        acc: set[str] = set()
        for sid in ids:
            acc |= self._dedup_keys_from_sheet_rows(sid)
        logger.info(
            "[%s] Court-wide dedup: %d keys from %d spreadsheet(s).",
            court_type.upper(),
            len(acc),
            len(ids),
        )
        return acc

    def _get_cache_path(self, court_type: str) -> Path:
        from daily_run.config import _pdir
        cache_dir = _pdir / ".dedup_cache"
        try:
            cache_dir.mkdir(exist_ok=True)
        except Exception:
            pass
        return cache_dir / f"{court_type}_keys.txt"

    def _load_local_dedup_cache(self, court_type: str) -> set[str]:
        path = self._get_cache_path(court_type)
        if not path.exists():
            return set()
        try:
            with open(path, "r") as f:
                return {line.strip() for line in f if line.strip()}
        except Exception as e:
            logger.debug("Failed to load dedup cache for %s: %s", court_type, e)
            return set()

    def _save_local_dedup_cache(self, court_type: str, keys: set[str]):
        path = self._get_cache_path(court_type)
        try:
            with open(path, "w") as f:
                for k in sorted(keys):
                    f.write(f"{k}\n")
        except Exception as e:
            logger.warning("Could not save dedup cache: %s", e)

    def _ensure_court_wide_dedup(self, court_type: str) -> set[str]:
        if court_type not in self._court_wide_dedup:
            # Stagger startup reads to avoid simultaneous 429s from parallel scrapers
            time.sleep(random.uniform(0.1, 8.0))

            cached = self._load_local_dedup_cache(court_type)
            live = self._load_court_wide_dedup(court_type)
            if cached:
                logger.info(
                    "[%s] Initialized %d keys from local dedup cache.",
                    court_type.upper(),
                    len(cached),
                )
            merged = set(cached) | set(live)
            self._court_wide_dedup[court_type] = merged
            self._save_local_dedup_cache(court_type, merged)
        return self._court_wide_dedup[court_type]

    def find_duplicate_groups(self, court_type: str) -> dict[str, Any]:
        """Audit duplicate logical rows across all paginated sheets for a court."""
        ids = self._get_sheet_ids_for_court(court_type)
        seen: dict[str, dict[str, Any]] = {}
        duplicates: list[dict[str, Any]] = []
        logical_rows = 0

        for sid in ids:
            for group in self._primary_row_groups_from_sheet(sid):
                logical_rows += 1
                key = str(group.get("dedup_key", "")).strip()
                if not key:
                    continue
                if key in seen:
                    duplicates.append(
                        {
                            **group,
                            "first_sheet_id": seen[key]["sheet_id"],
                            "first_start_row": seen[key]["start_row"],
                            "first_end_row": seen[key]["end_row"],
                        }
                    )
                    continue
                seen[key] = group

        return {
            "court_type": court_type,
            "sheet_ids": ids,
            "spreadsheets": len(ids),
            "logical_rows": logical_rows,
            "unique_keys": len(seen),
            "duplicates": duplicates,
            "duplicate_groups": len(duplicates),
        }

    def cleanup_duplicate_groups(
        self,
        court_type: str,
        apply_delete: bool = False,
    ) -> dict[str, Any]:
        """Audit and optionally delete later duplicate row groups for a court."""
        audit = self.find_duplicate_groups(court_type)
        duplicates = list(audit.get("duplicates", []))
        per_sheet: dict[str, list[dict[str, Any]]] = {}
        for dup in duplicates:
            sid = str(dup.get("sheet_id", "")).strip()
            if not sid:
                continue
            per_sheet.setdefault(sid, []).append(dup)

        delete_ranges_by_sheet: dict[str, list[tuple[int, int]]] = {}
        for sid, groups in per_sheet.items():
            ranges: list[tuple[int, int]] = []
            for group in sorted(groups, key=lambda item: int(item["start_row"])):
                start_row = int(group["start_row"])
                end_row = int(group["end_row"])
                if ranges and start_row <= ranges[-1][1] + 1:
                    prev_start, prev_end = ranges[-1]
                    ranges[-1] = (prev_start, max(prev_end, end_row))
                    continue
                ranges.append((start_row, end_row))
            delete_ranges_by_sheet[sid] = ranges

        deleted_groups = 0
        deleted_rows = 0
        if apply_delete and delete_ranges_by_sheet:
            for sid, ranges in delete_ranges_by_sheet.items():
                sh = self._gc.open_by_key(sid)
                ws = sh.get_worksheet(0)
                for start_row, end_row in sorted(ranges, reverse=True):
                    ws.delete_rows(start_row, end_row)
                    deleted_rows += end_row - start_row + 1
                deleted_groups += len(per_sheet.get(sid, []))
                self._existing_case_cache.pop(sid, None)
                self._header_cache.pop(sid, None)
                self._case_col_cache.pop(sid, None)

            self._court_wide_dedup.pop(court_type, None)
            self._active_sheet_info.pop(court_type, None)
            self._save_local_dedup_cache(court_type, self._load_court_wide_dedup(court_type))

        return {
            **audit,
            "delete_ranges_by_sheet": delete_ranges_by_sheet,
            "delete_range_count": sum(len(v) for v in delete_ranges_by_sheet.values()),
            "deleted_groups": deleted_groups if apply_delete else 0,
            "deleted_rows": deleted_rows if apply_delete else 0,
            "applied": apply_delete,
        }

    def _build_rows_with_overflow(
        self,
        cases: list[dict[str, Any]],
        header: list[str],
    ) -> tuple[list[list[str]], int]:
        """
        Split oversized cell content into continuation rows.
        Non-overflow columns are blank on continuation rows.
        """
        rows_2d: list[list[str]] = []
        overflow_cases = 0

        for case in cases:
            cell_values: list[str] = []
            for h in header:
                val = case.get(h)
                if isinstance(val, (dict, list)):
                    val = json.dumps(val)
                if val is None:
                    val = ""
                cell_values.append(str(val))

            max_chunks = 1
            for val in cell_values:
                if len(val) > _MAX_CELL_CHARS:
                    chunks = (len(val) + _MAX_CELL_CHARS - 1) // _MAX_CELL_CHARS
                    if chunks > max_chunks:
                        max_chunks = chunks

            if max_chunks > 1:
                overflow_cases += 1

            for chunk_idx in range(max_chunks):
                out_row: list[str] = []
                for val in cell_values:
                    if len(val) <= _MAX_CELL_CHARS:
                        out_row.append(val if chunk_idx == 0 else "")
                    else:
                        start = chunk_idx * _MAX_CELL_CHARS
                        end = start + _MAX_CELL_CHARS
                        out_row.append(val[start:end])
                rows_2d.append(out_row)

        return rows_2d, overflow_cases

    async def write_cases(
        self, court_type: str, cases: list[dict[str, Any]]
    ) -> int:
        """
        Write cases to the active sheet with deduplication.
        Splits large cells into continuation rows to fit 50K limit.
        Never exceeds MAX_ROWS_PER_SHEET physical rows per tab (rotates mid-batch).
        Retries retryable API errors until success.

        Returns the number of actually written (non-duplicate) rows.
        """
        if not cases:
            return 0

        import gspread

        loop = asyncio.get_event_loop()

        def read_total_systems() -> int:
            now = time.monotonic()
            if (
                self._cluster_total_systems is not None
                and now - self._cluster_total_systems_at < 120.0
            ):
                return max(1, int(self._cluster_total_systems))
            try:
                from daily_run.cluster import read_config_row_sync

                cfg = read_config_row_sync(self._index_sh, CONFIG_WORKSHEET_NAME)
                ts = max(1, int(cfg.get("total_systems", 1)))
                self._cluster_total_systems = ts
                self._cluster_total_systems_at = now
                return ts
            except Exception:
                return 1

        total_systems = max(1, int(await loop.run_in_executor(None, read_total_systems)))
        if total_systems > 1 and CONFIG_WORKSHEET_NAME:
            await acquire_write_lock(
                self._index_sh,
                CONFIG_WORKSHEET_NAME,
                court_type,
                CLUSTER_WORKER_ID,
                WRITE_LOCK_POLL_SECONDS,
            )
        try:

            async def run_writes() -> int:
                started = time.monotonic()
                from daily_run.config import WRITE_BATCH_SIZE as CFG_BATCH_SIZE
                effective_batch_size = max(CFG_BATCH_SIZE, 5000)

                active_id = await self.get_active_sheet(court_type)
                await loop.run_in_executor(None, self._ensure_domain_editor_access, active_id)

                sh0 = self._gc.open_by_key(active_id)
                ws0 = sh0.get_worksheet(0)
                header = self._header_cache.get(active_id)
                if header is None:
                    header = ws0.row_values(1)
                if not header:
                    from config import CSV_COLUMNS

                    header = CSV_COLUMNS
                    ws0.append_row(header)
                self._header_cache[active_id] = header

                existing = self._ensure_court_wide_dedup(court_type)
                new_cases: list[dict[str, Any]] = []
                dup_keys: list[str] = []
                for case in cases:
                    k = row_dedup_key(case)
                    if k in existing:
                        dup_keys.append(k)
                        continue
                    new_cases.append(case)
                    existing.add(k)

                if dup_keys:
                    logger.info(
                        "[%s] Skipped %d duplicate row(s) (court-wide dedup).",
                        court_type.upper(),
                        len(dup_keys),
                    )
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            "[%s] Duplicate key sample: %s",
                            court_type.upper(),
                            dup_keys[:5],
                        )

                if not new_cases:
                    logger.info(
                        "[%s] All %d cases were duplicates (court-wide).",
                        court_type.upper(),
                        len(cases),
                    )
                    return 0

                rows_2d, overflow_cases = self._build_rows_with_overflow(
                    new_cases, header
                )
                cn_set = set(self._existing_case_cache.get(active_id, set()))
                for c in new_cases:
                    v = str(c.get("caseNumber", "")).strip()
                    if v:
                        cn_set.add(v)
                self._existing_case_cache[active_id] = cn_set

                queue: list[list[str]] = list(rows_2d)

                def append_chunk(sheet_id: str, batch_cap: int) -> int:
                    """Append up to batch_cap rows from queue to sheet_id. Returns rows appended."""
                    if not queue:
                        return 0
                    sh = self._gc.open_by_key(sheet_id)
                    ws = sh.get_worksheet(0)
                    n_existing = DailyRunSheetsManager._physical_used_row_count(ws)
                    if n_existing >= MAX_ROWS_PER_SHEET:
                        return 0
                    room = MAX_ROWS_PER_SHEET - n_existing
                    take = min(room, len(queue), batch_cap)
                    if take <= 0:
                        return 0
                    batch = queue[:take]
                    ws.append_rows(batch, value_input_option="USER_ENTERED")
                    del queue[:take]
                    return take

                while queue:
                    sheet_id = await self.get_active_sheet(court_type)
                    await loop.run_in_executor(None, self._ensure_domain_editor_access, sheet_id)
                    while queue:
                        before = len(queue)
                        await loop.run_in_executor(
                            None, append_chunk, sheet_id, effective_batch_size
                        )
                        if len(queue) == before:
                            break
                        
                        # Update local row count cache
                        written_in_chunk = before - len(queue)
                        if court_type in self._active_sheet_info:
                            self._active_sheet_info[court_type]["row_count"] += written_in_chunk
                            
                    if queue:
                        logger.debug(
                            "[%s] Rotating to next tab (%d rows left in queue).",
                            court_type.upper(),
                            len(queue),
                        )

                logger.info(
                    "[%s] Wrote %d new cases as %d sheet rows "
                    "(skipped %d dupes, overflow_cases=%d)",
                    court_type.upper(),
                    len(new_cases),
                    len(rows_2d),
                    len(cases) - len(new_cases),
                    overflow_cases,
                )
                logger.info(
                    "[%s] Write stage took %.2fs for %d candidate rows",
                    court_type.upper(),
                    time.monotonic() - started,
                    len(cases),
                )
                self._save_local_dedup_cache(court_type, existing)
                return len(new_cases)

            attempt = 0
            while True:
                attempt += 1
                try:
                    return await run_writes()
                except gspread.exceptions.APIError as e:
                    if not self._is_retryable_error(e):
                        logger.error(
                            "[%s] Non-retryable write API error: %s",
                            court_type.upper(),
                            e,
                        )
                        return 0
                    delay = min(_RETRY_BASE_DELAY * (2 ** min(attempt - 1, 6)), 300)
                    if self._is_rate_limit_error(e):
                        delay = max(
                            delay, _RATE_LIMIT_MIN_DELAY + random.uniform(0.0, 5.0)
                        )
                    logger.error(
                        "[%s] Write API Error attempt %d: %s. Retrying in %ds.",
                        court_type.upper(),
                        attempt,
                        e,
                        int(delay),
                    )
                    await asyncio.sleep(delay)
                except Exception as e:
                    if not self._is_retryable_error(e):
                        logger.error(
                            "[%s] Non-retryable write error: %s",
                            court_type.upper(),
                            e,
                        )
                        return 0
                    delay = min(_RETRY_BASE_DELAY * (2 ** min(attempt - 1, 6)), 300)
                    if self._is_rate_limit_error(e):
                        delay = max(
                            delay, _RATE_LIMIT_MIN_DELAY + random.uniform(0.0, 5.0)
                        )
                    logger.error(
                        "[%s] Write failed attempt %d: %s. Retrying in %ds.",
                        court_type.upper(),
                        attempt,
                        e,
                        int(delay),
                    )
                    await asyncio.sleep(delay)
        finally:
            if total_systems > 1 and CONFIG_WORKSHEET_NAME:
                await release_write_lock(
                    self._index_sh,
                    CONFIG_WORKSHEET_NAME,
                    court_type,
                    CLUSTER_WORKER_ID,
                )
