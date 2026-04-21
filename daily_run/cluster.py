
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

logger = logging.getLogger("legal_scraper.daily_run.cluster")

# Legacy layout: values only in row 1 (A1=total, B1/C1/D1=locks)
_LEGACY_LOCK_CELLS = {"dc": "B1", "hc": "C1", "sc": "D1"}
_VALUE_ROW = 2

def _norm_header(s: str) -> str:
    return re.sub(r"\s+", "_", str(s).strip().lower()).replace("-", "_")

def _row_looks_like_header_row(row: list[str]) -> bool:
    """True when row 1 is labels (e.g. total_systems), not legacy numeric A1."""
    if not row or not str(row[0]).strip():
        return False
    first = str(row[0]).strip().lower()
    if first.isdigit():
        try:
            n = int(first)
            if 1 <= n <= 99 and len(row) > 1:
                second = str(row[1]).strip() if len(row) > 1 else ""
                if not second or second.isdigit():
                    return False
        except ValueError:
            pass
    return any(c.isalpha() for c in first)

def parse_cluster_row(row: list[str]) -> dict[str, Any]:
    """Parse a single worksheet row as legacy values (A1=total, B1/C1/D1=locks)."""
    def cell(i: int) -> str:
        if i < len(row) and row[i] is not None:
            return str(row[i]).strip()
        return ""

    raw_total = cell(0)
    try:
        total_systems = int(raw_total) if raw_total else 1
    except ValueError:
        total_systems = 1
    total_systems = max(1, total_systems)

    return {
        "total_systems": total_systems,
        "lock_dc": cell(1),
        "lock_hc": cell(2),
        "lock_sc": cell(3),
    }

def parse_config_header_value_rows(
    header_row: list[str],
    value_row: list[str],
) -> dict[str, Any]:
    """Map headers in row 1 to cells in row 2."""
    hdr = [_norm_header(h) for h in header_row]
    vals = [
        str(value_row[i]).strip() if i < len(value_row) and value_row[i] is not None else ""
        for i in range(len(hdr))
    ]

    def getv(*names: str) -> str:
        for name in names:
            n = _norm_header(name)
            for i, h in enumerate(hdr):
                if h == n:
                    return vals[i] if i < len(vals) else ""
        return ""

    raw_total = getv("total_systems", "totalsystems", "total_system")
    try:
        total_systems = int(raw_total) if raw_total else 1
    except ValueError:
        total_systems = 1
    total_systems = max(1, total_systems)

    return {
        "total_systems": total_systems,
        "lock_dc": getv("dc_write_lock", "dc_lock", "writelock_dc"),
        "lock_hc": getv("hc_write_lock", "hc_lock", "writelock_hc"),
        "lock_sc": getv("sc_write_lock", "sc_lock", "writelock_sc"),
    }

def read_config_row_sync(index_spreadsheet: Any, worksheet_name: str) -> dict[str, Any]:
    """Read ``config``: prefer row 1 = headers, row 2 = values."""
    try:
        ws = index_spreadsheet.worksheet(worksheet_name)
        rows = ws.get_values("A1:Z2")
    except Exception as exc:
        logger.debug("Cluster config read failed (%s): %s", worksheet_name, exc)
        return parse_cluster_row([])

    if len(rows) >= 2 and any(str(x).strip() for x in rows[1]):
        if _row_looks_like_header_row(rows[0]):
            return parse_config_header_value_rows(rows[0], rows[1])
        return parse_cluster_row(rows[0])

    if len(rows) >= 1:
        if _row_looks_like_header_row(rows[0]) and (
            len(rows) < 2 or not any(str(x).strip() for x in rows[1])
        ):
            logger.info(
                "Config sheet row 2 is empty but row 1 looks like headers; "
                "using total_systems=1 until row 2 is filled."
            )
            return parse_config_header_value_rows(rows[0], ["1", "", "", ""])
        return parse_cluster_row(rows[0])

    return parse_cluster_row([])

def _lock_cell_a1(ws: Any, court_type: str) -> str | None:
    """A1 cell reference for the write-lock (row 2 when headers present)."""
    try:
        rows = ws.get_values("A1:Z2")
    except Exception:
        return _LEGACY_LOCK_CELLS.get(court_type)

    if len(rows) >= 2 and _row_looks_like_header_row(rows[0]):
        hdr = [_norm_header(h) for h in rows[0]]
        aliases = {
            "dc": ("dc_write_lock", "dc_lock", "writelock_dc"),
            "hc": ("hc_write_lock", "hc_lock", "writelock_hc"),
            "sc": ("sc_write_lock", "sc_lock", "writelock_sc"),
        }
        for i, h in enumerate(hdr):
            if h in aliases.get(court_type, ()):
                try:
                    from gspread.utils import rowcol_to_a1

                    return rowcol_to_a1(_VALUE_ROW, i + 1)
                except Exception:
                    col = i + 1
                    letters = ""
                    while col:
                        col, rem = divmod(col - 1, 26)
                        letters = chr(65 + rem) + letters
                    return f"{letters}{_VALUE_ROW}"

    return _LEGACY_LOCK_CELLS.get(court_type)

def try_acquire_write_lock_sync(
    index_spreadsheet: Any,
    worksheet_name: str,
    court_type: str,
    worker_id: str,
) -> bool:
    """Claim the lock cell for this court type if empty or already ours."""
    try:
        ws = index_spreadsheet.worksheet(worksheet_name)
    except Exception as exc:
        logger.warning("Write lock: worksheet %s: %s", worksheet_name, exc)
        return True

    cell_ref = _lock_cell_a1(ws, court_type)
    if not cell_ref:
        return True
    try:
        current = (ws.acell(cell_ref).value or "").strip()
        if current and current != worker_id:
            return False
        ws.update_acell(cell_ref, worker_id)
        time.sleep(0.25)
        verify = (ws.acell(cell_ref).value or "").strip()
        return verify == worker_id
    except Exception as exc:
        logger.warning("Write lock acquire failed: %s", exc)
        return False

def release_write_lock_sync(
    index_spreadsheet: Any,
    worksheet_name: str,
    court_type: str,
    worker_id: str,
) -> None:
    try:
        ws = index_spreadsheet.worksheet(worksheet_name)
    except Exception:
        return

    cell_ref = _lock_cell_a1(ws, court_type)
    if not cell_ref:
        return
    try:
        current = (ws.acell(cell_ref).value or "").strip()
        if current == worker_id:
            ws.update_acell(cell_ref, "")
    except Exception as exc:
        logger.debug("Write lock release: %s", exc)

async def acquire_write_lock(
    index_spreadsheet: Any,
    worksheet_name: str,
    court_type: str,
    worker_id: str,
    poll_seconds: float,
) -> None:
    if not worksheet_name:
        return
    loop = asyncio.get_event_loop()
    attempt = 0
    while True:
        ok = await loop.run_in_executor(
            None,
            lambda: try_acquire_write_lock_sync(
                index_spreadsheet, worksheet_name, court_type, worker_id
            ),
        )
        if ok:
            return
        attempt += 1
        if attempt == 1 or attempt % 6 == 0:
            logger.info(
                "[%s] Waiting for sheet write lock (%s)...",
                court_type.upper(),
                worker_id,
            )
        await asyncio.sleep(poll_seconds)

async def release_write_lock(
    index_spreadsheet: Any,
    worksheet_name: str,
    court_type: str,
    worker_id: str,
) -> None:
    if not worksheet_name:
        return
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None,
        lambda: release_write_lock_sync(
            index_spreadsheet, worksheet_name, court_type, worker_id
        ),
    )

def slice_for_shard(
    items: list[Any],
    shard_id: int,
    total_shards: int,
) -> list[Any]:
    """
    Return the shard_id-th slice (1-based) of items across total_shards parts.

    Example: 10 items, 3 shards → sizes 4,3,3.
    """
    if total_shards <= 1 or not items:
        return list(items)
    sid = max(1, min(shard_id, total_shards))
    n = len(items)
    start = (sid - 1) * n // total_shards
    end = sid * n // total_shards
    return items[start:end]
