
from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from utils.logging_utils import format_percent

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
    loop = asyncio.get_running_loop()
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
    loop = asyncio.get_running_loop()
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


@dataclass(slots=True)
class PipelineStats:
    """Counters returned by the bounded case-detail pipeline."""

    search_total: int
    detail_started: int = 0
    detail_success: int = 0
    detail_failure: int = 0
    duplicates_skipped: int = 0
    written: int = 0


async def bounded_detail_pipeline(
    *,
    items: Sequence[Any],
    build_row: Callable[[Any], Awaitable[dict[str, Any] | None]],
    write_rows: Callable[[list[dict[str, Any]]], Awaitable[int]],
    detail_limit: int,
    write_batch_size: int,
    telemetry_every: int,
    logger: logging.Logger,
    log_prefix: str,
    target_label: str,
    worker_label: str,
    key_func: Callable[[Any], str] | None = None,
    task_timeout_seconds: float = 600.0,
    session_written_base: int = 0,
) -> PipelineStats:
    """
    Build detail rows with bounded concurrency and bounded write buffering.

    HC/DC searches return a list of summary rows. The old scraper first enqueued
    the full list and only then began detail work, which delayed every write and
    duplicated memory. This helper starts detail requests immediately, caps
    in-flight tasks, and writes rows as soon as a batch is ready.
    """

    detail_limit = max(1, int(detail_limit))
    write_batch_size = max(1, int(write_batch_size))
    telemetry_every = max(1, int(telemetry_every))
    task_timeout_seconds = max(1.0, float(task_timeout_seconds))

    stats = PipelineStats(search_total=len(items))
    pending: set[asyncio.Task[dict[str, Any] | None]] = set()
    write_buffer: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    async def timed_build(item: Any) -> dict[str, Any] | None:
        return await asyncio.wait_for(build_row(item), timeout=task_timeout_seconds)

    async def flush_write_buffer(force: bool = False) -> None:
        while write_buffer and (force or len(write_buffer) >= write_batch_size):
            take = len(write_buffer) if force else min(len(write_buffer), write_batch_size)
            chunk = write_buffer[:take]
            del write_buffer[:take]
            stats.written += await write_rows(chunk)

    async def drain_completed(block: bool = False) -> None:
        if not pending:
            return
        if block:
            done, _ = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
            )
        else:
            done = {task for task in pending if task.done()}
            if not done:
                return

        for task in done:
            pending.discard(task)
            try:
                row = task.result()
            except Exception:
                stats.detail_failure += 1
                continue
            if row is None:
                stats.detail_failure += 1
                continue
            stats.detail_success += 1
            write_buffer.append(row)

        await flush_write_buffer(force=False)

    for item in items:
        await drain_completed(block=False)

        if key_func is not None:
            key = key_func(item)
            if key:
                if key in seen_keys:
                    stats.duplicates_skipped += 1
                    continue
                seen_keys.add(key)

        task = asyncio.create_task(timed_build(item))
        pending.add(task)
        stats.detail_started += 1

        if stats.detail_started % telemetry_every == 0:
            detail_done = stats.detail_success + stats.detail_failure
            detail_left = max(
                stats.search_total - detail_done - stats.duplicates_skipped,
                0,
            )
            success_rate = format_percent(stats.detail_success, max(detail_done, 1))
            logger.info(
                "[%s] Pipeline telemetry: worker=%s target={%s} search_total=%d "
                "detail_started=%d detail_done=%d detail_ok=%d detail_fail=%d "
                "detail_left=%d detail_pct=%s detail_success=%s in_flight=%d "
                "write_buffer=%d/%d buffer_pct=%s stage_written=%d "
                "session_written=%d duplicate_skips=%d",
                log_prefix,
                worker_label,
                target_label,
                stats.search_total,
                stats.detail_started,
                detail_done,
                stats.detail_success,
                stats.detail_failure,
                detail_left,
                format_percent(detail_done, max(stats.search_total, 1)),
                success_rate,
                len(pending),
                len(write_buffer),
                write_batch_size,
                format_percent(len(write_buffer), write_batch_size),
                stats.written,
                session_written_base + stats.written,
                stats.duplicates_skipped,
            )

        while len(pending) >= detail_limit:
            await drain_completed(block=True)

    if pending:
        logger.info(
            "[%s] Draining %d remaining detail tasks...",
            log_prefix,
            len(pending),
        )
    while pending:
        await drain_completed(block=True)

    await flush_write_buffer(force=True)
    return stats
