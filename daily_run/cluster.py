
from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from utils.logging_utils import format_main_progress, format_percent

logger = logging.getLogger("legal_scraper.daily_run.cluster")

# Legacy layout: values only in row 1 (A1=total, B1/C1/D1=locks)
_LEGACY_LOCK_CELLS = {"dc": "B1", "hc": "C1", "sc": "D1"}
_VALUE_ROW = 2
_TOTAL_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}

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

def _parse_total_systems(raw: str) -> int:
    """Accept values like '2', '2 PCs', or 'two PCs'."""
    text = str(raw or "").strip().lower()
    if not text:
        return 1
    match = re.search(r"\d+", text)
    if match:
        return max(1, int(match.group(0)))
    for word, value in _TOTAL_WORDS.items():
        if re.search(rf"\b{word}\b", text):
            return value
    return 1

def parse_cluster_row(row: list[str]) -> dict[str, Any]:
    """Parse a single worksheet row as legacy values (A1=total, B1/C1/D1=locks)."""
    def cell(i: int) -> str:
        if i < len(row) and row[i] is not None:
            return str(row[i]).strip()
        return ""

    raw_total = cell(0)
    total_systems = _parse_total_systems(raw_total)

    return {
        "total_systems": total_systems,
        "raw_total_systems": raw_total,
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
    total_systems = _parse_total_systems(raw_total)

    return {
        "total_systems": total_systems,
        "raw_total_systems": raw_total,
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

def ensure_config_worksheet_sync(
    index_spreadsheet: Any,
    worksheet_name: str,
    headers: Sequence[str],
) -> None:
    """Create/populate the config worksheet expected by cluster workers."""
    if not worksheet_name:
        return
    try:
        ws = index_spreadsheet.worksheet(worksheet_name)
    except Exception:
        try:
            ws = index_spreadsheet.add_worksheet(
                title=worksheet_name,
                rows=2,
                cols=max(4, len(headers)),
            )
        except Exception as exc:
            logger.warning("Could not create config worksheet %s: %s", worksheet_name, exc)
            return

    try:
        rows = ws.get_values("A1:Z2")
        header_row = rows[0] if rows else []
        value_row = rows[1] if len(rows) > 1 else []
        if not _row_looks_like_header_row(header_row):
            ws.update("A1", [list(headers)], value_input_option="RAW")
        if not any(str(v).strip() for v in value_row):
            ws.update("A2", [["1", "", "", ""]], value_input_option="RAW")
    except Exception as exc:
        logger.warning("Could not initialize config worksheet %s: %s", worksheet_name, exc)

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

def _parse_lock_value(raw: str) -> tuple[str, float | None]:
    text = str(raw or "").strip()
    if not text:
        return "", None
    if "|" not in text:
        return text, None
    owner, ts = text.rsplit("|", 1)
    try:
        return owner.strip(), float(ts.strip())
    except ValueError:
        return text, None

def try_acquire_write_lock_sync(
    index_spreadsheet: Any,
    worksheet_name: str,
    court_type: str,
    worker_id: str,
    stale_seconds: float,
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
        now = time.time()
        current_raw = (ws.acell(cell_ref).value or "").strip()
        current_owner, current_ts = _parse_lock_value(current_raw)
        lock_is_stale = (
            bool(current_owner)
            and current_ts is not None
            and stale_seconds > 0
            and now - current_ts > stale_seconds
        )
        if current_owner and current_owner != worker_id and not lock_is_stale:
            return False
        claimed_value = f"{worker_id}|{now:.0f}"
        ws.update_acell(cell_ref, claimed_value)
        time.sleep(0.25)
        verify_owner, _verify_ts = _parse_lock_value(ws.acell(cell_ref).value or "")
        if verify_owner == worker_id:
            if lock_is_stale:
                logger.warning(
                    "[%s] Replaced stale sheet write lock owner=%s",
                    court_type.upper(),
                    current_owner,
                )
            return True
        return False
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
        current_owner, _current_ts = _parse_lock_value(ws.acell(cell_ref).value or "")
        if current_owner == worker_id:
            ws.update_acell(cell_ref, "")
    except Exception as exc:
        logger.debug("Write lock release: %s", exc)

async def acquire_write_lock(
    index_spreadsheet: Any,
    worksheet_name: str,
    court_type: str,
    worker_id: str,
    poll_seconds: float,
    stale_seconds: float = 0.0,
) -> None:
    if not worksheet_name:
        return
    loop = asyncio.get_running_loop()
    attempt = 0
    while True:
        ok = await loop.run_in_executor(
            None,
            lambda: try_acquire_write_lock_sync(
                index_spreadsheet,
                worksheet_name,
                court_type,
                worker_id,
                stale_seconds,
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
        return list(items) if shard_id <= 1 else []
    sid = max(1, int(shard_id))
    if sid > total_shards:
        return []
    n = len(items)
    base, extra = divmod(n, total_shards)
    start = (sid - 1) * base + min(sid - 1, extra)
    end = start + base + (1 if sid <= extra else 0)
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
    progress_court: str | None = None,
    progress_name: str = "progress",
    progress_current_name: str = "-",
    progress_current_code: str | int = "-",
    progress_completed: int = 0,
    progress_total: int = 0,
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
            if progress_court:
                logger.info(
                    format_main_progress(
                        court=progress_court,
                        progress_name=progress_name,
                        current_name=progress_current_name,
                        current_code=progress_current_code,
                        completed=progress_completed,
                        total=progress_total,
                        cases_collected=stats.detail_success,
                        written=session_written_base + stats.written,
                        write_buffer=len(write_buffer),
                        write_batch_size=write_batch_size,
                    )
                )
            else:
                logger.info(
                    "[%s] Pipeline: collected=%d failed=%d left=%d success=%s buffer=%d/%d written=%d",
                    log_prefix,
                    stats.detail_success,
                    stats.detail_failure,
                    detail_left,
                    success_rate,
                    len(write_buffer),
                    write_batch_size,
                    session_written_base + stats.written,
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
