
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

from config import (
    HTTP_CLIENT,
    MAX_CONCURRENT,
    MAX_PROXY_FAILURES,
    PROXY_BAN_DURATION,
    PROXY_FILE,
    REQUEST_DELAY,
)
from daily_run.cluster import read_config_row_sync, slice_for_shard
from daily_run.config import (
    CONFIG_WORKSHEET_NAME,
    DETAIL_SESSION_POOL_SIZE,
    HC_END_YEAR,
    HC_PROGRESS_FILE,
    HC_SEARCH_WORKERS,
    HC_DETAIL_WORKERS,
    HC_TELEMETRY_EVERY,
    HC_WRITE_BATCH_SIZE,
    HC_START_YEAR,
    SYSTEM_SHARD_ID,
)
from daily_run.high_court.extractor import HCContinuousExtractor, HIGH_COURTS
from daily_run.high_court.parser import build_hc_row, parse_detail_html
from daily_run.sheets_manager import DailyRunSheetsManager
from utils.normalize import normalize_row
from utils.proxy import ProxyRotator
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.daily_run.hc.scraper")

STATUSES = ["Pending", "Disposed"]

class HCContinuousScraper:

    def __init__(self) -> None:
        self._proxy_rotator = ProxyRotator(
            PROXY_FILE,
            max_failures=MAX_PROXY_FAILURES,
            ban_duration=PROXY_BAN_DURATION,
        )
        self._sm = SessionManager(
            client_type=HTTP_CLIENT,
            headers={},
            max_failures=10,
            semaphore_limit=MAX_CONCURRENT,
            request_delay=REQUEST_DELAY,
            proxy_rotator=self._proxy_rotator,
        )
        # Each detail session gets independent cookies → independent captcha challenges
        n_detail = max(1, int(DETAIL_SESSION_POOL_SIZE))
        self._detail_sessions: list[SessionManager] = [self._sm]
        for _ in range(1, n_detail):
            self._detail_sessions.append(
                SessionManager(
                    client_type=HTTP_CLIENT,
                    headers={},
                    max_failures=10,
                    semaphore_limit=max(30, MAX_CONCURRENT // 2),
                    request_delay=REQUEST_DELAY,
                    proxy_rotator=self._proxy_rotator,
                )
            )
        self._extractor = HCContinuousExtractor(self._sm, self._detail_sessions)
        self._sheets = DailyRunSheetsManager()
        self._bench_cache: dict[str, list[dict[str, str]]] = {}
        self._case_type_cache: dict[tuple[str, str], list[dict[str, str]]] = {}
        self._courts_slice: list[dict[str, str]] = []

    async def close(self) -> None:
        seen: set[int] = set()
        for sm in self._detail_sessions:
            sid = id(sm)
            if sid in seen:
                continue
            seen.add(sid)
            await sm.close()

    def _load_progress(self) -> dict:
        p = Path(HC_PROGRESS_FILE)
        if p.exists():
            with p.open() as f:
                return json.load(f)
        return {
            "state_idx": 0,
            "bench_idx": 0,
            "case_type_idx": 0,
            "year": HC_END_YEAR,
            "status_idx": 0,
        }

    def _save_progress(self, prog: dict) -> None:
        with open(HC_PROGRESS_FILE, "w") as f:
            json.dump(prog, f, indent=4)

    async def run(self) -> None:
        from utils.captcha import warm_up_reader

        warm_up_reader()
        logger.info("Starting HC Continuous 24/7 Scraper...")

        def refresh_court_slice() -> None:
            cfg = read_config_row_sync(
                self._sheets._index_sh, CONFIG_WORKSHEET_NAME
            )
            total = max(1, int(cfg.get("total_systems", 1)))
            self._courts_slice = slice_for_shard(
                list(HIGH_COURTS), SYSTEM_SHARD_ID, total
            )
            logger.info(
                "[HC] Cluster total_systems=%d shard_id=%d → %d high courts on this worker",
                total,
                SYSTEM_SHARD_ID,
                len(self._courts_slice),
            )

        refresh_court_slice()

        while True:
            try:
                states = self._courts_slice
                prog = self._load_progress()
                s_idx = prog.get("state_idx", 0)

                if s_idx >= len(states):
                    logger.info("COMPLETED FULL HC RUN! Resetting.")
                    prog = {
                        "state_idx": 0,
                        "bench_idx": 0,
                        "case_type_idx": 0,
                        "year": HC_END_YEAR,
                        "status_idx": 0,
                    }
                    self._save_progress(prog)
                    continue

                state = states[s_idx]
                state_code = state["state_code"]

                benches = self._bench_cache.get(state_code)
                if benches is None:
                    benches = await self._extractor.get_benches(state_code)
                    self._bench_cache[state_code] = benches
                if not benches:
                    logger.warning("No benches for %s. Skipping.", state_code)
                    prog["state_idx"] += 1
                    prog["bench_idx"] = 0
                    self._save_progress(prog)
                    continue

                b_idx = prog.get("bench_idx", 0)
                if b_idx >= len(benches):
                    prog["state_idx"] += 1
                    prog["bench_idx"] = 0
                    self._save_progress(prog)
                    continue

                bench = benches[b_idx]
                court_code = bench["court_code"]

                ct_key = (state_code, court_code)
                case_types = self._case_type_cache.get(ct_key)
                if case_types is None:
                    case_types = await self._extractor.get_case_types(
                        state_code, court_code
                    )
                    self._case_type_cache[ct_key] = case_types
                logger.debug(
                    "[HC] Found %d case types in %s",
                    len(case_types),
                    bench["bench_name"],
                )
                if not case_types:
                    prog["bench_idx"] += 1
                    prog["case_type_idx"] = 0
                    self._save_progress(prog)
                    continue

                ct_idx = prog.get("case_type_idx", 0)
                if ct_idx >= len(case_types):
                    prog["bench_idx"] += 1
                    prog["case_type_idx"] = 0
                    self._save_progress(prog)
                    continue

                ct = case_types[ct_idx]
                case_type_code = ct["type_code"]

                yr = prog.get("year", HC_END_YEAR)
                if yr < HC_START_YEAR:
                    prog["case_type_idx"] += 1
                    prog["year"] = HC_END_YEAR
                    self._save_progress(prog)
                    continue

                status_idx = prog.get("status_idx", 0)
                if status_idx >= len(STATUSES):
                    prog["year"] -= 1
                    prog["status_idx"] = 0
                    self._save_progress(prog)
                    continue

                target_status = STATUSES[status_idx]

                logger.info(
                    "[HC 24/7] state=%s bench=%s type=%s year=%d status=%s",
                    state["name"],
                    bench["bench_name"],
                    ct["type_name"],
                    yr,
                    target_status,
                )

                search_started = time.monotonic()
                worker_count = max(1, int(HC_SEARCH_WORKERS))
                detail_limit = max(1, int(HC_DETAIL_WORKERS))
                telemetry_every = max(20, int(HC_TELEMETRY_EVERY))
                write_batch_size = max(1, int(HC_WRITE_BATCH_SIZE))

                queue: asyncio.Queue[dict | None] = asyncio.Queue()
                pending_detail_tasks: set[asyncio.Task[dict | None]] = set()
                write_buffer: list[dict[str, Any]] = []

                court_info = {
                    "name": state["name"],
                    "bench_name": bench["bench_name"],
                    "selected_case_type": ct.get("type_name", ""),
                }

                cases, count = await self._extractor.search_cases_by_type(
                    state_code, court_code, yr, case_type_code, target_status
                )

                async def enqueue_worker(worker_idx: int) -> None:
                    for idx in range(worker_idx, len(cases), worker_count):
                        await queue.put(cases[idx])
                    await queue.put(None)

                async def build_row(case_data: dict) -> dict | None:
                    cino = case_data.get("cino", "")
                    case_no = case_data.get("case_no", "")
                    html = await self._extractor.fetch_case_detail(
                        state_code, court_code, case_no, cino
                    )
                    if html in (None, "SQL_ERROR_SKIP"):
                        return None
                    parsed = parse_detail_html(html)
                    row = build_hc_row(detail=parsed, fallback=case_data, court=court_info)
                    normalize_row(row)
                    return row

                async def flush_details(block: bool = False) -> tuple[int, int]:
                    if not pending_detail_tasks:
                        return 0, 0
                    if block:
                        done, _ = await asyncio.wait(
                            pending_detail_tasks, return_when=asyncio.FIRST_COMPLETED
                        )
                    else:
                        done = {t for t in pending_detail_tasks if t.done()}
                        if not done:
                            return 0, 0

                    success = 0
                    failure = 0
                    for task in done:
                        pending_detail_tasks.discard(task)
                        try:
                            row = task.result()
                        except Exception:
                            # logger.exception("[HC] Detail task failed.")
                            failure += 1
                            continue
                        if row is None:
                            failure += 1
                            continue
                        success += 1
                        write_buffer.append(row)
                    return success, failure

                async def flush_write_buffer(force: bool = False) -> int:
                    if not write_buffer:
                        return 0
                    if not force and len(write_buffer) < write_batch_size:
                        return 0
                    chunk = list(write_buffer[:write_batch_size] if not force else write_buffer)
                    del write_buffer[: len(chunk)]
                    await self._sheets.write_cases("hc", chunk)
                    return len(chunk)

                search_tasks = [
                    asyncio.create_task(enqueue_worker(i)) for i in range(worker_count)
                ]
                await asyncio.gather(*search_tasks)

                finished_workers = 0
                enqueued = 0
                detail_success_total = 0
                detail_failure_total = 0
                written_total = 0
                telemetry_tick = 0

                # Tracker for task start times
                task_start_times: dict[asyncio.Task, float] = {}

                async def monitor_stale_tasks():
                    now = time.monotonic()
                    for task, start_time in list(task_start_times.items()):
                        if not task.done() and (now - start_time) > 600: # 10 minutes
                            logger.warning("[HC] Cancelling stale detail task (elapsed %.1fs)", now - start_time)
                            task.cancel()
                            task_start_times.pop(task, None)
                        elif task.done():
                            task_start_times.pop(task, None)

                while finished_workers < worker_count:
                    await monitor_stale_tasks()
                    # Non-blocking check for any finished tasks
                    s, f = await flush_details(block=False)
                    detail_success_total += s
                    detail_failure_total += f
                    written_total += await flush_write_buffer()

                    # Block until an item is available from the search workers (or they all finish)
                    # We use a timeout to ensure we periodically check for finished tasks and stale tasks
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=10.0)
                    except asyncio.TimeoutError:
                        continue

                    if item is None:
                        finished_workers += 1
                        continue

                    # Start detail task
                    task = asyncio.create_task(build_row(item))
                    pending_detail_tasks.add(task)
                    task_start_times[task] = time.monotonic()
                    enqueued += 1

                    # Telemetry (check every N enqueues during the fill phase)
                    if enqueued % telemetry_every == 0:
                        logger.info(
                            "[HC] Pipeline telemetry: enqueued=%d detail_ok=%d detail_fail=%d in_flight=%d write_buffer=%d written=%d",
                            enqueued,
                            detail_success_total,
                            detail_failure_total,
                            len(pending_detail_tasks),
                            len(write_buffer),
                            written_total,
                        )

                    # If at concurrency limit, block until at least one task finishes
                    while len(pending_detail_tasks) >= detail_limit:
                        await monitor_stale_tasks()
                        s, f = await flush_details(block=True)
                        detail_success_total += s
                        detail_failure_total += f
                        written_total += await flush_write_buffer()

                # Final drain phase: Wait for all remaining detail tasks to finish
                if pending_detail_tasks:
                    logger.info("[HC] Draining %d remaining detail tasks...", len(pending_detail_tasks))
                    while pending_detail_tasks:
                        await monitor_stale_tasks()
                        s, f = await flush_details(block=True)
                        detail_success_total += s
                        detail_failure_total += f
                        written_total += await flush_write_buffer()

                written_total += await flush_write_buffer(force=True)
                search_elapsed = time.monotonic() - search_started
                logger.info(
                    "Found %d cases. Search+pipeline stage took %.2fs.",
                    count,
                    search_elapsed,
                )
                logger.info(
                    "[HC] Stage timings: total=%.2fs detail_ok=%d detail_fail=%d written=%d",
                    search_elapsed,
                    detail_success_total,
                    detail_failure_total,
                    written_total,
                )

                prog["status_idx"] += 1
                self._save_progress(prog)

            except Exception as e:
                logger.error("Unexpected error in HC block: %s", e, exc_info=True)
                await asyncio.sleep(10)
