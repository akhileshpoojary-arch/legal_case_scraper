
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
    DC_DETAIL_WORKERS,
    DC_END_YEAR,
    DC_SEARCH_WORKERS,
    SHEET_FLUSH_CASES,
    DC_TELEMETRY_EVERY,
    DC_PROGRESS_FILE,
    DC_START_YEAR,
    DETAIL_SESSION_POOL_SIZE,
    SYSTEM_SHARD_ID,
)
from daily_run.district_court.extractor import DCContinuousExtractor, DC_STATES
from daily_run.district_court.parser import build_dc_row, parse_detail_html
from daily_run.sheets_manager import DailyRunSheetsManager
from utils.normalize import normalize_row
from utils.proxy import ProxyRotator
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.daily_run.dc.scraper")

STATUSES = ["Pending", "Disposed"]

class DCContinuousScraper:

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
        self._extractor = DCContinuousExtractor(self._sm, self._detail_sessions)
        self._sheets = DailyRunSheetsManager()
        self._states_slice: list[dict[str, str]] = []
        self._district_cache: dict[str, list[dict[str, str]]] = {}
        self._complex_cache: dict[tuple[str, str], list[dict[str, str]]] = {}
        self._establishment_cache: dict[
            tuple[str, str, str], list[dict[str, str]]
        ] = {}
        self._case_type_cache: dict[
            tuple[str, str, str, str], list[dict[str, str]]
        ] = {}

    async def close(self) -> None:
        seen: set[int] = set()
        for sm in self._detail_sessions:
            sid = id(sm)
            if sid in seen:
                continue
            seen.add(sid)
            await sm.close()

    def _load_progress(self) -> dict:
        p = Path(DC_PROGRESS_FILE)
        if p.exists():
            with p.open() as f:
                return json.load(f)
        return {
            "state_idx": 0,
            "dist_idx": 0,
            "complex_idx": 0,
            "est_idx": 0,
            "case_type_idx": 0,
            "year": DC_END_YEAR,
            "status_idx": 0,
            "pending": [],
        }

    def _save_progress(self, prog: dict) -> None:
        with open(DC_PROGRESS_FILE, "w") as f:
            json.dump(prog, f, indent=4)

    async def run(self) -> None:
        from utils.captcha import warm_up_reader

        warm_up_reader()
        logger.info("Starting DC Continuous 24/7 Scraper...")

        def refresh_state_slice() -> None:
            cfg = read_config_row_sync(
                self._sheets._index_sh, CONFIG_WORKSHEET_NAME
            )
            total = max(1, int(cfg.get("total_systems", 1)))
            self._states_slice = slice_for_shard(
                list(DC_STATES), SYSTEM_SHARD_ID, total
            )
            logger.info(
                "[DC] Cluster total_systems=%d shard_id=%d → %d states in this worker",
                total,
                SYSTEM_SHARD_ID,
                len(self._states_slice),
            )

        refresh_state_slice()

        while True:
            try:
                states = self._states_slice
                if not states:
                    logger.error("No states found. Retrying in 10s...")
                    await asyncio.sleep(10)
                    continue

                prog = self._load_progress()
                s_idx = prog.get("state_idx", 0)

                if s_idx >= len(states):
                    # Flush any pending cases before starting over
                    pending = prog.get("pending", [])
                    if pending:
                        logger.info("[DC] Flushing %d pending cases before reset...", len(pending))
                        await self._sheets.write_cases("dc", pending)
                        pending.clear()

                    prog = {
                        "state_idx": 0,
                        "dist_idx": 0,
                        "complex_idx": 0,
                        "est_idx": 0,
                        "case_type_idx": 0,
                        "year": DC_END_YEAR,
                        "status_idx": 0,
                        "pending": [],
                    }
                    self._save_progress(prog)
                    continue

                state = states[s_idx]
                state_code = state["state_code"]

                districts = self._district_cache.get(state_code)
                if districts is None:
                    districts = await self._extractor.get_districts(state_code)
                    self._district_cache[state_code] = districts
                logger.debug(
                    "[DC] Found %d districts in %s", len(districts), state["name"]
                )
                d_idx = prog.get("dist_idx", 0)
                if d_idx >= len(districts):
                    prog["state_idx"] += 1
                    prog["dist_idx"] = 0
                    self._save_progress(prog)
                    continue

                dist = districts[d_idx]
                dist_code = dist["dist_code"]

                complex_key = (state_code, dist_code)
                complexes = self._complex_cache.get(complex_key)
                if complexes is None:
                    complexes = await self._extractor.get_complexes(state_code, dist_code)
                    self._complex_cache[complex_key] = complexes
                logger.debug(
                    "[DC] Found %d complexes in %s", len(complexes), dist["dist_name"]
                )
                c_idx = prog.get("complex_idx", 0)
                if c_idx >= len(complexes):
                    prog["dist_idx"] += 1
                    prog["complex_idx"] = 0
                    self._save_progress(prog)
                    continue

                complex_data = complexes[c_idx]
                cplx_code = complex_data["complex_code"]

                est_key = (state_code, dist_code, cplx_code)
                establishments = self._establishment_cache.get(est_key)
                if establishments is None:
                    establishments = await self._extractor.get_establishments(
                        state_code, dist_code, cplx_code
                    )
                    self._establishment_cache[est_key] = establishments
                if not establishments:
                    establishments = [
                        {"est_code": "", "est_name": "All Establishments"}
                    ]

                e_idx = prog.get("est_idx", 0)
                if e_idx >= len(establishments):
                    prog["complex_idx"] += 1
                    prog["est_idx"] = 0
                    self._save_progress(prog)
                    continue

                est = establishments[e_idx]
                est_code = est["est_code"]

                ct_key = (state_code, dist_code, cplx_code, est_code)
                case_types = self._case_type_cache.get(ct_key)
                if case_types is None:
                    case_types = await self._extractor.get_case_types(
                        state_code, dist_code, cplx_code, est_code
                    )
                    self._case_type_cache[ct_key] = case_types
                logger.debug(
                    "[DC] Found %d case types in %s", len(case_types), est["est_name"]
                )
                if not case_types:
                    logger.warning(
                        "No case types for %s/%s/%s/%s. Skipping.",
                        state_code,
                        dist_code,
                        cplx_code,
                        est_code,
                    )
                    prog["est_idx"] += 1
                    prog["case_type_idx"] = 0
                    self._save_progress(prog)
                    continue

                ct_idx = prog.get("case_type_idx", 0)
                if ct_idx >= len(case_types):
                    prog["est_idx"] += 1
                    prog["case_type_idx"] = 0
                    self._save_progress(prog)
                    continue

                ct = case_types[ct_idx]
                case_type_code = ct["type_code"]
                case_type_name = ct["type_name"]

                yr = prog.get("year", DC_END_YEAR)
                if yr < DC_START_YEAR:
                    prog["case_type_idx"] += 1
                    prog["year"] = DC_END_YEAR
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
                    "[DC 24/7] state=%s dist=%s est=%s type=%s year=%d status=%s",
                    state["name"],
                    dist["dist_name"],
                    est["est_name"],
                    case_type_name,
                    yr,
                    target_status,
                )

                search_started = time.monotonic()
                worker_count = max(1, int(DC_SEARCH_WORKERS))
                detail_limit = max(1, int(DC_DETAIL_WORKERS))
                telemetry_every = max(20, int(DC_TELEMETRY_EVERY))
                write_batch_size = max(1, int(SHEET_FLUSH_CASES))

                queue: asyncio.Queue[dict | None] = asyncio.Queue()
                pending_detail_tasks: set[asyncio.Task[dict]] = set()

                court_info = {
                    "state_name": state["name"],
                    "dist_name": dist["dist_name"],
                    "complex_name": complex_data["complex_name"],
                    "est_name": est["est_name"],
                    "selected_case_type": case_type_name,
                }

                cases, count, search_state = await self._extractor.search_cases_by_type(
                    state_code,
                    dist_code,
                    cplx_code,
                    est_code,
                    yr,
                    case_type_code,
                    target_status,
                )
                if search_state == "retryable_error":
                    logger.warning(
                        "[DC] Search unstable for state=%s dist=%s complex=%s est=%s type=%s year=%d status=%s; retrying same block.",
                        state_code,
                        dist_code,
                        cplx_code,
                        est_code,
                        case_type_code,
                        yr,
                        target_status,
                    )
                    await asyncio.sleep(3)
                    continue

                async def enqueue_worker(worker_idx: int) -> None:
                    for idx in range(worker_idx, len(cases), worker_count):
                        await queue.put(cases[idx])
                    await queue.put(None)

                async def build_row(case_data: dict) -> dict:
                    html = await self._extractor.fetch_case_detail(
                        state_code, dist_code, cplx_code, case_data
                    )
                    parsed = parse_detail_html(html) if html else None
                    row = build_dc_row(detail=parsed, fallback=case_data, court=court_info)
                    normalize_row(row)
                    return row

                async def flush_details(block: bool = False) -> tuple[int, int, list[dict]]:
                    if not pending_detail_tasks:
                        return 0, 0, []
                    if block:
                        done, _ = await asyncio.wait(
                            pending_detail_tasks, return_when=asyncio.FIRST_COMPLETED
                        )
                    else:
                        done = {t for t in pending_detail_tasks if t.done()}
                        if not done:
                            return 0, 0, []
                    success = 0
                    failure = 0
                    rows: list[dict] = []
                    for task in done:
                        pending_detail_tasks.discard(task)
                        try:
                            row = task.result()
                        except Exception:
                            # logger.exception("[DC] Detail task failed.")
                            failure += 1
                            continue
                        success += 1
                        rows.append(row)
                    return success, failure, rows

                search_tasks = [
                    asyncio.create_task(enqueue_worker(i)) for i in range(worker_count)
                ]
                await asyncio.gather(*search_tasks)

                pending = prog.setdefault("pending", [])
                seen_case_keys: set[str] = set()
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
                            logger.warning("[DC] Cancelling stale detail task (elapsed %.1fs)", now - start_time)
                            task.cancel()
                            task_start_times.pop(task, None)
                        elif task.done():
                            task_start_times.pop(task, None)

                while finished_workers < worker_count:
                    await monitor_stale_tasks()
                    # Non-blocking check for any finished tasks
                    s, f, rows = await flush_details(block=False)
                    detail_success_total += s
                    detail_failure_total += f
                    pending.extend(rows)
                    while len(pending) >= write_batch_size:
                        chunk = pending[:write_batch_size]
                        await self._sheets.write_cases("dc", chunk)
                        written_total += len(chunk)
                        del pending[: len(chunk)]

                    # Block until an item is available from the search workers (or they all finish)
                    # We use a timeout to ensure we periodically check for finished tasks and stale tasks
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=10.0)
                    except asyncio.TimeoutError:
                        continue

                    if item is None:
                        finished_workers += 1
                        continue

                    case_key = f"{item.get('cino','')}::{item.get('case_no','')}"
                    if case_key in seen_case_keys:
                        continue
                    seen_case_keys.add(case_key)

                    # Start detail task
                    task = asyncio.create_task(build_row(item))
                    pending_detail_tasks.add(task)
                    task_start_times[task] = time.monotonic()
                    enqueued += 1

                    # Telemetry
                    if enqueued % telemetry_every == 0:
                        logger.info(
                            "[DC] Pipeline telemetry: enqueued=%d detail_ok=%d detail_fail=%d in_flight=%d buffer=%d written=%d",
                            enqueued,
                            detail_success_total,
                            detail_failure_total,
                            len(pending_detail_tasks),
                            len(pending),
                            written_total,
                        )

                    # If at concurrency limit, block until at least one task finishes
                    while len(pending_detail_tasks) >= detail_limit:
                        await monitor_stale_tasks()
                        s, f, rows = await flush_details(block=True)
                        detail_success_total += s
                        detail_failure_total += f
                        pending.extend(rows)
                        while len(pending) >= write_batch_size:
                            chunk = pending[:write_batch_size]
                            await self._sheets.write_cases("dc", chunk)
                            written_total += len(chunk)
                            del pending[: len(chunk)]

                # Final drain phase: Wait for all remaining detail tasks to finish
                if pending_detail_tasks:
                    logger.info("[DC] Draining %d remaining detail tasks...", len(pending_detail_tasks))
                    while pending_detail_tasks:
                        await monitor_stale_tasks()
                        s, f, rows = await flush_details(block=True)
                        detail_success_total += s
                        detail_failure_total += f
                        pending.extend(rows)

                if pending:
                    logger.info("[DC] Final flush: writing %d remaining cases...", len(pending))
                    await self._sheets.write_cases("dc", pending)
                    written_total += len(pending)
                    pending.clear()

                search_elapsed = time.monotonic() - search_started
                logger.info(
                    "[DC] Stage summary: total=%.2fs search_total=%d detail_ok=%d detail_fail=%d pending_buffer=%d written=%d",
                    search_elapsed,
                    count,
                    detail_success_total,
                    detail_failure_total,
                    len(pending),
                    written_total,
                )

                prog["status_idx"] += 1
                self._save_progress(prog)

            except Exception as e:
                logger.error("Unexpected error in DC block: %s", e, exc_info=True)
                await asyncio.sleep(10)
