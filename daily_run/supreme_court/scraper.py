
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

from config import (
    HTTP_CLIENT,
    MAX_PROXY_FAILURES,
    PROXY_BAN_DURATION,
    PROXY_FILE,
)
from daily_run.cluster import read_config_row_sync, slice_for_shard
from daily_run.config import (
    CONFIG_WORKSHEET_NAME,
    DETAIL_SESSION_POOL_SIZE,
    SC_END_YEAR,
    SC_MAX_CONSECUTIVE_FAILURES,
    SC_PROGRESS_FILE,
    SC_SEARCH_WORKERS,
    SHEET_FLUSH_CASES,
    SC_START_YEAR,
    SYSTEM_SHARD_ID,
    WORKER_LABEL,
)
from daily_run.supreme_court.extractor import SCI_CASE_TYPES, SCIContinuousExtractor
from daily_run.supreme_court.parser import build_sc_row
from daily_run.sheets_manager import DailyRunSheetsManager
from utils.logging_utils import (
    descending_year_progress,
    sc_target_label,
    stage_progress,
)
from utils.normalize import normalize_row
from utils.proxy import ProxyRotator
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.daily_run.sc.scraper")

def _sc_empty_jump(consecutive_empty: int) -> int:
    """Adaptive case_no jump to skip long empty ranges faster."""
    if consecutive_empty <= 10:
        return 1
    if consecutive_empty <= 20:
        return 5
    if consecutive_empty <= 30:
        return 10
    if consecutive_empty <= 40:
        return 50
    if consecutive_empty <= 50:
        return 100
    if consecutive_empty <= 60:
        return 500
    return 1000

class SCContinuousScraper:

    def __init__(self) -> None:
        self._proxy_rotator = ProxyRotator(
            PROXY_FILE,
            max_failures=MAX_PROXY_FAILURES,
            ban_duration=PROXY_BAN_DURATION,
        )
        hdr = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "referer": "https://www.sci.gov.in/case-status-case-no/",
            "x-requested-with": "XMLHttpRequest",
        }
        self._sm = SessionManager(
            client_type=HTTP_CLIENT,
            headers=hdr,
            max_failures=10,
            semaphore_limit=30,
            request_delay=0.1,
            proxy_rotator=self._proxy_rotator,
        )
        # Each detail session gets independent cookies → independent captcha challenges
        n_detail = max(1, int(DETAIL_SESSION_POOL_SIZE))
        self._detail_sessions: list[SessionManager] = [self._sm]
        for _ in range(1, n_detail):
            self._detail_sessions.append(
                SessionManager(
                    client_type=HTTP_CLIENT,
                    headers=dict(hdr),
                    max_failures=10,
                    semaphore_limit=30,
                    request_delay=0.1,
                    proxy_rotator=self._proxy_rotator,
                )
            )
        self._extractor = SCIContinuousExtractor(self._sm, self._detail_sessions)
        n_search = max(1, int(SC_SEARCH_WORKERS))
        self._search_extractors: list[SCIContinuousExtractor] = [self._extractor]
        for _ in range(1, n_search):
            search_sm = SessionManager(
                client_type=HTTP_CLIENT,
                headers=dict(hdr),
                max_failures=10,
                semaphore_limit=30,
                request_delay=0.1,
                proxy_rotator=self._proxy_rotator,
            )
            self._search_extractors.append(
                SCIContinuousExtractor(search_sm, self._detail_sessions)
            )
        self._sheets = DailyRunSheetsManager()
        self._case_types_slice: list[dict[str, str]] = []
        self._session_written_total = 0
        self._session_detail_total = 0
        self._session_stage_total = 0

    async def close(self) -> None:
        seen: set[int] = set()
        for sm in self._detail_sessions:
            sid = id(sm)
            if sid in seen:
                continue
            seen.add(sid)
            await sm.close()
        for ex in self._search_extractors:
            sm = ex._sm
            sid = id(sm)
            if sid in seen:
                continue
            seen.add(sid)
            await sm.close()

    def _load_progress(self) -> dict:
        p = Path(SC_PROGRESS_FILE)
        if p.exists():
            with p.open() as f:
                return json.load(f)
        return {
            "case_type_idx": 0,
            "year": SC_END_YEAR,
            "case_no": 1,
        }

    def _save_progress(self, prog: dict) -> None:
        with open(SC_PROGRESS_FILE, "w") as f:
            json.dump(prog, f, indent=4)

    async def run(self) -> None:
        from utils.captcha import warm_up_reader

        warm_up_reader()
        logger.info("Starting SC Continuous 24/7 Scraper (Case Type + Case No)...")

        def refresh_case_type_slice() -> None:
            cfg = read_config_row_sync(
                self._sheets._index_sh, CONFIG_WORKSHEET_NAME
            )
            total = max(1, int(cfg.get("total_systems", 1)))
            self._case_types_slice = slice_for_shard(
                list(SCI_CASE_TYPES), SYSTEM_SHARD_ID, total
            )
            logger.info(
                "[SC] Worker slice: worker=%s total_systems=%d shard_id=%d case_types=%d search_workers=%d detail_sessions=%d",
                WORKER_LABEL,
                total,
                SYSTEM_SHARD_ID,
                len(self._case_types_slice),
                len(self._search_extractors),
                len(self._detail_sessions),
            )

        refresh_case_type_slice()

        while True:
            try:
                prog = self._load_progress()
                case_types = self._case_types_slice

                ct_idx = prog.get("case_type_idx", 0)
                if ct_idx >= len(case_types):
                    logger.info("COMPLETED FULL SC RUN! Resetting.")
                    prog = {
                        "case_type_idx": 0,
                        "year": SC_END_YEAR,
                        "case_no": 1,
                    }
                    self._save_progress(prog)
                    continue

                ct = case_types[ct_idx]
                ct_code = ct["code"]
                ct_name = ct["name"]

                year = prog.get("year", SC_END_YEAR)
                if year < SC_START_YEAR:
                    prog["case_type_idx"] += 1
                    prog["year"] = SC_END_YEAR
                    prog["case_no"] = 1
                    self._save_progress(prog)
                    continue

                case_no_start = prog.get("case_no", 1)
                total_case_types = len(case_types)
                years_progress = descending_year_progress(
                    year, SC_START_YEAR, SC_END_YEAR
                )
                case_type_progress = stage_progress(ct_idx, total_case_types)
                target_label = sc_target_label(ct_name, ct_code, year)

                logger.info(
                    "[SC] Selection ready: worker=%s target={%s} next_case_no=%d progress=case_types:%s years:%s sheet_flush_at=%d session_written=%d session_detail_ok=%d",
                    WORKER_LABEL,
                    target_label,
                    case_no_start,
                    case_type_progress,
                    years_progress,
                    max(1, int(SHEET_FLUSH_CASES)),
                    self._session_written_total,
                    self._session_detail_total,
                )

                batch_results: list[dict[str, Any]] = []
                pending_detail_tasks: set[asyncio.Task[dict[str, Any] | None]] = set()
                search_started = time.monotonic()
                search_workers = self._search_extractors
                worker_count = len(search_workers)
                max_searched_case_no = case_no_start
                max_case_lock = asyncio.Lock()
                result_queue: asyncio.Queue[tuple[dict[str, Any], int] | None] = asyncio.Queue()
                progress_flush_every = max(10, worker_count * 5)
                search_tick = 0
                detail_success_total = 0
                detail_failure_total = 0
                written_total = 0
                telemetry_every = max(100, worker_count * 25)

                async def build_case_row(
                    case_result: dict[str, Any],
                    searched_case_no: int,
                ) -> dict[str, Any] | None:
                    case_result["searched_case_no"] = str(searched_case_no)
                    case_result["searched_case_type"] = ct_name
                    case_result["sc_type_code"] = ct_code
                    case_result["sc_year"] = str(year)
                    diary_no = case_result.get("diary_no", "")
                    diary_year = case_result.get("diary_year", "")
                    if diary_no and diary_year:
                        tabs = await self._extractor.fetch_detail(diary_no, diary_year)
                        row = build_sc_row(detail=tabs, fallback=case_result)
                    else:
                        row = build_sc_row(detail={}, fallback=case_result)
                    normalize_row(row)
                    return row

                async def drain_completed_details(block: bool = False) -> None:
                    nonlocal batch_results, detail_success_total, detail_failure_total, written_total
                    if not pending_detail_tasks:
                        return

                    done: set[asyncio.Task[dict[str, Any] | None]]
                    if block:
                        done, _ = await asyncio.wait(
                            pending_detail_tasks,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    else:
                        done = {t for t in pending_detail_tasks if t.done()}
                        if not done:
                            return

                    for task in done:
                        pending_detail_tasks.discard(task)
                        try:
                            row = task.result()
                        except Exception:
                            logger.exception("[SC] Detail task failed.")
                            detail_failure_total += 1
                            continue
                        if not row:
                            detail_failure_total += 1
                            continue
                        detail_success_total += 1
                        batch_results.append(row)

                    while len(batch_results) >= SHEET_FLUSH_CASES:
                        chunk = batch_results[:SHEET_FLUSH_CASES]
                        logger.info(
                            "[SC] Writing batch of %d cases to Google Sheet...",
                            len(chunk),
                        )
                        write_started = time.monotonic()
                        written_total += await self._sheets.write_cases("sc", chunk)
                        logger.info(
                            "[SC] Batch write took %.2fs for %d rows",
                            time.monotonic() - write_started,
                            len(chunk),
                        )
                        del batch_results[:SHEET_FLUSH_CASES]

                async def sc_search_worker(
                    worker_idx: int,
                    extractor: SCIContinuousExtractor,
                ) -> None:
                    nonlocal max_searched_case_no
                    case_no = case_no_start + worker_idx
                    consecutive_empty = 0
                    search_stride = worker_count

                    scid: str | None = None
                    tok_name: str | None = None
                    tok_value: str | None = None

                    while consecutive_empty < SC_MAX_CONSECUTIVE_FAILURES:
                        if not scid or not tok_name or not tok_value:
                            scid, tok_name, tok_value = await extractor.get_base_tokens()
                            if not scid or not tok_name:
                                await asyncio.sleep(1.0)
                                continue

                        result = await extractor.search_by_case_no(
                            case_type=ct_code,
                            case_no=case_no,
                            year=year,
                            scid=scid,
                            tok_name=tok_name,
                            tok_value=tok_value,
                            search_label=target_label,
                        )
                        search_state = (
                            result.get("_search_state")
                            if isinstance(result, dict)
                            else None
                        )
                        if search_state in {"captcha_error", "retryable_error"}:
                            scid = tok_name = tok_value = None
                            await asyncio.sleep(0.25)
                            continue

                        async with max_case_lock:
                            if case_no > max_searched_case_no:
                                max_searched_case_no = case_no

                        if result is None:
                            consecutive_empty += 1
                            jump = _sc_empty_jump(consecutive_empty)
                            case_no += max(search_stride, jump)
                            if consecutive_empty % 100 == 0:
                                scid = tok_name = tok_value = None
                            continue

                        consecutive_empty = 0
                        await result_queue.put((result, case_no))
                        case_no += search_stride
                        if case_no % 200 == 0:
                            scid = tok_name = tok_value = None

                    await result_queue.put(None)

                worker_tasks = [
                    asyncio.create_task(sc_search_worker(idx, ex))
                    for idx, ex in enumerate(search_workers)
                ]

                finished_workers = 0
                consumed_results = 0
                detail_worker_limit = max(6, len(self._detail_sessions) * 2)
                while finished_workers < worker_count:
                    await drain_completed_details(block=False)
                    item = await result_queue.get()
                    search_tick += 1
                    if item is None:
                        finished_workers += 1
                        continue
                    result, searched_case_no = item
                    consumed_results += 1
                    logger.debug(
                        "[SC] Found result: %s", result.get("case_number", "unknown")
                    )
                    pending_detail_tasks.add(
                        asyncio.create_task(build_case_row(result, searched_case_no))
                    )
                    while len(pending_detail_tasks) >= detail_worker_limit:
                        await drain_completed_details(block=True)

                    if consumed_results % progress_flush_every == 0:
                        prog["case_no"] = max_searched_case_no + 1
                        self._save_progress(prog)

                    if search_tick % 100 == 0:
                        prog["case_no"] = max_searched_case_no + 1
                        self._save_progress(prog)
                    if search_tick % telemetry_every == 0:
                        metric_totals = {
                            "attempts": 0,
                            "search_hits": 0,
                            "no_records": 0,
                            "retryable_errors": 0,
                        }
                        for ex in search_workers:
                            metrics = ex.snapshot_search_metrics()
                            for key in metric_totals:
                                metric_totals[key] += int(metrics.get(key, 0))
                        detail_done = detail_success_total + detail_failure_total
                        logger.info(
                            "[SC] Pipeline telemetry: worker=%s target={%s} case_no_start=%d searched_upto=%d hits=%d detail_started=%d detail_done=%d detail_ok=%d detail_fail=%d detail_left=%d in_flight=%d buffer=%d/%d stage_written=%d session_written=%d attempts=%d no_records=%d retryable=%d",
                            WORKER_LABEL,
                            target_label,
                            case_no_start,
                            max_searched_case_no,
                            consumed_results,
                            consumed_results,
                            detail_done,
                            detail_success_total,
                            detail_failure_total,
                            max(consumed_results - detail_done, 0),
                            len(pending_detail_tasks),
                            len(batch_results),
                            max(1, int(SHEET_FLUSH_CASES)),
                            written_total,
                            self._session_written_total + written_total,
                            metric_totals["attempts"],
                            metric_totals["no_records"],
                            metric_totals["retryable_errors"],
                        )

                for task in worker_tasks:
                    await task

                while pending_detail_tasks:
                    await drain_completed_details(block=True)
                if batch_results:
                    logger.info(
                        "[SC] Writing remaining %d cases to Google Sheet...",
                        len(batch_results),
                    )
                    write_started = time.monotonic()
                    written_total += await self._sheets.write_cases("sc", batch_results)
                    logger.info(
                        "[SC] Remaining write took %.2fs for %d rows",
                        time.monotonic() - write_started,
                        len(batch_results),
                    )
                search_elapsed = time.monotonic() - search_started
                captcha_totals = {
                    "attempts": 0,
                    "captcha_solved": 0,
                    "captcha_rejected": 0,
                    "captcha_empty": 0,
                    "captcha_image_missing": 0,
                    "retryable_errors": 0,
                    "transport_failures": 0,
                    "search_hits": 0,
                    "no_records": 0,
                    "captcha_exhausted": 0,
                    "hard_failures": 0,
                }
                for ex in search_workers:
                    metrics = ex.drain_search_metrics()
                    for key, value in metrics.items():
                        captcha_totals[key] = captcha_totals.get(key, 0) + int(value)

                self._session_stage_total += 1
                self._session_detail_total += detail_success_total
                self._session_written_total += written_total
                logger.info(
                    "[SC] Captcha summary: worker=%s target={%s} attempts=%d solved=%d rejected=%d empty=%d no_image=%d exhausted=%d retryable=%d",
                    WORKER_LABEL,
                    target_label,
                    captcha_totals["attempts"],
                    captcha_totals["captcha_solved"],
                    captcha_totals["captcha_rejected"],
                    captcha_totals["captcha_empty"],
                    captcha_totals["captcha_image_missing"],
                    captcha_totals["captcha_exhausted"],
                    captcha_totals["retryable_errors"],
                )
                logger.info(
                    "[SC] Stage summary: worker=%s target={%s} case_no_start=%d searched_upto=%d duration=%.2fs search_hits=%d detail_ok=%d detail_fail=%d written=%d no_records=%d session_written=%d session_detail_ok=%d stages_done=%d",
                    WORKER_LABEL,
                    target_label,
                    case_no_start,
                    max_searched_case_no,
                    search_elapsed,
                    consumed_results,
                    detail_success_total,
                    detail_failure_total,
                    written_total,
                    captcha_totals["no_records"],
                    self._session_written_total,
                    self._session_detail_total,
                    self._session_stage_total,
                )

                logger.info(
                    "[SC] Block complete: worker=%s target={%s} exhausted_at_case_no=%d empty_cutoff=%d next_year=%d next_case_no=%d session_written=%d",
                    WORKER_LABEL,
                    target_label,
                    max_searched_case_no,
                    SC_MAX_CONSECUTIVE_FAILURES,
                    year - 1,
                    1,
                    self._session_written_total,
                )

                prog["year"] = year - 1
                prog["case_no"] = 1
                self._save_progress(prog)

            except Exception as e:
                logger.error("Unexpected error in SC block: %s", e, exc_info=True)
                await asyncio.sleep(10)
