
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
from daily_run.cluster import (
    bounded_detail_pipeline,
    read_config_row_sync,
    slice_for_shard,
)
from daily_run.config import (
    CONFIG_WORKSHEET_NAME,
    DETAIL_SESSION_POOL_SIZE,
    HC_END_YEAR,
    HC_PROGRESS_FILE,
    HC_DETAIL_WORKERS,
    SHEET_FLUSH_CASES,
    HC_TELEMETRY_EVERY,
    HC_START_YEAR,
    SYSTEM_SHARD_ID,
    WORKER_LABEL,
)
from daily_run.high_court.extractor import HCContinuousExtractor, HIGH_COURTS
from daily_run.high_court.parser import build_hc_row, parse_detail_html
from daily_run.sheets_manager import DailyRunSheetsManager
from utils.logging_utils import (
    descending_year_progress,
    hc_target_label,
    stage_progress,
)
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
                "[HC] Worker slice: worker=%s total_systems=%d shard_id=%d courts=%d detail_workers=%d",
                WORKER_LABEL,
                total,
                SYSTEM_SHARD_ID,
                len(self._courts_slice),
                max(1, int(HC_DETAIL_WORKERS)),
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
                state_progress = stage_progress(s_idx, len(states))
                bench_progress = stage_progress(b_idx, len(benches))
                case_type_progress = stage_progress(ct_idx, len(case_types))
                year_progress = descending_year_progress(yr, HC_START_YEAR, HC_END_YEAR)
                status_progress = stage_progress(status_idx, len(STATUSES))
                target_label = hc_target_label(
                    state["name"],
                    bench["bench_name"],
                    ct["type_name"],
                    yr,
                    target_status,
                )

                logger.info(
                    "[HC] Selection ready: worker=%s target={%s} progress=courts:%s benches:%s case_types:%s years:%s statuses:%s sheet_flush_at=%d session_written=%d session_detail_ok=%d",
                    WORKER_LABEL,
                    target_label,
                    state_progress,
                    bench_progress,
                    case_type_progress,
                    year_progress,
                    status_progress,
                    max(1, int(SHEET_FLUSH_CASES)),
                    self._session_written_total,
                    self._session_detail_total,
                )

                search_started = time.monotonic()
                detail_limit = max(1, int(HC_DETAIL_WORKERS))
                telemetry_every = max(20, int(HC_TELEMETRY_EVERY))
                write_batch_size = max(1, int(SHEET_FLUSH_CASES))

                court_info = {
                    "name": state["name"],
                    "bench_name": bench["bench_name"],
                    "selected_case_type": ct.get("type_name", ""),
                }

                cases, count, search_state = await self._extractor.search_cases_by_type(
                    state_code,
                    court_code,
                    yr,
                    case_type_code,
                    target_status,
                    search_label=target_label,
                )
                if search_state == "retryable_error":
                    logger.warning(
                        "[HC] Search unstable: worker=%s target={%s}; retrying same block.",
                        WORKER_LABEL,
                        target_label,
                    )
                    await asyncio.sleep(3)
                    continue

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

                async def write_rows(rows: list[dict[str, Any]]) -> int:
                    return await self._sheets.write_cases("hc", rows)

                pipe_stats = await bounded_detail_pipeline(
                    items=cases,
                    build_row=build_row,
                    write_rows=write_rows,
                    detail_limit=detail_limit,
                    write_batch_size=write_batch_size,
                    telemetry_every=telemetry_every,
                    logger=logger,
                    log_prefix="HC",
                    target_label=target_label,
                    worker_label=WORKER_LABEL,
                    session_written_base=self._session_written_total,
                )

                detail_success_total = pipe_stats.detail_success
                detail_failure_total = pipe_stats.detail_failure
                written_total = pipe_stats.written
                search_elapsed = time.monotonic() - search_started
                self._session_stage_total += 1
                self._session_detail_total += detail_success_total
                self._session_written_total += written_total
                logger.info(
                    "[HC] Stage summary: worker=%s target={%s} total=%.2fs search_total=%d detail_ok=%d detail_fail=%d written=%d session_written=%d session_detail_ok=%d stages_done=%d",
                    WORKER_LABEL,
                    target_label,
                    search_elapsed,
                    count,
                    detail_success_total,
                    detail_failure_total,
                    written_total,
                    self._session_written_total,
                    self._session_detail_total,
                    self._session_stage_total,
                )

                prog["status_idx"] += 1
                self._save_progress(prog)

            except Exception as e:
                logger.error("Unexpected error in HC block: %s", e, exc_info=True)
                await asyncio.sleep(10)
