
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
    CLUSTER_CONFIG_REFRESH_SECONDS,
    CONFIG_WORKSHEET_NAME,
    DC_DETAIL_WORKERS,
    DC_END_YEAR,
    SHEET_FLUSH_CASES,
    DC_TELEMETRY_EVERY,
    DC_PROGRESS_FILE,
    DC_START_YEAR,
    DETAIL_SESSION_POOL_SIZE,
    SYSTEM_SHARD_ID,
    WORKER_LABEL,
)
from daily_run.district_court.extractor import DCContinuousExtractor, DC_STATES
from daily_run.district_court.parser import build_dc_row, parse_detail_html
from daily_run.sheets_manager import DailyRunSheetsManager
from utils.logging_utils import (
    dc_target_label,
    descending_year_progress,
    format_duration,
    format_kv_block,
    format_main_progress,
    format_percent,
    stage_progress,
)
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
            name="DC-search",
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
                    semaphore_limit=max(4, min(20, MAX_CONCURRENT // 2)),
                    request_delay=REQUEST_DELAY,
                    proxy_rotator=self._proxy_rotator,
                    name=f"DC-detail-{len(self._detail_sessions) + 1}",
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
        self._session_written_total = 0
        self._session_detail_total = 0
        self._session_stage_total = 0
        self._cluster_total_systems = 0
        self._cluster_slice_key: tuple[str, ...] = ()
        self._cluster_last_refresh_at = 0.0

    def _default_progress(self) -> dict:
        return {
            "state_idx": 0,
            "dist_idx": 0,
            "complex_idx": 0,
            "est_idx": 0,
            "case_type_idx": 0,
            "year": DC_END_YEAR,
            "status_idx": 0,
        }

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
        return self._default_progress()

    def _save_progress(self, prog: dict) -> None:
        with open(DC_PROGRESS_FILE, "w") as f:
            json.dump(prog, f, indent=4)

    def _refresh_state_slice(self, *, force: bool = False) -> bool:
        now = time.monotonic()
        if (
            not force
            and self._cluster_last_refresh_at
            and now - self._cluster_last_refresh_at < CLUSTER_CONFIG_REFRESH_SECONDS
        ):
            return False

        self._cluster_last_refresh_at = now
        cfg = read_config_row_sync(self._sheets._index_sh, CONFIG_WORKSHEET_NAME)
        total = max(1, int(cfg.get("total_systems", 1)))
        states = slice_for_shard(list(DC_STATES), SYSTEM_SHARD_ID, total)
        new_key = tuple(str(item.get("state_code", "")) for item in states)
        had_assignment = self._cluster_total_systems > 0 or bool(self._cluster_slice_key)
        changed = total != self._cluster_total_systems or new_key != self._cluster_slice_key

        self._states_slice = states
        self._cluster_total_systems = total
        self._cluster_slice_key = new_key

        if changed:
            first = states[0]["name"] if states else "-"
            last = states[-1]["name"] if states else "-"
            logger.info(
                format_kv_block(
                    "[DC] Cluster assignment",
                    {
                        "Config": {
                            "sheet": CONFIG_WORKSHEET_NAME,
                            "raw_total_systems": cfg.get("raw_total_systems", "-"),
                            "parsed_total_systems": total,
                            "dc_write_lock": cfg.get("lock_dc", "-"),
                        },
                        "Worker": {
                            "id": WORKER_LABEL,
                            "shard_id": SYSTEM_SHARD_ID,
                            "refresh_seconds": CLUSTER_CONFIG_REFRESH_SECONDS,
                        },
                        "Assignment": {
                            "assigned_states": len(states),
                            "first": first,
                            "last": last,
                            "detail_workers": max(1, int(DC_DETAIL_WORKERS)),
                        },
                    },
                )
            )
        return changed and had_assignment

    def _align_progress_to_state_slice(self, prog: dict) -> dict:
        current_code = str(prog.get("state_code", "")).strip()
        index_by_code = {
            str(item.get("state_code", "")): idx
            for idx, item in enumerate(self._states_slice)
        }
        if current_code and current_code in index_by_code:
            old_idx = int(prog.get("state_idx", 0) or 0)
            new_idx = index_by_code[current_code]
            prog["state_idx"] = new_idx
            logger.info(
                "[DC] Cluster change kept current state: state_code=%s old_idx=%d new_idx=%d",
                current_code,
                old_idx,
                new_idx,
            )
            return prog

        reset = self._default_progress()
        logger.info(
            "[DC] Cluster change moved current state outside this worker slice; resetting local DC progress."
        )
        return reset

    async def run(self) -> None:
        logger.info("Starting DC Continuous 24/7 Scraper...")

        self._refresh_state_slice(force=True)

        while True:
            try:
                assignment_changed = self._refresh_state_slice()
                states = self._states_slice
                if not states:
                    logger.warning(
                        "[DC] No states assigned to shard_id=%d total_systems=%d. Waiting for config change.",
                        SYSTEM_SHARD_ID,
                        self._cluster_total_systems,
                    )
                    await asyncio.sleep(10)
                    continue

                prog = self._load_progress()
                if assignment_changed:
                    prog = self._align_progress_to_state_slice(prog)
                    self._save_progress(prog)

                legacy_pending = prog.pop("pending", [])
                if legacy_pending:
                    logger.info(
                        "[DC] Flushing %d legacy pending cases from progress file...",
                        len(legacy_pending),
                    )
                    await self._sheets.write_cases("dc", legacy_pending)
                    self._save_progress(prog)
                s_idx = prog.get("state_idx", 0)

                if s_idx >= len(states):
                    prog = self._default_progress()
                    self._save_progress(prog)
                    continue

                state = states[s_idx]
                state_code = state["state_code"]
                prog["state_code"] = state_code

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
                state_progress = stage_progress(s_idx, len(states))
                district_progress = stage_progress(d_idx, len(districts))
                complex_progress = stage_progress(c_idx, len(complexes))
                establishment_progress = stage_progress(e_idx, len(establishments))
                case_type_progress = stage_progress(ct_idx, len(case_types))
                year_progress = descending_year_progress(yr, DC_START_YEAR, DC_END_YEAR)
                status_progress = stage_progress(status_idx, len(STATUSES))
                target_label = dc_target_label(
                    state["name"],
                    dist["dist_name"],
                    complex_data["complex_name"],
                    est["est_name"],
                    case_type_name,
                    yr,
                    target_status,
                )

                logger.info(
                    format_main_progress(
                        court="DISTRICT COURT",
                        progress_name="state_progress",
                        current_name=state["name"],
                        current_code=state_code,
                        completed=s_idx,
                        total=len(states),
                        cases_collected=0,
                        written=self._session_written_total,
                        write_buffer=0,
                        write_batch_size=max(1, int(SHEET_FLUSH_CASES)),
                    )
                )

                search_started = time.monotonic()
                detail_limit = max(1, int(DC_DETAIL_WORKERS))
                telemetry_every = max(20, int(DC_TELEMETRY_EVERY))
                write_batch_size = max(1, int(SHEET_FLUSH_CASES))

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
                    search_label=target_label,
                )
                if search_state == "retryable_error":
                    logger.warning(
                        "[DC] Search unstable: worker=%s target={%s}; retrying same block.",
                        WORKER_LABEL,
                        target_label,
                    )
                    await asyncio.sleep(3)
                    continue

                async def build_row(case_data: dict) -> dict:
                    html = await self._extractor.fetch_case_detail(
                        state_code, dist_code, cplx_code, case_data
                    )
                    parsed = parse_detail_html(html) if html else None
                    row = build_dc_row(detail=parsed, fallback=case_data, court=court_info)
                    normalize_row(row)
                    return row

                async def write_rows(rows: list[dict[str, Any]]) -> int:
                    return await self._sheets.write_cases("dc", rows)

                def case_key(item: dict) -> str:
                    return f"{item.get('cino', '')}::{item.get('case_no', '')}"

                pipe_stats = await bounded_detail_pipeline(
                    items=cases,
                    build_row=build_row,
                    write_rows=write_rows,
                    detail_limit=detail_limit,
                    write_batch_size=write_batch_size,
                    telemetry_every=telemetry_every,
                    logger=logger,
                    log_prefix="DC",
                    target_label=target_label,
                    worker_label=WORKER_LABEL,
                    key_func=case_key,
                    session_written_base=self._session_written_total,
                    progress_court="DISTRICT COURT",
                    progress_name="state_progress",
                    progress_current_name=state["name"],
                    progress_current_code=state_code,
                    progress_completed=s_idx,
                    progress_total=len(states),
                )

                detail_success_total = pipe_stats.detail_success
                detail_failure_total = pipe_stats.detail_failure
                written_total = pipe_stats.written

                search_elapsed = time.monotonic() - search_started
                self._session_stage_total += 1
                self._session_detail_total += detail_success_total
                self._session_written_total += written_total
                logger.info(
                    "[DC] Stage done: target={%s} cases=%d details=%d/%d written=%d dups=%d dur=%s session_total=%d",
                    target_label,
                    count,
                    detail_success_total,
                    detail_success_total + detail_failure_total,
                    written_total,
                    pipe_stats.duplicates_skipped,
                    format_duration(search_elapsed),
                    self._session_written_total,
                )

                prog["status_idx"] += 1
                self._save_progress(prog)

            except Exception as e:
                logger.error("Unexpected error in DC block: %s", e, exc_info=True)
                await asyncio.sleep(10)
