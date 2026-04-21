"""
⚠️ DEPRECATED — NOT USED FOR PARTY SEARCH ANYMORE.
Party name searches now use Google Sheets via daily_run/sheet_search.py.
Kept for reference only. Use daily_run/district_court/ for the 24/7 pipeline.

District Court Scraper — 3-phase pipeline architecture.

Phase 1 (DISCOVER):  Walk state/district/complex/establishment hierarchy
Phase 2 (SEARCH):    Captcha + case list collection (NO details)
Phase 3 (DETAILS):   Fetch case detail HTML for the entire queue
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import config
from config import (
    COMMON_HEADERS,
    DC_LIMIT_CASES,
    DC_LIMIT_ESTABLISHMENTS,
    DC_PARALLEL_WORKERS,
    HTTP_CLIENT,
    MAX_CONCURRENT,
    MAX_PROXY_FAILURES,
    PROXY_BAN_DURATION,
    PROXY_FILE,
    REQUEST_DELAY,
    TESTING,
)
from scrapers.base import BaseScraper
from scrapers.district_court.extractor import (
    DC_HEADERS,
    DCExtractor,
)
from scrapers.district_court.parser import DCParser, parse_detail_html
from utils.proxy import ProxyRotator
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.district_court")


@dataclass
class _SearchTask:
    """One captcha-gated search unit."""

    state_code: str
    state_name: str
    dist_code: str
    dist_name: str
    complex_code: str
    complex_name: str
    est_code: str
    est_name: str
    year: int


@dataclass
class _CaseRef:
    """One case reference discovered in Phase 2, queued for Phase 3."""

    case_data: dict[str, Any]
    state_code: str
    dist_code: str
    complex_code: str
    court_info: dict[str, str]


class DistrictCourtScraper(BaseScraper):
    """Full pipeline for District Court case collection."""

    NAME = "district_court"
    SOURCE = "DISTRICT_COURT"

    def __init__(self) -> None:
        self._proxy_rotator = ProxyRotator(
            PROXY_FILE,
            max_failures=MAX_PROXY_FAILURES,
            ban_duration=PROXY_BAN_DURATION,
        )
        self._discovery_sm = SessionManager(
            client_type=HTTP_CLIENT,
            headers=DC_HEADERS,
            max_failures=10,
            semaphore_limit=MAX_CONCURRENT,
            request_delay=REQUEST_DELAY,
            proxy_rotator=self._proxy_rotator,
        )
        self._discovery_extractor = DCExtractor(self._discovery_sm)
        self._parser = DCParser()

    async def close(self) -> None:
        await self._discovery_sm.close()

    # ══════════════════════════════════════════════════════════════
    #  PHASE 1 — DISCOVER ALL ESTABLISHMENTS
    # ══════════════════════════════════════════════════════════════

    async def _discover_establishments(self) -> list[_SearchTask]:
        """Build flat list of all (establishment × year) search tasks."""
        extractor = self._discovery_extractor
        state_codes = extractor.states
        t0 = time.monotonic()
        state_counter = {"done": 0, "total": len(state_codes)}

        logger.info(
            "[DISCOVERY] Scanning %d states for establishments...",
            len(state_codes),
        )

        async def process_state(state: dict) -> list[_SearchTask]:
            state_code = state["state_code"]
            state_name = state["name"]
            tasks: list[_SearchTask] = []

            districts = await extractor.get_districts(state_code)
            if not districts:
                state_counter["done"] += 1
                logger.info(
                    "[DISCOVERY][%d/%d] %s → 0 districts",
                    state_counter["done"],
                    state_counter["total"],
                    state_name,
                )
                return []

            async def process_district(district: dict) -> list[_SearchTask]:
                dist_code = district["dist_code"]
                dist_name = district["dist_name"]
                dist_tasks: list[_SearchTask] = []

                complexes = await extractor.get_complexes(state_code, dist_code)
                for cplx in complexes:
                    cplx_code = cplx["complex_code"]
                    cplx_name = cplx["complex_name"]

                    establishments = await extractor.get_establishments(
                        state_code, dist_code, cplx_code
                    )
                    if not establishments:
                        establishments = [
                            {"est_code": "", "est_name": "All Establishments"}
                        ]

                    for est in establishments:
                        for year in range(config.DC_YEAR_FROM, config.DC_YEAR_TO + 1):
                            dist_tasks.append(
                                _SearchTask(
                                    state_code=state_code,
                                    state_name=state_name,
                                    dist_code=dist_code,
                                    dist_name=dist_name,
                                    complex_code=cplx_code,
                                    complex_name=cplx_name,
                                    est_code=est["est_code"],
                                    est_name=est["est_name"],
                                    year=year,
                                )
                            )

                logger.info(
                    "[DISCOVERY]   %s > %s → %d establishments",
                    state_name,
                    dist_name,
                    len(dist_tasks),
                )
                return dist_tasks

            dist_results = await asyncio.gather(
                *[process_district(d) for d in districts]
            )
            for r in dist_results:
                tasks.extend(r)

            state_counter["done"] += 1
            logger.info(
                "[DISCOVERY][%d/%d] %s → %d districts, %d tasks",
                state_counter["done"],
                state_counter["total"],
                state_name,
                len(districts),
                len(tasks),
            )
            return tasks

        state_results = await asyncio.gather(
            *[process_state(s) for s in state_codes]
        )

        all_tasks: list[_SearchTask] = []
        for r in state_results:
            all_tasks.extend(r)

        elapsed = time.monotonic() - t0

        # Apply test limit
        if DC_LIMIT_ESTABLISHMENTS is not None:
            original = len(all_tasks)
            all_tasks = all_tasks[:DC_LIMIT_ESTABLISHMENTS]
            logger.info(
                "[DISCOVERY] Limited to %d/%d establishments (DC_LIMIT_ESTABLISHMENTS=%d)",
                len(all_tasks),
                original,
                DC_LIMIT_ESTABLISHMENTS,
            )

        logger.info(
            "[DISCOVERY] Found %d establishments across %d states (%.1fs)",
            len(all_tasks),
            len(state_codes),
            elapsed,
        )

        for i, task in enumerate(all_tasks, 1):
            logger.info(
                "[DISCOVERY] E%d → %s > %s > %s [%d]",
                i,
                task.state_name,
                task.dist_name,
                task.est_name,
                task.year,
            )

        return all_tasks

    # ══════════════════════════════════════════════════════════════
    #  PHASE 2 — CAPTCHA + CASE LIST (NO DETAILS)
    # ══════════════════════════════════════════════════════════════

    async def _search_worker(
        self,
        worker_id: int,
        queue: asyncio.Queue[tuple[int, _SearchTask]],
        total: int,
        case_queue: list[_CaseRef],
        case_lock: asyncio.Lock,
        party_name: str,
    ) -> None:
        """Worker: solve captcha, collect case list, push to case_queue."""
        sm = SessionManager(
            client_type=HTTP_CLIENT,
            headers=DC_HEADERS,
            max_failures=10,
            semaphore_limit=5,
            request_delay=REQUEST_DELAY,
            proxy_rotator=self._proxy_rotator,
        )
        extractor = DCExtractor(sm)

        try:
            while True:
                try:
                    idx, task = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                court_label = f"{task.state_name} > {task.dist_name} > {task.est_name}"
                logger.info(
                    "========== [PHASE2][%d/%d] %s [%d] ==========",
                    idx,
                    total,
                    court_label,
                    task.year,
                )

                t0 = time.monotonic()
                cases, case_count = await extractor.search_cases(
                    task.state_code,
                    task.dist_code,
                    task.complex_code,
                    task.est_code,
                    task.year,
                    party_name,
                )
                elapsed = time.monotonic() - t0

                if not cases:
                    logger.info(
                        "[PHASE2][%d/%d] %s → 0 cases (%.1fs)",
                        idx,
                        total,
                        court_label,
                        elapsed,
                    )
                    queue.task_done()
                    continue

                court_info = {
                    "state_name": task.state_name,
                    "dist_name": task.dist_name,
                    "complex_name": task.complex_name,
                    "est_name": task.est_name,
                }

                refs = [
                    _CaseRef(
                        case_data=c,
                        state_code=task.state_code,
                        dist_code=task.dist_code,
                        complex_code=task.complex_code,
                        court_info=court_info,
                    )
                    for c in cases
                ]

                async with case_lock:
                    case_queue.extend(refs)

                logger.info(
                    "[PHASE2][%d/%d] %s → %d cases found (%.1fs)",
                    idx,
                    total,
                    court_label,
                    len(cases),
                    elapsed,
                )

                queue.task_done()
        finally:
            await sm.close()

    async def _collect_cases(
        self,
        tasks: list[_SearchTask],
        party_name: str,
    ) -> list[_CaseRef]:
        """Phase 2: solve captchas and collect case lists across all establishments."""
        total = len(tasks)
        t0 = time.monotonic()

        queue: asyncio.Queue[tuple[int, _SearchTask]] = asyncio.Queue()
        for i, task in enumerate(tasks, 1):
            queue.put_nowait((i, task))

        case_queue: list[_CaseRef] = []
        case_lock = asyncio.Lock()
        num_workers = min(DC_PARALLEL_WORKERS, total)

        logger.info(
            "[PHASE2] Starting %d workers for %d establishments...",
            num_workers,
            total,
        )

        workers = [
            asyncio.create_task(
                self._search_worker(
                    i, queue, total, case_queue, case_lock, party_name
                )
            )
            for i in range(num_workers)
        ]
        await asyncio.gather(*workers)

        elapsed = time.monotonic() - t0

        # Apply case limit
        if DC_LIMIT_CASES is not None:
            original = len(case_queue)
            case_queue = case_queue[:DC_LIMIT_CASES]
            logger.info(
                "[PHASE2] Limited to %d/%d cases (DC_LIMIT_CASES=%d)",
                len(case_queue),
                original,
                DC_LIMIT_CASES,
            )

        logger.info(
            "[PHASE2 SUMMARY] %d total cases from %d establishments (%.1fs)",
            len(case_queue),
            total,
            elapsed,
        )
        return case_queue

    # ══════════════════════════════════════════════════════════════
    #  PHASE 3 — FETCH DETAILS (CLEAN + FAST)
    # ══════════════════════════════════════════════════════════════

    async def _detail_worker(
        self,
        worker_id: int,
        queue: asyncio.Queue[tuple[int, _CaseRef]],
        total: int,
        results: list[dict[str, Any]],
        results_lock: asyncio.Lock,
        party_name: str,
    ) -> None:
        """Worker: fetch case detail HTML and parse rows."""
        sm = SessionManager(
            client_type=HTTP_CLIENT,
            headers=DC_HEADERS,
            max_failures=10,
            semaphore_limit=5,
            request_delay=REQUEST_DELAY,
            proxy_rotator=self._proxy_rotator,
        )
        extractor = DCExtractor(sm)

        try:
            while True:
                try:
                    idx, ref = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                case_no = ref.case_data.get("case_no", "unknown")

                detail_html = await extractor.fetch_case_detail(
                    state_code=ref.state_code,
                    dist_code=ref.dist_code,
                    complex_code=ref.complex_code,
                    case=ref.case_data,
                )

                parsed_detail = None
                if detail_html:
                    parsed_detail = parse_detail_html(detail_html)

                row = self._parser.build_row(
                    detail=parsed_detail,
                    fallback=ref.case_data,
                    party_name=party_name,
                    court=ref.court_info,
                )

                status = "SUCCESS" if detail_html else "FALLBACK"
                if idx % 100 == 0 or idx == total:
                    logger.info(
                        "    ↳ [%d/%d] details fetched Case: %s → %s",
                        idx,
                        total,
                        case_no,
                        status,
                    )

                async with results_lock:
                    results.append(row)

                queue.task_done()
        finally:
            await sm.close()

    async def _fetch_details(
        self,
        case_queue: list[_CaseRef],
        party_name: str,
    ) -> list[dict[str, Any]]:
        """Phase 3: fetch HTML detail for every case in the queue."""
        total = len(case_queue)
        t0 = time.monotonic()

        queue: asyncio.Queue[tuple[int, _CaseRef]] = asyncio.Queue()
        for i, ref in enumerate(case_queue, 1):
            queue.put_nowait((i, ref))

        results: list[dict[str, Any]] = []
        results_lock = asyncio.Lock()
        num_workers = min(DC_PARALLEL_WORKERS, total)

        logger.info(
            "[PHASE3] Starting %d workers for %d case details...",
            num_workers,
            total,
        )

        workers = [
            asyncio.create_task(
                self._detail_worker(
                    i, queue, total, results, results_lock, party_name
                )
            )
            for i in range(num_workers)
        ]
        await asyncio.gather(*workers)

        elapsed = time.monotonic() - t0
        logger.info(
            "[PHASE3 SUMMARY] %d details fetched (%.1fs)",
            len(results),
            elapsed,
        )
        return results

    # ══════════════════════════════════════════════════════════════
    #  RUN — 3-PHASE ORCHESTRATOR
    # ══════════════════════════════════════════════════════════════

    async def run(self, party_name: str) -> list[dict[str, Any]]:
        """Scrape District Courts using the 3-phase pipeline."""
        from utils.captcha import warm_up_reader

        warm_up_reader()
        run_start = time.monotonic()

        logger.info("[INIT] Party='%s'", party_name)
        logger.info(
            "[INIT] Years=%d–%d  Workers=%d",
            config.DC_YEAR_FROM,
            config.DC_YEAR_TO,
            DC_PARALLEL_WORKERS,
        )
        if DC_LIMIT_ESTABLISHMENTS is not None:
            logger.info("[INIT] DC_LIMIT_ESTABLISHMENTS=%d", DC_LIMIT_ESTABLISHMENTS)
        if DC_LIMIT_CASES is not None:
            logger.info("[INIT] DC_LIMIT_CASES=%d", DC_LIMIT_CASES)

        # Phase 1: Discover all establishments
        establishments = await self._discover_establishments()
        if not establishments:
            logger.info("[INIT] No establishments discovered. Exiting.")
            return []

        # Phase 2: Captcha + case list collection
        cases = await self._collect_cases(establishments, party_name)
        if not cases:
            logger.info("[PHASE2] No cases found across all establishments.")
            return []

        # Phase 3: Fetch all details
        results = await self._fetch_details(cases, party_name)

        total_elapsed = time.monotonic() - run_start
        logger.info(
            "═══ DC COMPLETE: %d cases collected in %.1fs ═══",
            len(results),
            total_elapsed,
        )
        return results
