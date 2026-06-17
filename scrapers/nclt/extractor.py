"""
NCLT Extractor — API communication with efiling.nclt.gov.in.

Two-phase: search all 15 benches × years × party roles × statuses,
then fetch full case detail (proceedings, parties, linked IAs) per filing_no.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass
import logging
from datetime import datetime
from typing import Any

import config
from config import COMMON_HEADERS
from scrapers.base import BaseExtractor
from utils.logging_utils import format_duration, format_percent
from utils.session_utils import RequestFailure, SessionManager

logger = logging.getLogger("legal_scraper.nclt.extractor")

NCLT_BASE = "https://efiling.nclt.gov.in"
NCLT_SEARCH_API = f"{NCLT_BASE}/caseHistoryoptional.drt"
NCLT_DETAIL_API = f"{NCLT_BASE}/caseHistoryalldetails.drt"
NCLT_HOME_URL = f"{NCLT_BASE}/casehistorybeforeloginmenutrue.drt"

NCLT_HEADERS = {
    **COMMON_HEADERS,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json",
    "Origin": NCLT_BASE,
    "Referer": f"{NCLT_BASE}/casehistorybeforeloginmenutrue.drt",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "X-Requested-With": "XMLHttpRequest",
}

NCLT_COOKIES = {}

NCLT_BENCHES = [
    {"id": 10, "name": "Principal Bench / New Delhi Bench", "bench": "New Delhi"},
    {"id": 9,  "name": "Mumbai Bench",      "bench": "Mumbai"},
    {"id": 13, "name": "Cuttack Bench",      "bench": "Cuttack"},
    {"id": 1,  "name": "Ahmedabad Bench",    "bench": "Ahmedabad"},
    {"id": 12, "name": "Amaravati Bench",    "bench": "Amaravati"},
    {"id": 4,  "name": "Chandigarh Bench",   "bench": "Chandigarh"},
    {"id": 8,  "name": "Kolkata Bench",      "bench": "Kolkata"},
    {"id": 11, "name": "Jaipur Bench",       "bench": "Jaipur"},
    {"id": 3,  "name": "Bengaluru Bench",    "bench": "Bengaluru"},
    {"id": 5,  "name": "Chennai Bench",      "bench": "Chennai"},
    {"id": 6,  "name": "Guwahati Bench",     "bench": "Guwahati"},
    {"id": 7,  "name": "Hyderabad Bench",    "bench": "Hyderabad"},
    {"id": 14, "name": "Kochi Bench",        "bench": "Kochi"},
    {"id": 15, "name": "Indore Bench",       "bench": "Indore"},
    {"id": 2,  "name": "Allahabad Bench",    "bench": "Allahabad"},
]

NCLT_PARTY_TYPES = [
    {"code": "P", "label": "Petitioner"},
    {"code": "R", "label": "Respondent"},
]

NCLT_STATUSES = [
    {"code": "P", "label": "Pending"},
    {"code": "D", "label": "Disposed"},
]


@dataclass(frozen=True)
class NCLTSearchOutcome:
    task: dict[str, Any]
    cases: list[dict]
    ok: bool
    reason: str = ""


@dataclass(frozen=True)
class NCLTDetailOutcome:
    raw_case: dict
    task_ctx: dict
    detail: dict | None
    ok: bool
    reason: str = ""



class NCLTExtractor(BaseExtractor):
    """Async extractor for NCLT efiling cases — search + detail two-phase."""

    SOURCE = "NCLT"
    HEADERS = NCLT_HEADERS
    COOKIES = NCLT_COOKIES

    def __init__(self, session_manager: SessionManager) -> None:
        self._sm = session_manager

    @property
    def courts(self) -> list[dict[str, Any]]:
        return NCLT_BENCHES

    async def ensure_site_available(self) -> bool:
        """Warm up the NCLT session and fail fast when the endpoint is unreachable."""
        result = await self._sm.get_text(
            NCLT_HOME_URL,
            timeout=config.NCLT_HEALTHCHECK_TIMEOUT,
            label="NCLT home",
            return_failure=True,
        )
        if isinstance(result, RequestFailure):
            logger.error(
                "NCLT unavailable during health check: reason=%s detail=%s",
                result.reason,
                result.message or "-",
            )
            return False
        logger.info("NCLT session ready: home page reachable")
        return True

    # ── Phase 1: Search ───────────────────────────────────────────

    def _effective_years(self) -> list[int]:
        configured_from = int(config.NCLT_YEAR_FROM)
        configured_to = int(config.NCLT_YEAR_TO)
        if configured_from > configured_to:
            configured_from, configured_to = configured_to, configured_from

        year_from = max(config.NCLT_FIRST_YEAR, configured_from)
        year_to = min(configured_to, datetime.today().year)

        if configured_from < config.NCLT_FIRST_YEAR:
            logger.info(
                "NCLT: clamped start year from %d to %d because NCLT data starts in %d",
                configured_from,
                year_from,
                config.NCLT_FIRST_YEAR,
            )
        if year_to < year_from:
            return []
        return list(range(year_to, year_from - 1, -1))

    def build_tasks(self, party_name: str) -> list[dict[str, Any]]:
        """Cartesian product: benches × years × party_types × statuses."""
        tasks = []
        for year in self._effective_years():
            for bench in NCLT_BENCHES:
                for pt in NCLT_PARTY_TYPES:
                    for st in NCLT_STATUSES:
                        tasks.append({
                            "company": party_name,
                            "bench_id": bench["id"],
                            "bench_name": bench["name"],
                            "bench_short": bench["bench"],
                            "year": year,
                            "pt_code": pt["code"],
                            "pt_label": pt["label"],
                            "st_code": st["code"],
                            "st_label": st["label"],
                        })
        return tasks

    async def fetch_search_task(
        self, task: dict[str, Any],
    ) -> NCLTSearchOutcome:
        """Execute one search task against the NCLT party-name API."""
        payload = {
            "wayofselection": "partyname",
            "i_bench_id": "0",
            "filing_no": "",
            "i_bench_id_case_no": "0",
            "i_case_type_caseno": "0",
            "i_case_year_caseno": "0",
            "case_no": "",
            "i_party_search": "W",
            "i_bench_id_party": str(task["bench_id"]),
            "party_type_party": task["pt_code"],
            "party_name_party": task["company"],
            "i_case_year_party": str(task["year"]),
            "status_party": task["st_code"],
            "i_adv_search": "E",
            "i_bench_id_lawyer": "0",
            "party_lawer_name": "",
            "i_case_year_lawyer": "0",
            "bar_council_advocate": "",
        }

        result = await self._sm.post(
            NCLT_SEARCH_API,
            json_data=payload,
            timeout=config.NCLT_SEARCH_TIMEOUT,
            label=f"NCLT {task['bench_name']} {task['year']} {task['pt_label']}/{task['st_label']}",
            return_failure=True,
        )

        cases: list[dict] = []
        if isinstance(result, RequestFailure):
            return NCLTSearchOutcome(task=task, cases=[], ok=False, reason=result.reason)

        if isinstance(result, dict):
            if result.get("errormsg"):
                return NCLTSearchOutcome(task=task, cases=[], ok=True)
            cases = result.get("mainpanellist") or []
        elif isinstance(result, list):
            cases = result

        return NCLTSearchOutcome(task=task, cases=cases, ok=True)

    # ── Phase 2: Detail ───────────────────────────────────────────

    async def fetch_case_detail(self, filing_no: str) -> tuple[dict | None, str]:
        """GET case detail including proceedings, parties, and linked IAs."""
        url = f"{NCLT_DETAIL_API}?filing_no={filing_no}&flagIA=false"

        result = await self._sm.get(
            url,
            timeout=config.NCLT_DETAIL_TIMEOUT,
            label=f"NCLT detail fno={filing_no}",
            return_failure=True,
        )

        if isinstance(result, RequestFailure):
            return None, result.reason
        if isinstance(result, dict):
            return result, ""
        return None, "empty_response"

    # ── Combined runner ───────────────────────────────────────────

    async def run_all_tasks(
        self, party_name: str,
    ) -> list[tuple[dict, dict, dict | None]]:
        """
        Search all tasks, dedup by filing_no, then fetch detail for each unique case.

        Returns: [(search_case, task_ctx, detail_dict_or_None), ...]
        """
        if not await self.ensure_site_available():
            raise RuntimeError("NCLT endpoint unavailable during health check")

        tasks = self.build_tasks(party_name)
        total = len(tasks)
        if total == 0:
            logger.warning(
                "NCLT: no valid years to search after clamping configured range %d-%d",
                config.NCLT_YEAR_FROM,
                config.NCLT_YEAR_TO,
            )
            return []

        search_workers = max(1, min(config.NCLT_SEARCH_CONCURRENCY, total))
        logger.info(
            "NCLT search plan: party=%s benches=%d years=%d-%d tasks=%d workers=%d timeout=%ss",
            party_name,
            len(NCLT_BENCHES),
            min(t["year"] for t in tasks),
            max(t["year"] for t in tasks),
            total,
            search_workers,
            int(config.NCLT_SEARCH_TIMEOUT),
        )

        # Phase 1: bounded search workers with readable progress.
        queue: asyncio.Queue[tuple[int, dict[str, Any]]] = asyncio.Queue()
        for idx, task in enumerate(tasks):
            queue.put_nowait((idx, task))

        seen: set[str] = set()
        unique_pairs: list[tuple[dict, dict]] = []  # (raw_case, task_ctx)
        failure_counts: Counter[str] = Counter()
        done = 0
        search_started = asyncio.get_running_loop().time()
        lock = asyncio.Lock()

        async def _search_worker() -> None:
            nonlocal done
            while True:
                try:
                    _, task = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    outcome = await self.fetch_search_task(task)
                except Exception as exc:
                    outcome = NCLTSearchOutcome(
                        task=task,
                        cases=[],
                        ok=False,
                        reason=exc.__class__.__name__,
                    )

                async with lock:
                    done += 1
                    if not outcome.ok:
                        failure_counts[outcome.reason or "unknown_error"] += 1
                    for c in outcome.cases:
                        fno = c.get("filing_no", "")
                        if not fno or fno in seen:
                            continue
                        seen.add(fno)
                        unique_pairs.append((c, outcome.task))

                    if outcome.cases:
                        logger.info(
                            "NCLT hit: bench=%s year=%d role=%s status=%s cases=%d unique=%d",
                            outcome.task["bench_short"],
                            outcome.task["year"],
                            outcome.task["pt_label"],
                            outcome.task["st_label"],
                            len(outcome.cases),
                            len(unique_pairs),
                        )
                    if done % 50 == 0 or done == total:
                        elapsed = asyncio.get_running_loop().time() - search_started
                        fail_total = sum(failure_counts.values())
                        logger.info(
                            "NCLT search progress: %d/%d (%s) unique=%d failures=%d elapsed=%s",
                            done,
                            total,
                            format_percent(done, total),
                            len(unique_pairs),
                            fail_total,
                            format_duration(elapsed),
                        )
                queue.task_done()

        await asyncio.gather(*(_search_worker() for _ in range(search_workers)))

        if failure_counts:
            summary = ", ".join(
                f"{reason}={count}" for reason, count in failure_counts.most_common()
            )
            logger.warning("NCLT search failure summary: %s", summary)

        if failure_counts and sum(failure_counts.values()) == total:
            summary = ", ".join(
                f"{reason}={count}" for reason, count in failure_counts.most_common()
            )
            raise RuntimeError(
                "NCLT search failed for every task; "
                f"failure_summary={summary}. Check NCLT availability, network, or proxy settings."
            )

        logger.info("NCLT search complete: unique_cases=%d", len(unique_pairs))

        # Phase 2: fetch detail for each unique filing_no.
        detail_total = len(unique_pairs)
        if detail_total == 0:
            return []

        detail_workers = max(1, min(config.NCLT_DETAIL_CONCURRENCY, detail_total))
        logger.info(
            "NCLT detail plan: cases=%d workers=%d timeout=%ss",
            detail_total,
            detail_workers,
            int(config.NCLT_DETAIL_TIMEOUT),
        )

        detail_queue: asyncio.Queue[tuple[int, tuple[dict, dict]]] = asyncio.Queue()
        for idx, pair in enumerate(unique_pairs):
            detail_queue.put_nowait((idx, pair))

        results: list[NCLTDetailOutcome] = []
        detail_failures: Counter[str] = Counter()
        detail_done = 0
        detail_started = asyncio.get_running_loop().time()
        detail_lock = asyncio.Lock()

        async def _detail_worker() -> None:
            nonlocal detail_done
            while True:
                try:
                    _, pair = detail_queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                raw_case, task_ctx = pair
                fno = raw_case.get("filing_no", "")
                try:
                    detail, reason = await self.fetch_case_detail(fno)
                except Exception as exc:
                    detail, reason = None, exc.__class__.__name__

                async with detail_lock:
                    detail_done += 1
                    ok = detail is not None
                    if not ok:
                        detail_failures[reason or "unknown_error"] += 1
                    results.append(
                        NCLTDetailOutcome(
                            raw_case=raw_case,
                            task_ctx=task_ctx,
                            detail=detail,
                            ok=ok,
                            reason=reason,
                        )
                    )
                    if detail_done % 50 == 0 or detail_done == detail_total:
                        elapsed = asyncio.get_running_loop().time() - detail_started
                        logger.info(
                            "NCLT detail progress: %d/%d (%s) failed=%d elapsed=%s",
                            detail_done,
                            detail_total,
                            format_percent(detail_done, detail_total),
                            sum(detail_failures.values()),
                            format_duration(elapsed),
                        )
                detail_queue.task_done()

        await asyncio.gather(*(_detail_worker() for _ in range(detail_workers)))

        if detail_failures:
            summary = ", ".join(
                f"{reason}={count}" for reason, count in detail_failures.most_common()
            )
            logger.warning("NCLT detail failure summary: %s", summary)

        return [(r.raw_case, r.task_ctx, r.detail) for r in results]

    # BaseExtractor interface stubs — NCLT uses its own two-phase flow
    async def search(self, court: dict[str, Any], party_name: str) -> list[dict]:
        return []

    async def fetch_detail(
        self, court: dict[str, Any], search_result: dict[str, Any],
    ) -> dict | None:
        return None
