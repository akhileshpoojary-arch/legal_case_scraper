"""
NCLT Extractor — API communication with efiling.nclt.gov.in.

Two-phase: search all 15 benches × years × party roles × statuses,
then fetch full case detail (proceedings, parties, linked IAs) per filing_no.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

import config
from config import COMMON_HEADERS
from scrapers.base import BaseExtractor
from utils.session_utils import SessionManager

logger = logging.getLogger("legal_scraper.nclt.extractor")

NCLT_BASE = "https://efiling.nclt.gov.in"
NCLT_SEARCH_API = f"{NCLT_BASE}/caseHistoryoptional.drt"
NCLT_DETAIL_API = f"{NCLT_BASE}/caseHistoryalldetails.drt"

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

NCLT_COOKIES = {
    "SSESSe30be58c6a47b99f1b61880b1a109341": "XCEMPMl_PYKltqF49crfWGBIscZHEoiOw5KqAq-W_0Q",
    "JSESSIONID": "273A34420E0F7BBE9170DDA033F9F820",
    "SERVERID": "efiling-249-sch",
}

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

    # ── Phase 1: Search ───────────────────────────────────────────

    def build_tasks(self, party_name: str) -> list[dict[str, Any]]:
        """Cartesian product: benches × years × party_types × statuses."""
        tasks = []
        for bench in NCLT_BENCHES:
            for year in range(config.NCLT_YEAR_FROM, config.NCLT_YEAR_TO + 1):
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
    ) -> tuple[dict, list[dict]]:
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
            timeout=120.0,
            label=f"NCLT {task['bench_name']} {task['year']} {task['pt_label']}/{task['st_label']}",
        )

        cases: list[dict] = []
        if isinstance(result, dict):
            if result.get("errormsg"):
                return task, []
            cases = result.get("mainpanellist") or []

        return task, cases

    # ── Phase 2: Detail ───────────────────────────────────────────

    async def fetch_case_detail(self, filing_no: str) -> dict | None:
        """GET case detail including proceedings, parties, and linked IAs."""
        url = f"{NCLT_DETAIL_API}?filing_no={filing_no}&flagIA=false"

        result = await self._sm.get(
            url,
            timeout=120.0,
            label=f"NCLT detail fno={filing_no}",
        )

        if isinstance(result, dict):
            return result
        return None

    # ── Combined runner ───────────────────────────────────────────

    async def run_all_tasks(
        self, party_name: str,
    ) -> list[tuple[dict, dict, dict | None]]:
        """
        Search all tasks, dedup by filing_no, then fetch detail for each unique case.

        Returns: [(search_case, task_ctx, detail_dict_or_None), ...]
        """
        tasks = self.build_tasks(party_name)
        total = len(tasks)
        logger.info(
            "NCLT: %d search tasks for '%s' (benches=%d, years=%d–%d)",
            total, party_name, len(NCLT_BENCHES), config.NCLT_YEAR_FROM, config.NCLT_YEAR_TO,
        )

        # Phase 1: parallel search
        coros = [self.fetch_search_task(t) for t in tasks]
        search_results = await asyncio.gather(*coros, return_exceptions=True)

        # Dedup: keep first occurrence per filing_no
        seen: set[str] = set()
        unique_pairs: list[tuple[dict, dict]] = []  # (raw_case, task_ctx)

        done = 0
        for result in search_results:
            done += 1
            if isinstance(result, Exception):
                logger.error("NCLT search task failed: %s", result)
                continue

            task_ctx, cases = result
            for c in cases:
                fno = c.get("filing_no", "")
                if not fno or fno in seen:
                    continue
                seen.add(fno)
                unique_pairs.append((c, task_ctx))

            if cases and len(cases) > 0:
                logger.debug(
                    "  [%4d/%d] %s %d %s/%s → %d cases (unique total: %d)",
                    done, total,
                    task_ctx["bench_name"][:20], task_ctx["year"],
                    task_ctx["pt_label"], task_ctx["st_label"],
                    len(cases), len(unique_pairs),
                )

        logger.info("NCLT: %d unique cases found, fetching details...", len(unique_pairs))

        # Phase 2: fetch detail for each unique filing_no
        results: list[tuple[dict, dict, dict | None]] = []

        async def _fetch_detail_for(idx: int, pair: tuple[dict, dict]) -> None:
            raw_case, task_ctx = pair
            fno = raw_case.get("filing_no", "")
            detail = await self.fetch_case_detail(fno)
            results.append((raw_case, task_ctx, detail))

            if (idx + 1) % 50 == 0 or (idx + 1) == len(unique_pairs):
                logger.info(
                    "  Detail progress: %d/%d fetched", idx + 1, len(unique_pairs),
                )

        detail_coros = [
            _fetch_detail_for(i, pair) for i, pair in enumerate(unique_pairs)
        ]
        await asyncio.gather(*detail_coros, return_exceptions=True)

        return results

    # BaseExtractor interface stubs — NCLT uses its own two-phase flow
    async def search(self, court: dict[str, Any], party_name: str) -> list[dict]:
        return []

    async def fetch_detail(
        self, court: dict[str, Any], search_result: dict[str, Any],
    ) -> dict | None:
        return None
