"""Main entrypoint for the legal case scraper loop."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

warnings.filterwarnings("ignore", category=DeprecationWarning)

import pandas as pd

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import config
from config import (
    GOOGLE_SHEET_ID,
    INPUT_COMMAND_COL,
    SCRAPER_RESULT_CELLS,
    SheetStatus,
    TESTING,
)
from scrapers import get_active_scrapers
from sheets.google_sheets import SheetsClient
from utils.logging_utils import setup_logger
from utils.name_filter import filter_individual_matches
from utils.normalize import normalize_row, normalize_party_name

logger = setup_logger()


def _print_banner(party: list) -> None:
    print("\n" + "═" * 70)
    print("  LEGAL CASE EXTRACTOR")
    print("═" * 70)
    print(f"  Active Scrapers : {', '.join(config.ACTIVE_SCRAPERS)}")
    print(f"  Parties         : {party}")
    print(f"  Sheet ID        : {GOOGLE_SHEET_ID or '(not set)'}")
    print("═" * 70)


async def run_scrapers_for_party(
    party_name: str,
    scraper_classes: list,
    sheets: SheetsClient | None = None,
    row_num: int = -1,
) -> tuple[list[dict[str, Any]], list[tuple[type, datetime, datetime]]]:
    """Run all active scrapers for a single party concurrently, collect rows and times."""

    async def _run_one(scraper_cls: type) -> tuple[str, list[dict[str, Any]], type, datetime, datetime]:
        start_time = datetime.now()
        scraper = scraper_cls()
        source = scraper.SOURCE
        if sheets and row_num > 0:
            try:
                status_label = f"RUNNING"
                sheets.set_party_status(row_num, SheetStatus.RUNNING, label_override=status_label)
            except Exception as exc:
                logger.debug("Could not set custom RUNNING status: %s", exc)

        try:
            rows = await scraper.run(party_name)
            end_time = datetime.now()
            if rows:
                primary_count = sum(1 for r in rows if not r.get("_is_continuation"))
                logger.info("    ✅ %s: %d cases", source, primary_count)
            else:
                logger.info("    ⚠  %s: no data", source)
                rows = []
            return source, rows, scraper_cls, start_time, end_time
        except Exception as exc:
            logger.error("  ❌ %s failed for '%s': %s", source, party_name, exc)
            return source, [], scraper_cls, start_time, datetime.now()
        finally:
            if hasattr(scraper, "close"):
                await scraper.close()

    results = await asyncio.gather(
        *[_run_one(cls) for cls in scraper_classes],
        return_exceptions=True,
    )

    all_rows: list[dict[str, Any]] = []
    scraper_times: list[tuple[type, datetime, datetime]] = []
    for result in results:
        if isinstance(result, Exception):
            logger.error("Scraper task failed: %s", result)
            continue
        source, rows, cls, st, et = result
        all_rows.extend(rows)
        scraper_times.append((cls, st, et))

    return all_rows, scraper_times


def display_results(all_rows: list[dict]) -> None:
    """Pretty-print collected data in the terminal."""
    if not all_rows:
        print("\n  ⚠  No cases collected.")
        return

    df = pd.DataFrame(all_rows)

    print(f"\n{'─' * 70}")
    primary_count = sum(1 for r in all_rows if not r.get("_is_continuation"))
    print(f"  Total Cases : {primary_count}")

    if "courtType" in df.columns:
        print(f"\n  ── Cases per Court Type ──")
        ct_counts = df.groupby("courtType").size().reset_index(name="Cases")
        for _, row in ct_counts.iterrows():
            print(f"     {row['courtType']:<40} {row['Cases']:>5}")

    if "courtType" in df.columns and "benchName" in df.columns:
        nclt_df = df[df["courtType"] == "NCLT"]
        if not nclt_df.empty:
            print(f"\n  ── NCLT Bench Breakdown ──")
            bench_counts = nclt_df.groupby("benchName").size().reset_index(name="Cases")
            bench_counts = bench_counts.sort_values("Cases", ascending=False)
            for _, row in bench_counts.iterrows():
                print(f"     {row['benchName']:<35} {row['Cases']:>5}")
            print(f"     {'─' * 42}")
            print(f"     {'TOTAL':<35} {len(nclt_df):>5}")

        hc_df = df[df["courtType"].str.contains("HIGH_COURT", na=False)]
        if not hc_df.empty:
            print(f"\n  ── High Court Breakdown ──")
            hc_counts = hc_df.groupby("courtType").size().reset_index(name="Cases")
            hc_counts = hc_counts.sort_values("Cases", ascending=False)
            for _, row in hc_counts.iterrows():
                label = row["courtType"].replace("HIGH_COURT (", "").rstrip(")")
                print(f"     {label:<35} {row['Cases']:>5}")
            print(f"     {'─' * 42}")
            print(f"     {'TOTAL':<35} {len(hc_df):>5}")

    print(f"{'─' * 70}")


async def main() -> None:
    """Read parties → scrape DRT+DRAT → write CSV + Result Count. Runs infinitely."""

    if not get_active_scrapers():
        logger.warning("No active scrapers configured at start. Will check Config tab.")

    sheets: SheetsClient | None = None
    if GOOGLE_SHEET_ID:
        try:
            sheets = SheetsClient()
        except Exception as exc:
            logger.error("Failed to authenticate to Google Sheets: %s", exc)

    logger.info("Starting infinite Legal Case Scraper loop...")

    while True:
        try:
            start = datetime.now()
            parties: list[dict[str, str]] = []

            if sheets:
                try:
                    sheets.load_dynamic_config()
                    logger.info("Config loaded. Active scrapers: %s", config.ACTIVE_SCRAPERS)
                except Exception as exc:
                    logger.error("Failed to load dynamic config: %s", exc)

            scraper_classes = get_active_scrapers()
            if not scraper_classes:
                logger.info("No active scrapers in config. Sleeping for 1 min.")
                await asyncio.sleep(60)
                continue

            if sheets:
                try:
                    all_parties = sheets.read_parties()
                    for p in all_parties:
                        if p.get("status", "").strip().upper() == "FETCH":
                            parties.append(p)
                except Exception as exc:
                    logger.error("Failed to read from Google Sheets: %s", exc)

            if not parties:
                logger.info("No parties with 'FETCH' status in column %s. Sleeping 60s...", INPUT_COMMAND_COL)
                if sheets:
                    try:
                        sheets.set_party_status(1, SheetStatus.WAITING)
                    except Exception as exc:
                        logger.debug("Could not set waiting status: %s", exc)
                await asyncio.sleep(60)
                continue

            _print_banner([p["party"] for p in parties])

            if sheets:
                try:
                    clear_refs = list(SCRAPER_RESULT_CELLS.values())
                    clear_refs.extend(["C" + r[1:] for r in SCRAPER_RESULT_CELLS.values()])
                    clear_refs.extend(["D" + r[1:] for r in SCRAPER_RESULT_CELLS.values()])
                    clear_refs.extend(["E" + r[1:] for r in SCRAPER_RESULT_CELLS.values()])
                    sheets.clear_result_cells(clear_refs)
                except Exception as exc:
                    logger.debug("Could not clear result cells: %s", exc)

            all_rows: list[dict[str, Any]] = []

            for party_info in parties:
                party_name_raw = party_info["party"]
                party_name = normalize_party_name(party_name_raw)
                if not party_name:
                    party_name = party_name_raw
                    
                row_num = party_info["row"]

                print(f"\n{'━' * 70}")
                if party_name != party_name_raw:
                    print(f"  🔍  Party: {party_name} (normalized from: {party_name_raw})")
                else:
                    print(f"  🔍  Party: {party_name}")
                print(f"{'━' * 70}")

                if sheets:
                    try:
                        sheets.set_party_status(row_num, SheetStatus.RUNNING)
                    except Exception as exc:
                        logger.debug("Could not set RUNNING status: %s", exc)

                try:
                    party_rows, scraper_times = await run_scrapers_for_party(party_name, scraper_classes, sheets, row_num)

                    for row in party_rows:
                        normalize_row(row)

                    entity_type = getattr(config, "ENTITY_TYPE", "individual")
                    party_rows = filter_individual_matches(party_rows, party_name, entity_type)

                    all_rows.extend(party_rows)

                    primary_count = sum(1 for r in party_rows if not r.get("_is_continuation"))
                    logger.info("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                    logger.info("  ✅ Party Total: %d cases", primary_count)
                    logger.info("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

                    def _court_count(rows: list, *keywords: str) -> int:
                        return sum(
                            1 for r in rows
                            if any(kw in str(r.get("courtType", "")).upper() for kw in keywords)
                        )

                    scraper_counts = {
                        "drt": _court_count(party_rows, "DRT"),
                        "drat": _court_count(party_rows, "DRAT"),
                        "nclt": _court_count(party_rows, "NCLT"),
                        "ncdrc": _court_count(party_rows, "NCDRC"),
                        "scdrc": _court_count(party_rows, "SCDRC"),
                        "dcdrc": _court_count(party_rows, "DCDRC"),
                        "high_court": _court_count(party_rows, "HIGH_COURT", "HIGH COURT"),
                        "district_court": _court_count(party_rows, "DISTRICT_COURT", "DISTRICT COURT"),
                        "supreme_court": _court_count(party_rows, "SUPREME_COURT", "SUPREME COURT"),
                    }

                    cls_to_name = {cls: name for name, cls in zip(config.ACTIVE_SCRAPERS, scraper_classes)}
                    name_times = {cls_to_name.get(cls): (st, et) for cls, st, et in scraper_times if cls in cls_to_name}

                    cell_scraper_counts: dict[str, dict[str, int]] = {}
                    cell_scraper_times: dict[str, dict[str, tuple[datetime, datetime]]] = {}
                    for s_name in config.ACTIVE_SCRAPERS:
                        if s_name in SCRAPER_RESULT_CELLS:
                            cell = SCRAPER_RESULT_CELLS[s_name]
                            if cell not in cell_scraper_counts:
                                cell_scraper_counts[cell] = {}
                                cell_scraper_times[cell] = {}
                            cell_scraper_counts[cell][s_name] = scraper_counts.get(s_name, 0)
                            if s_name in name_times:
                                cell_scraper_times[cell][s_name] = name_times[s_name]

                    if sheets:
                        batch_cells: dict[str, str] = {}

                        def _fmt_duration(start: datetime, end: datetime) -> str:
                            """Format duration as human-readable string."""
                            secs = int((end - start).total_seconds())
                            if secs < 60:
                                return f"{secs}s"
                            mins, s = divmod(secs, 60)
                            return f"{mins}m {s}s"

                        for cell, counts_by_scraper in cell_scraper_counts.items():
                            total = sum(counts_by_scraper.values())
                            if total == 0:
                                result_str = "no"
                            elif len(counts_by_scraper) > 1:
                                parts = ", ".join(
                                    f"{name}: {cnt}" for name, cnt in counts_by_scraper.items()
                                )
                                result_str = f"yes({parts})"
                            else:
                                result_str = f"yes ({total})"

                            row_suffix = cell[1:]  # e.g. "8" from "B8"
                            batch_cells[cell] = result_str

                            times_for_cell = cell_scraper_times.get(cell, {})
                            if times_for_cell:
                                if len(times_for_cell) > 1:
                                    start_parts = ", ".join(
                                        f"{name}: {t[0].strftime('%I:%M:%S %p')}"
                                        for name, t in times_for_cell.items()
                                    )
                                    end_parts = ", ".join(
                                        f"{name}: {t[1].strftime('%I:%M:%S %p')}"
                                        for name, t in times_for_cell.items()
                                    )
                                    e_parts = ", ".join(
                                        f"{name}: {_fmt_duration(t[0], t[1])}"
                                        for name, t in times_for_cell.items()
                                    )
                                else:
                                    single_times = next(iter(times_for_cell.values()))
                                    start_parts = single_times[0].strftime("%I:%M:%S %p")
                                    end_parts = single_times[1].strftime("%I:%M:%S %p")
                                    e_parts = _fmt_duration(single_times[0], single_times[1])

                                batch_cells[f"C{row_suffix}"] = start_parts
                                batch_cells[f"D{row_suffix}"] = end_parts
                                batch_cells[f"E{row_suffix}"] = e_parts

                        try:
                            sheets.batch_write_result_cells(batch_cells)
                        except Exception as exc:
                            logger.error("Could not batch-write result cells: %s", exc)

                        try:
                            sheets.set_party_status(row_num, SheetStatus.WAITING)
                            sheets.set_party_command(row_num, "WAITING")
                        except Exception as exc:
                            logger.error("Could not set final status for %s: %s", party_name, exc)
                except Exception as party_exc:
                    logger.error("Failed processing party %s: %s", party_name, party_exc)
                    if sheets:
                        try:
                            sheets.set_party_status(row_num, SheetStatus.ERROR)
                            sheets.set_party_command(row_num, "WAITING")
                        except Exception as exc:
                            logger.error("Could not set error status: %s", exc)

            if sheets and all_rows and GOOGLE_SHEET_ID:
                try:
                    sheets.write_results(all_rows)
                except Exception as exc:
                    logger.error("Sheets write failed: %s", exc)

            display_results(all_rows)

            primary_total = sum(1 for r in all_rows if not r.get("_is_continuation"))
            elapsed = int((datetime.now() - start).total_seconds())
            print(f"\n{'═' * 70}")
            print(f"  ✅  LOOP CYCLE DONE")
            print(f"  Total cases      : {primary_total}")
            print(f"  Time             : {elapsed // 60}m {elapsed % 60}s")
            print(f"{'═' * 70}\n")

            await asyncio.sleep(10)
        except Exception as main_exc:
            logger.error("Error in main loop: %s", main_exc)
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
