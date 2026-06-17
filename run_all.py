"""
Single entrypoint that runs all three court scrapers concurrently — the 24/7
collector. This is the long-running "console" process.

All three scrapers share the same event loop but operate on different court
websites with independent HTTP sessions.

Handles SIGTERM/SIGINT for graceful shutdown (e.g. `docker stop`).
"""

import asyncio
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor

from daily_run.district_court.scraper import DCContinuousScraper
from daily_run.high_court.scraper import HCContinuousScraper
from daily_run.supreme_court.scraper import SCContinuousScraper
from daily_run.config import (
    CLUSTER_CONFIG_REFRESH_SECONDS,
    CONFIG_WORKSHEET_NAME,
    DC_END_YEAR,
    DC_START_YEAR,
    HC_END_YEAR,
    HC_START_YEAR,
    INDEX_SHEET_ID,
    SC_EMPTY_JUMP_ENABLED,
    SC_END_YEAR,
    SC_START_YEAR,
    SHEET_FLUSH_CASES,
    SYSTEM_SHARD_ID,
    WRITE_BATCH_SIZE,
    WORKER_LABEL,
)
from utils.logging_utils import format_kv_block, setup_logger

logger = setup_logger()

_shutdown_event: asyncio.Event | None = None


def _handle_signal(sig: int, _frame: object) -> None:
    """SIGTERM/SIGINT handler — signals graceful shutdown."""
    logger.info("Received signal %d, initiating graceful shutdown...", sig)
    if _shutdown_event:
        _shutdown_event.set()


async def _run_scraper(name: str, coro: object) -> None:
    """Run a single scraper with isolated error handling."""
    try:
        await coro
    except asyncio.CancelledError:
        logger.info("%s scraper cancelled.", name)
    except Exception as e:
        logger.error("%s scraper crashed: %s", name, e, exc_info=True)


async def main() -> None:
    global _shutdown_event
    _shutdown_event = asyncio.Event()

    logger.info("=" * 60)
    logger.info("  LEGAL CASE SCRAPER — ALL COURTS (24/7 collector)")
    logger.info("=" * 60)
    logger.info(
        format_kv_block(
            "[cluster] Startup",
            {
                "Worker": {
                    "id": WORKER_LABEL,
                    "shard_id": SYSTEM_SHARD_ID,
                },
                "Config": {
                    "index_sheet_id": INDEX_SHEET_ID,
                    "worksheet": CONFIG_WORKSHEET_NAME,
                    "refresh_seconds": CLUSTER_CONFIG_REFRESH_SECONDS,
                    "sheet_flush_cases": SHEET_FLUSH_CASES,
                    "write_batch_size": WRITE_BATCH_SIZE,
                    "sc_empty_jump": SC_EMPTY_JUMP_ENABLED,
                },
                "Year ranges": {
                    "district_court": f"{DC_START_YEAR}-{DC_END_YEAR}",
                    "high_court": f"{HC_START_YEAR}-{HC_END_YEAR}",
                    "supreme_court": f"{SC_START_YEAR}-{SC_END_YEAR}",
                },
            },
        )
    )

    loop = asyncio.get_running_loop()
    max_workers = max(2, int(os.environ.get("DEFAULT_EXECUTOR_WORKERS", "4")))
    loop.set_default_executor(
        ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="defaultio")
    )
    logger.info("Configured default executor workers=%d", max_workers)

    # Register signal handlers for graceful shutdown (docker stop / Ctrl-C)
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda s=sig: _shutdown_event.set())
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            signal.signal(sig, _handle_signal)

    # Warm up captcha models once (shared across all scrapers)
    from utils.captcha import warm_up_reader
    warm_up_reader()

    # One-time migration: if the local index is empty but data already exists in
    # Google Sheets, import it once so search works immediately and the collector
    # de-duplicates against it (no re-appending old cases). Idempotent; skipped on
    # later starts once the index has rows. Manual equivalent:
    #   python -m daily_run.case_index backfill
    from daily_run.case_index import CaseIndex, backfill
    index = CaseIndex.get_or_create()
    if index.count() == 0:
        logger.info("Case index is empty — running one-time backfill from Sheets...")
        try:
            await loop.run_in_executor(None, backfill)
            logger.info("Backfill complete. Indexed %d cases.", index.count())
        except Exception as exc:
            logger.error(
                "Backfill failed (%s). Continuing; run "
                "`python -m daily_run.case_index backfill` manually to retry.",
                exc,
            )

    dc = DCContinuousScraper()
    hc = HCContinuousScraper()
    sc = SCContinuousScraper()

    scraper_tasks: set[asyncio.Task[None]] = set()
    shutdown_task: asyncio.Task[bool] | None = None
    try:
        # Each scraper runs independently. A crashed scraper is reported and
        # removed; SIGTERM/SIGINT cancels the remaining long-running tasks.
        scraper_tasks = {
            asyncio.create_task(_run_scraper("DC", dc.run()), name="DC"),
            asyncio.create_task(_run_scraper("HC", hc.run()), name="HC"),
            asyncio.create_task(_run_scraper("SC", sc.run()), name="SC"),
        }
        shutdown_task = asyncio.create_task(_shutdown_event.wait(), name="shutdown")

        while scraper_tasks:
            done, _pending = await asyncio.wait(
                [shutdown_task, *scraper_tasks],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if shutdown_task in done:
                logger.info("Shutdown requested; cancelling scraper tasks...")
                break
            for task in done:
                scraper_tasks.discard(task)

        for task in scraper_tasks:
            task.cancel()
        if scraper_tasks:
            await asyncio.gather(*scraper_tasks, return_exceptions=True)
    finally:
        if shutdown_task:
            shutdown_task.cancel()
        await dc.close()
        await hc.close()
        await sc.close()
        logger.info("All scrapers shut down.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
