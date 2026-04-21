"""
Single entrypoint that runs all three court scrapers concurrently.

Used by Railway.app Procfile / Dockerfile as the main process.
All three scrapers share the same event loop but operate on different
court websites with independent HTTP sessions.
"""

import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor

from daily_run.district_court.scraper import DCContinuousScraper
from daily_run.high_court.scraper import HCContinuousScraper
from daily_run.supreme_court.scraper import SCContinuousScraper
from utils.logging_utils import setup_logger

logger = setup_logger()


async def main() -> None:
    logger.info("=" * 60)
    logger.info("  LEGAL CASE SCRAPER — ALL COURTS (Railway)")
    logger.info("=" * 60)
    loop = asyncio.get_running_loop()
    max_workers = max(4, int(os.environ.get("DEFAULT_EXECUTOR_WORKERS", "8")))
    loop.set_default_executor(
        ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="defaultio")
    )
    logger.info("Configured default executor workers=%d", max_workers)

    dc = DCContinuousScraper()
    hc = HCContinuousScraper()
    sc = SCContinuousScraper()

    try:
        await asyncio.gather(
            dc.run(),
            hc.run(),
            sc.run(),
        )
    except KeyboardInterrupt:
        logger.info("Shutting down all pipelines manually.")
    except Exception as e:
        logger.error(f"Fatal error in combined runner: {e}", exc_info=True)
    finally:
        await dc.close()
        await hc.close()
        await sc.close()
        logger.info("All scrapers shut down.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
