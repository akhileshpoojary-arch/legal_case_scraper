"""Entrypoint for the continuous District Court scraper."""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from daily_run.district_court.scraper import DCContinuousScraper
from utils.logging_utils import setup_logger

logger = setup_logger()


async def main():
    logger.info("Initializing 24/7 District Court Pipeline...")
    loop = asyncio.get_running_loop()
    max_workers = max(4, int(os.environ.get("DEFAULT_EXECUTOR_WORKERS", "8")))
    loop.set_default_executor(
        ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="defaultio")
    )
    scraper = DCContinuousScraper()
    try:
        await scraper.run()
    except KeyboardInterrupt:
        logger.info("Shutting down District Court pipeline manually.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
