"""Entrypoint for the continuous High Court scraper."""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from daily_run.high_court.scraper import HCContinuousScraper
from utils.logging_utils import setup_logger

logger = setup_logger()


async def main():
    logger.info("Initializing 24/7 High Court Pipeline...")
    loop = asyncio.get_running_loop()
    max_workers = max(4, int(os.environ.get("DEFAULT_EXECUTOR_WORKERS", "8")))
    loop.set_default_executor(
        ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="defaultio")
    )
    scraper = HCContinuousScraper()
    try:
        await scraper.run()
    except KeyboardInterrupt:
        logger.info("Shutting down High Court pipeline manually.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
