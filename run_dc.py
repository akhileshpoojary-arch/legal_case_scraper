"""Entrypoint for the continuous District Court scraper."""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from daily_run.district_court.scraper import DCContinuousScraper
from daily_run.config import (
    CLUSTER_CONFIG_REFRESH_SECONDS,
    CONFIG_WORKSHEET_NAME,
    INDEX_SHEET_ID,
    SYSTEM_SHARD_ID,
    WORKER_LABEL,
)
from utils.logging_utils import format_kv_block, setup_logger

logger = setup_logger()


async def main():
    logger.info("Initializing 24/7 District Court Pipeline...")
    logger.info(
        format_kv_block(
            "[cluster] Startup",
            {
                "Worker": {"id": WORKER_LABEL, "shard_id": SYSTEM_SHARD_ID},
                "Config": {
                    "index_sheet_id": INDEX_SHEET_ID,
                    "worksheet": CONFIG_WORKSHEET_NAME,
                    "refresh_seconds": CLUSTER_CONFIG_REFRESH_SECONDS,
                },
            },
        )
    )
    loop = asyncio.get_running_loop()
    max_workers = max(2, int(os.environ.get("DEFAULT_EXECUTOR_WORKERS", "4")))
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
