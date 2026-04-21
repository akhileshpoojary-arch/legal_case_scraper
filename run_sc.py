"""Entrypoint for the continuous Supreme Court scraper."""

import asyncio

from daily_run.supreme_court.scraper import SCContinuousScraper
from utils.logging_utils import setup_logger

logger = setup_logger()


async def main():
    logger.info("Initializing 24/7 Supreme Court Pipeline...")
    scraper = SCContinuousScraper()
    try:
        await scraper.run()
    except KeyboardInterrupt:
        logger.info("Shutting down Supreme Court pipeline manually.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
