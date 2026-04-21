"""Entrypoint for the continuous High Court scraper."""

import asyncio

from daily_run.high_court.scraper import HCContinuousScraper
from utils.logging_utils import setup_logger

logger = setup_logger()


async def main():
    logger.info("Initializing 24/7 High Court Pipeline...")
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
