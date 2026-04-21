"""Entrypoint for the continuous District Court scraper."""

import asyncio

from daily_run.district_court.scraper import DCContinuousScraper
from utils.logging_utils import setup_logger

logger = setup_logger()


async def main():
    logger.info("Initializing 24/7 District Court Pipeline...")
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
