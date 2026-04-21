"""
Scraper registry — maps scraper names to classes, filtered by ACTIVE_SCRAPERS.

Adding a new website:
  1. Create scrapers/new_site/__init__.py with a BaseScraper subclass
  2. Import and add it to _REGISTRY below
  3. Add "new_site" to ACTIVE_SCRAPERS in config.py
"""

from __future__ import annotations

import logging

import config
from scrapers.base import BaseScraper
from scrapers.drat import DRATScraper
from scrapers.drt import DRTScraper
from scrapers.ncdrc import NCDRCScraper
from scrapers.scdrc import SCDRCScraper
from scrapers.dcdrc import DCDRCScraper
from scrapers.nclt import NCLTScraper

logger = logging.getLogger("legal_scraper.registry")

# Lazy-loaded to avoid circular import with daily_run.sheet_search → scrapers.base
_SHEET_SCRAPERS: dict[str, type[BaseScraper]] | None = None


def _get_registry() -> dict[str, type[BaseScraper]]:
    """Build registry with lazy imports for sheet search scrapers."""
    global _SHEET_SCRAPERS
    if _SHEET_SCRAPERS is None:
        from daily_run.sheet_search import (
            DCSheetScraper,
            HCSheetScraper,
            SCSheetScraper,
        )
        _SHEET_SCRAPERS = {
            "district_court": DCSheetScraper,
            "high_court": HCSheetScraper,
            "supreme_court": SCSheetScraper,
        }

    return {
        "drt": DRTScraper,
        "drat": DRATScraper,
        "nclt": NCLTScraper,
        "ncdrc": NCDRCScraper,
        "scdrc": SCDRCScraper,
        "dcdrc": DCDRCScraper,
        **_SHEET_SCRAPERS,
    }


def get_active_scrapers() -> list[type[BaseScraper]]:
    """Return scraper classes that are enabled in ACTIVE_SCRAPERS config."""
    registry = _get_registry()
    active = []
    for name in config.ACTIVE_SCRAPERS:
        if name in registry:
            active.append(registry[name])
        else:
            logger.warning("Unknown scraper in ACTIVE_SCRAPERS: %r", name)
    return active


def get_all_scraper_names() -> list[str]:
    """Return all registered scraper names."""
    return list(_get_registry().keys())


__all__ = ["get_active_scrapers", "get_all_scraper_names"]

