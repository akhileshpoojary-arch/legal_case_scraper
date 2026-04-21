"""Logging setup — structured, colored terminal output."""

from __future__ import annotations

import logging
import sys


def setup_logger(
    name: str = "legal_scraper",
    level: int = logging.INFO,
) -> logging.Logger:
    """Configure and return a module-level logger with rich formatting."""
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-7s  [%(name)s]  %(message)s",
        datefmt="%H:%M:%S",
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    return logger
