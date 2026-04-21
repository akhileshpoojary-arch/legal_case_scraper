"""Logging setup and helpers for concise terminal output."""

from __future__ import annotations

import logging
import sys


def stage_progress(index: int, total: int) -> str:
    """Format zero-based stage progress as current/total with done/left counts."""
    total = max(0, int(total))
    if total == 0:
        return "0/0 done=0 left=0"
    done = min(max(0, int(index)), total)
    current = min(done + 1, total)
    left = max(total - done - 1, 0)
    return f"{current}/{total} done={done} left={left}"


def descending_year_progress(current_year: int, start_year: int, end_year: int) -> str:
    """Format descending year progress for a block that moves end_year -> start_year."""
    total = max(0, int(end_year) - int(start_year) + 1)
    if total == 0:
        return "0/0 done=0 left=0"
    done = min(max(int(end_year) - int(current_year), 0), total)
    current = min(done + 1, total)
    left = max(total - done - 1, 0)
    return f"{current}/{total} done={done} left={left}"


def setup_logger(
    name: str = "legal_scraper",
    level: int = logging.INFO,
) -> logging.Logger:
    """Configure and return a module-level logger with concise formatting."""
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = False

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    return logger
