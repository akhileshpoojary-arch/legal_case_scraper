"""Logging setup and helpers for concise terminal output."""

from __future__ import annotations

import logging
import sys


def format_log_value(value: object) -> str:
    """Render a stable log value that stays readable when it contains spaces."""
    if value is None:
        return "-"
    text = " ".join(str(value).strip().split())
    if not text:
        return "-"
    if any(ch.isspace() for ch in text) or "=" in text or '"' in text:
        return f'"{text.replace(chr(34), chr(39))}"'
    return text


def format_log_fields(**fields: object) -> str:
    """Render key=value pairs with quoting only when needed."""
    return " ".join(
        f"{key}={format_log_value(value)}" for key, value in fields.items()
    )


def dc_target_label(
    state_name: str,
    district_name: str,
    complex_name: str,
    establishment_name: str,
    case_type_name: str,
    year: int,
    status: str,
) -> str:
    return format_log_fields(
        state=state_name,
        district=district_name,
        complex=complex_name,
        establishment=establishment_name,
        case_type=case_type_name,
        year=year,
        status=status,
    )


def hc_target_label(
    court_name: str,
    bench_name: str,
    case_type_name: str,
    year: int,
    status: str,
) -> str:
    return format_log_fields(
        court=court_name,
        bench=bench_name,
        case_type=case_type_name,
        year=year,
        status=status,
    )


def sc_target_label(case_type_name: str, type_code: str, year: int) -> str:
    return format_log_fields(
        case_type=case_type_name,
        type_code=type_code,
        year=year,
    )


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
