"""Logging setup and helpers for concise terminal output."""

from __future__ import annotations

import logging
import sys
from collections.abc import Mapping

SECTION_WIDTH = 72


def format_percent(done: int | float, total: int | float) -> str:
    """Return a compact percentage string for progress logs."""
    try:
        total_f = float(total)
        if total_f <= 0:
            return "0.0%"
        pct = max(0.0, min(100.0, (float(done) / total_f) * 100.0))
        return f"{pct:.1f}%"
    except Exception:
        return "0.0%"


def format_duration(seconds: int | float) -> str:
    """Return a compact duration for logs."""
    try:
        total = max(0, int(float(seconds)))
    except Exception:
        total = 0
    if total < 60:
        return f"{total}s"
    minutes, sec = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m {sec}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m {sec}s"


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


def format_status_line(ok: bool | None) -> str:
    if ok is True:
        return "OK"
    if ok is False:
        return "FAIL"
    return "UNKNOWN"


def format_kv_block(title: str, sections: Mapping[str, Mapping[str, object]]) -> str:
    """Render a readable multi-line log block with sections and indentation."""
    lines = [title]
    for section, values in sections.items():
        lines.append(f"  {section}:")
        for key, value in values.items():
            label = str(key).replace("_", " ")
            lines.append(f"    {label:<18}: {format_log_value(value)}")
    return "\n".join(lines)


def format_section_block(title: str, rows: Mapping[str, object]) -> str:
    """Render a compact section with a strong visual boundary."""
    clean_title = f" {str(title).strip().upper()} "
    line = clean_title.center(SECTION_WIDTH, "=")
    body = [line]
    for key, value in rows.items():
        label = str(key).replace("_", " ").title()
        body.append(f"  {label:<20}: {format_log_value(value)}")
    body.append("=" * SECTION_WIDTH)
    return "\n".join(body)


def format_main_progress(
    *,
    court: str,
    progress_name: str,
    current_name: str,
    current_code: str | int,
    completed: int,
    total: int,
    cases_collected: int,
    written: int,
    write_buffer: int,
    write_batch_size: int,
    pending_write: int | None = None,
) -> str:
    """Compact single-line progress for terminal readability."""
    total = max(0, int(total))
    completed = min(max(0, int(completed)), total)
    current_no = min(completed + 1, total) if total else 0
    left = max(total - completed - 1, 0) if total else 0
    pending = write_buffer if pending_write is None else max(0, int(pending_write))
    pct = format_percent(completed, total)
    return (
        f"[{court}] {current_no}/{total}({pct}) "
        f"current={format_log_value(current_name)} code={current_code} "
        f"collected={cases_collected} written={written} pending={pending} "
        f"buf={write_buffer}/{max(1, int(write_batch_size))}"
    )


def format_captcha_line(
    *,
    court: str,
    attempt_no: int,
    max_attempts: int,
    prediction: str | None,
    site_result: str,
    correct: bool | None,
    will_retry: bool,
    solver: str | None = None,
    case_no: int | str | None = None,
    total: int | None = None,
) -> str:
    fields: dict[str, object] = {
        "attempt": f"{attempt_no}/{max_attempts}",
        "predicted": prediction or "-",
        "solver": solver or "-",
        "correct": format_status_line(correct),
        "result": site_result,
        "retry": "yes" if will_retry else "no",
    }
    if case_no is not None:
        fields["case_no"] = case_no
    if total is not None:
        fields["cases_found"] = total
    return f"[{court}] CAPTCHA {format_log_fields(**fields)}"


def captcha_attempt_block(
    court: str,
    target_label: str,
    attempt_no: int,
    max_attempts: int,
    prediction: str | None,
    site_result: str,
    *,
    success: bool,
    will_retry: bool,
    case_no: int | str | None = None,
    total: int | None = None,
    solver: str | None = None,
) -> str:
    return format_captcha_line(
        court=court,
        attempt_no=attempt_no,
        max_attempts=max_attempts,
        prediction=prediction,
        site_result=site_result,
        correct=success,
        will_retry=will_retry,
        case_no=case_no,
        total=total,
        solver=solver,
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
        return "0/0 pct=0.0% done=0 left=0"
    done = min(max(0, int(index)), total)
    current = min(done + 1, total)
    left = max(total - done - 1, 0)
    return f"{current}/{total} pct={format_percent(done, total)} done={done} left={left}"


def descending_year_progress(current_year: int, start_year: int, end_year: int) -> str:
    """Format descending year progress for a block that moves end_year -> start_year."""
    total = max(0, int(end_year) - int(start_year) + 1)
    if total == 0:
        return "0/0 pct=0.0% done=0 left=0"
    done = min(max(int(end_year) - int(current_year), 0), total)
    current = min(done + 1, total)
    left = max(total - done - 1, 0)
    return f"{current}/{total} pct={format_percent(done, total)} done={done} left={left}"


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
