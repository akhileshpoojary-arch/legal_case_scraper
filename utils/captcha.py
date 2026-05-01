"""
CAPTCHA solver for ecourts.gov.in and sci.gov.in captchas.

Uses an ensemble approach: Keras model + ddddocr + image preprocessing.
The ensemble auto-tracks which solver is more accurate at runtime.

- Type 1 (CTC): DC/HC securimage text captchas → 6-char alphanumeric
- Type 2 (Classifier): SC math captchas → numeric answer (0–20)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

logger = logging.getLogger("legal_scraper.captcha")

_CAPTCHA_EXECUTOR = ThreadPoolExecutor(
    max_workers=max(1, int(os.environ.get("CAPTCHA_EXECUTOR_WORKERS", "4"))),
    thread_name_prefix="captcha",
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def warm_up_reader() -> None:
    """Load all CAPTCHA solvers into memory on startup."""
    from utils.captcha_ensemble import get_ensemble_solver

    solver = get_ensemble_solver()
    logger.info(
        "Captcha ensemble solver ready (mode=%s, preprocess=%s)",
        os.environ.get("CAPTCHA_SOLVER_MODE", "ensemble"),
        _env_bool("CAPTCHA_PREPROCESS", True),
    )


def solve(image_bytes: bytes, expected_length: int = 6, prefix: str = "hc") -> str:
    """
    Solve a CAPTCHA from raw image bytes using the ensemble solver.

    DC/HC (prefix='dc'/'hc'): Type 1 → 6-char text
    SCI (prefix='sci'): Type 2 → numeric answer string

    Returns empty string on failure.
    """
    prediction, _solver = solve_with_metadata(image_bytes, expected_length, prefix)
    return prediction


def solve_with_metadata(
    image_bytes: bytes,
    expected_length: int = 6,
    prefix: str = "hc",
) -> tuple[str, str]:
    """Solve a CAPTCHA and return ``(answer, solver_name)`` for feedback."""
    from utils.captcha_ensemble import get_ensemble_solver

    try:
        solver = get_ensemble_solver()

        if prefix == "sci":
            prediction = solver.predict_type2_with_source(image_bytes)
            return prediction.answer, prediction.solver

        prediction = solver.predict_type1_with_source(image_bytes)
        return prediction.answer, prediction.solver
    except Exception as exc:
        logger.warning("CAPTCHA solve failed (ensemble): %s", exc)
        return "", "none"


async def solve_async(
    image_bytes: bytes,
    expected_length: int = 6,
    prefix: str = "hc",
) -> str:
    """
    Async wrapper around solve() using a dedicated, bounded thread pool.
    Prevents unbounded default-executor growth under heavy parallel load.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _CAPTCHA_EXECUTOR, solve, image_bytes, expected_length, prefix
    )


async def solve_async_with_metadata(
    image_bytes: bytes,
    expected_length: int = 6,
    prefix: str = "hc",
) -> tuple[str, str]:
    """Async wrapper returning ``(answer, solver_name)``."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _CAPTCHA_EXECUTOR,
        solve_with_metadata,
        image_bytes,
        expected_length,
        prefix,
    )


def record_captcha_feedback(
    prefix: str,
    accepted: bool,
    solver_name: str = "keras",
) -> None:
    """
    Record whether the server accepted/rejected the last captcha answer.

    Called by extractors after each search attempt to help the ensemble
    learn which solver is performing better at runtime.
    """
    try:
        from utils.captcha_ensemble import get_ensemble_solver
        solver = get_ensemble_solver()
        solver.record_feedback(prefix, solver_name, accepted)
    except Exception:
        pass


def save_captcha_image(image_bytes: bytes, response: str, prefix: str) -> None:
    """
    Save CAPTCHA image to subfolder named after the response.
    sc uses timestamp to avoid overwrites.
    """
    if not _env_bool("CAPTCHA_SAVE_SUCCESS_IMAGES", False):
        return

    if not image_bytes or not response:
        return

    # Map prefixes to folders
    folder_map = {"sci": "sc", "hc": "hc", "dc": "dc"}
    folder_name = folder_map.get(prefix, prefix)

    # Root of scraper is one level up from utils/
    base_dir = Path(__file__).parent.parent / "captcha_img"
    out_dir = base_dir / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)

    if folder_name == "sc":
        # SCI (SC) answers are 0-20, so we need timestamps
        filename = f"{response}_{int(time.time() * 1000)}.png"
    else:
        # HC/DC text usually unique or overwrite is acceptable
        filename = f"{response}.png"

    try:
        with open(out_dir / filename, "wb") as f:
            f.write(image_bytes)
    except Exception as e:
        logger.warning("Failed to save captcha image: %s", e)
