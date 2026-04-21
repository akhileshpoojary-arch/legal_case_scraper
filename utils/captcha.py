"""
CAPTCHA solver for ecourts.gov.in and sci.gov.in captchas.

Uses custom-trained Keras models:
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

import requests as req

logger = logging.getLogger("legal_scraper.captcha")

_CAPTCHA_EXECUTOR = ThreadPoolExecutor(
    max_workers=max(1, int(os.environ.get("CAPTCHA_EXECUTOR_WORKERS", "2"))),
    thread_name_prefix="captcha",
)


def warm_up_reader() -> None:
    """Load Keras models into memory on startup (singleton, safe to call multiple times)."""
    from utils.captcha_model import get_solver

    solver = get_solver()
    logger.info(
        "Captcha solver ready (Keras model mode — Type1 CTC + Type2 Classifier)"
    )


def solve(image_bytes: bytes, expected_length: int = 6, prefix: str = "hc") -> str:
    """
    Solve a CAPTCHA from raw image bytes using the trained Keras model.

    DC/HC (prefix='dc'/'hc'): Type 1 CTC model → 6-char text
    SCI (prefix='sci'): Type 2 Classifier → numeric answer string

    Returns empty string on failure.
    """
    from utils.captcha_model import get_solver

    try:
        solver = get_solver()

        if prefix == "sci":
            return solver.predict_type2(image_bytes)

        return solver.predict_type1(image_bytes)
    except Exception as exc:
        logger.warning("CAPTCHA solve failed (model): %s", exc)
        return ""


async def solve_async(
    image_bytes: bytes,
    expected_length: int = 6,
    prefix: str = "hc",
) -> str:
    """
    Async wrapper around solve() using a dedicated, bounded thread pool.
    This prevents unbounded default-executor growth under heavy parallel load.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _CAPTCHA_EXECUTOR, solve, image_bytes, expected_length, prefix
    )


def download_and_solve(
    captcha_url: str,
    cookies: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 15.0,
    prefix: str = "hc",
) -> tuple[str, dict[str, str]]:
    """
    Download captcha via requests and solve it.

    Pass session cookies so the server associates the answer with the right session.
    """
    img_headers = {
        **(headers or {}),
        "accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "sec-fetch-dest": "image",
        "sec-fetch-mode": "no-cors",
    }
    try:
        resp = req.get(
            captcha_url,
            cookies=cookies,
            headers=img_headers,
            timeout=timeout,
        )
        resp.raise_for_status()
        if len(resp.content) < 200:
            return "", {}
        return solve(resp.content, prefix=prefix), resp.cookies.get_dict()
    except Exception as exc:
        logger.debug("Captcha download+solve failed: %s", exc)
        return "", {}


def save_captcha_image(image_bytes: bytes, response: str, prefix: str) -> None:
    """
    Save CAPTCHA image to subfolder named after the response.
    sc uses timestamp to avoid overwrites.
    """
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
