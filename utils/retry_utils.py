"""Retry decorator with exponential backoff — works for both sync and async."""

from __future__ import annotations

import asyncio
import functools
import logging
from typing import Any, Callable

logger = logging.getLogger("legal_scraper.retry")


def retry_async(
    max_retries: int = 3,
    base_delay: float = 1.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable:
    """
    Async retry decorator with exponential backoff.

    Retries on specified exceptions, doubling the wait each attempt.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Exception | None = None

            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt < max_retries:
                        wait = base_delay * (2 ** (attempt - 1))
                        logger.warning(
                            "%s attempt %d/%d failed: %s → retry in %.1fs",
                            func.__name__,
                            attempt,
                            max_retries,
                            exc,
                            wait,
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            func.__name__,
                            max_retries,
                            exc,
                        )

            raise last_exc  # type: ignore[misc]

        return wrapper

    return decorator
