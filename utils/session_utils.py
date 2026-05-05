"""
Session management with automatic cookie/session rotation on failures.

Tracks consecutive failures and creates a fresh session + rotates cookies
when MAX_FAILURES_BEFORE_ROTATE is exceeded. Optionally routes traffic
through a ProxyRotator for IP diversity.

Includes User-Agent refresh when a session is rotated.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any

from utils.logging_utils import format_kv_block
from utils.http_client import BaseHTTPClient, create_http_client
from utils.proxy import ProxyRotator

logger = logging.getLogger("legal_scraper.session")

# Browser User-Agent pool used when a failed session is refreshed.
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
]


class SessionManager:
    """
    Wraps an HTTP client with failure tracking and auto-rotation.

    After MAX_FAILURES consecutive fails, rotates the underlying session
    and logs the event for debugging.
    """

    def __init__(
        self,
        client_type: str = "aiohttp",
        cookies: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        max_failures: int = 5,
        semaphore_limit: int = 20,
        request_delay: float = 0.05,
        proxy_rotator: ProxyRotator | None = None,
        name: str | None = None,
    ) -> None:
        self._client_type = client_type
        self._name = name or client_type
        self._cookies = dict(cookies) if cookies else {}
        self._headers = dict(headers) if headers else {}
        self._max_failures = max_failures
        self._consecutive_failures = 0
        self._rotation_count = 0
        self._client: BaseHTTPClient | None = None
        self.semaphore = asyncio.Semaphore(semaphore_limit)
        self._request_delay = request_delay
        self._proxy_rotator = proxy_rotator
        self._current_proxy: str | None = None
        self._last_failure_reason: str | None = None
        self._last_proxy_success_report_at = 0.0

    async def _ensure_client(self) -> BaseHTTPClient:
        if self._client is None:
            if self._proxy_rotator and not self._current_proxy:
                self._current_proxy = await self._proxy_rotator.get_proxy()
            self._client = create_http_client(
                self._client_type, proxy=self._current_proxy
            )
        return self._client

    async def _rotate_session(self) -> None:
        """Close current session and create a fresh one with a new proxy + UA."""
        self._rotation_count += 1
        if self._proxy_rotator and self._current_proxy:
            await self._proxy_rotator.report_failure(self._current_proxy)
        proxy_summary = (
            self._proxy_rotator.stats_summary() if self._proxy_rotator else "no proxy pool"
        )
        logger.warning(
            format_kv_block(
                f"[session:{self._name}] Rotation",
                {
                    "Session": {
                        "rotation": self._rotation_count,
                        "failures": self._consecutive_failures,
                        "reason": self._last_failure_reason or "unknown",
                    },
                    "Proxy": {
                        "current": self._proxy_label(self._current_proxy),
                        "pool": proxy_summary,
                    },
                },
            )
        )
        if self._client:
            await self._client.close()
            self._client = None
        self._consecutive_failures = 0
        # Rotate User-Agent on session refresh
        self._headers["User-Agent"] = random.choice(_USER_AGENTS)
        # Pick a fresh proxy for the next session
        if self._proxy_rotator:
            self._current_proxy = await self._proxy_rotator.get_proxy()

    async def force_refresh(
        self, home_url: str, headers: dict[str, str] | None = None
    ) -> bool:
        """
        Force a session rotation and hit a home URL to acquire fresh cookies.
        Useful for scrapers where session tokens expire mid-run.
        """
        async with self.semaphore:
            logger.info("Forcing session refresh via %s", home_url)
            await self._rotate_session()
            client = await self._ensure_client()
            try:
                # HTTP GET to the homepage just to receive `Set-Cookie` headers
                await client.get(
                    home_url, headers=headers or self._headers, timeout=20.0
                )
                self._record_success()
                return True
            except Exception as exc:
                logger.error("Force refresh failed: %s", exc)
                return False

    def _record_success(self) -> None:
        had_failures = self._consecutive_failures > 0
        self._consecutive_failures = 0
        self._last_failure_reason = None
        if self._proxy_rotator and self._current_proxy:
            now = time.monotonic()
            if had_failures or now - self._last_proxy_success_report_at >= 30.0:
                self._last_proxy_success_report_at = now
                asyncio.create_task(
                    self._proxy_rotator.report_success(self._current_proxy)
                )

    @staticmethod
    def _classify_exception(exc: Exception) -> str:
        """Return a short, log-friendly category for transport errors."""
        name = exc.__class__.__name__.lower()
        msg = str(exc).lower()
        if "timeout" in name or "timeout" in msg or isinstance(exc, TimeoutError):
            return "timeout"
        if "proxy" in name or "proxy" in msg:
            return "proxy_error"
        if "connect" in name or "connection" in name or "connect" in msg:
            return "connection_error"
        if "response" in name or "status" in msg:
            return "http_error"
        return name or "unknown_error"

    @staticmethod
    def _proxy_label(proxy_url: str | None) -> str:
        if not proxy_url:
            return "-"
        try:
            return proxy_url.split("@", 1)[1] if "@" in proxy_url else proxy_url
        except Exception:
            return "unknown"

    def consume_last_failure_reason(self) -> str | None:
        """Fetch and clear the last recorded failure reason."""
        reason = self._last_failure_reason
        self._last_failure_reason = None
        return reason

    async def _record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._max_failures:
            await self._rotate_session()
        else:
            # Adaptive backoff: 0.5s → 1s → 2s → 4s (capped)
            delay = min(0.5 * (2 ** (self._consecutive_failures - 1)), 4.0)
            await asyncio.sleep(delay)

    async def post(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        timeout: float = 20.0,
        label: str = "",
    ) -> dict | list | None:
        """POST with semaphore, delay, and failure tracking."""
        async with self.semaphore:
            await asyncio.sleep(self._request_delay)
            client = await self._ensure_client()
            try:
                result = await client.post(
                    url,
                    data=data,
                    json_data=json_data,
                    headers=self._headers,
                    cookies=self._cookies,
                    timeout=timeout,
                )
                self._record_success()
                return result
            except Exception as exc:
                self._last_failure_reason = self._classify_exception(exc)
                await self._record_failure()
                if label:
                    logger.debug("%s → %s", label, exc)
                return None

    async def get(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        timeout: float = 20.0,
        label: str = "",
    ) -> dict | list | None:
        """GET with semaphore, delay, and failure tracking."""
        async with self.semaphore:
            await asyncio.sleep(self._request_delay)
            client = await self._ensure_client()
            try:
                result = await client.get(
                    url,
                    params=params,
                    headers=self._headers,
                    cookies=self._cookies,
                    timeout=timeout,
                )
                self._record_success()
                return result
            except Exception as exc:
                self._last_failure_reason = self._classify_exception(exc)
                await self._record_failure()
                if label:
                    logger.debug("%s → %s", label, exc)
                return None

    async def post_text(
        self,
        url: str,
        *,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 20.0,
        label: str = "",
    ) -> str | None:
        """POST returning raw text (for HTML/BOM responses)."""
        async with self.semaphore:
            await asyncio.sleep(self._request_delay)
            client = await self._ensure_client()
            try:
                result = await client.post_text(
                    url,
                    data=data,
                    headers={**self._headers, **(headers or {})},
                    cookies=self._cookies,
                    timeout=timeout,
                )
                self._record_success()
                return result
            except Exception as exc:
                self._last_failure_reason = self._classify_exception(exc)
                await self._record_failure()
                if label:
                    logger.debug("%s → %s", label, exc)
                return None

    async def get_text(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 20.0,
        label: str = "",
    ) -> str | None:
        """GET returning raw text."""
        async with self.semaphore:
            await asyncio.sleep(self._request_delay)
            client = await self._ensure_client()
            try:
                result = await client.get_text(
                    url,
                    params=params,
                    headers={**self._headers, **(headers or {})},
                    cookies=self._cookies,
                    timeout=timeout,
                )
                self._record_success()
                return result
            except Exception as exc:
                self._last_failure_reason = self._classify_exception(exc)
                await self._record_failure()
                if label:
                    logger.debug("%s → %s", label, exc)
                return None

    async def get_bytes(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float = 20.0,
        label: str = "",
    ) -> bytes | None:
        """GET returning raw bytes (captcha images, binary downloads)."""
        async with self.semaphore:
            await asyncio.sleep(self._request_delay)
            client = await self._ensure_client()
            try:
                result = await client.get_bytes(
                    url,
                    headers={**self._headers, **(headers or {})},
                    cookies=self._cookies,
                    timeout=timeout,
                )
                self._record_success()
                return result
            except Exception as exc:
                self._last_failure_reason = self._classify_exception(exc)
                await self._record_failure()
                if label:
                    logger.debug("%s → %s", label, exc)
                return None

    def update_cookies(self, new_cookies: dict[str, str]) -> None:
        """Hot-swap cookies without restarting the session."""
        self._cookies.update(new_cookies)
        logger.info("Cookies updated: %s", list(new_cookies.keys()))

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    @property
    def stats(self) -> dict[str, int]:
        return {
            "consecutive_failures": self._consecutive_failures,
            "total_rotations": self._rotation_count,
        }
