"""
Proxy rotator for Webshare-format proxy lists.

Parses ip:port:user:pass lines, round-robins through them,
and temporarily bans proxies that accumulate too many failures.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger("legal_scraper.proxy")


@dataclass
class _ProxyEntry:
    url: str
    consecutive_failures: int = 0
    banned_until: float = 0.0


class ProxyRotator:
    """Thread-safe round-robin proxy pool with automatic banning."""

    def __init__(
        self,
        proxy_file: Path | str,
        max_failures: int = 10,
        ban_duration: int = 600,
    ) -> None:
        self._max_failures = max_failures
        self._ban_duration = ban_duration
        self._lock = asyncio.Lock()
        self._entries = self._load(Path(proxy_file))
        self._index = 0
        logger.info("Loaded %d proxies from %s", len(self._entries), proxy_file)

    @staticmethod
    def _load(path: Path) -> list[_ProxyEntry]:
        entries: list[_ProxyEntry] = []
        if not path.exists():
            logger.info("Proxy file %s not found; running without proxies.", path)
            return entries

        raw = path.read_text(encoding="utf-8", errors="ignore")
        for line in raw.splitlines():
            line = line.strip().replace("\r", "")
            if not line or line.startswith("#"):
                continue

            url = ""
            if "://" in line:
                parsed = urlparse(line)
                if parsed.scheme and parsed.hostname and parsed.port:
                    url = line
            else:
                parts = line.split(":")
                if len(parts) >= 4:
                    ip, port, user, passwd = parts[0], parts[1], parts[2], parts[3]
                    url = f"http://{user}:{passwd}@{ip}:{port}"
                elif len(parts) == 2:
                    host, port = parts
                    url = f"http://{host}:{port}"

            if not url:
                continue
            entries.append(_ProxyEntry(url=url))
        return entries

    async def get_proxy(self) -> str | None:
        """Return next usable proxy URL, skipping banned ones."""
        if not self._entries:
            return None

        async with self._lock:
            now = time.monotonic()
            attempts = 0
            total = len(self._entries)

            while attempts < total:
                entry = self._entries[self._index]
                self._index = (self._index + 1) % total
                if entry.banned_until <= now:
                    return entry.url
                attempts += 1

            # All proxies banned — unban the least-recently-banned one
            logger.warning("All proxies banned; unbanning oldest")
            oldest = min(self._entries, key=lambda e: e.banned_until)
            oldest.banned_until = 0.0
            oldest.consecutive_failures = 0
            return oldest.url

    async def report_success(self, proxy_url: str) -> None:
        """Reset failure count on success."""
        async with self._lock:
            for entry in self._entries:
                if entry.url == proxy_url:
                    entry.consecutive_failures = 0
                    break

    async def report_failure(self, proxy_url: str) -> None:
        """Increment failure count; ban proxy if threshold exceeded."""
        async with self._lock:
            for entry in self._entries:
                if entry.url == proxy_url:
                    entry.consecutive_failures += 1
                    if entry.consecutive_failures >= self._max_failures:
                        entry.banned_until = time.monotonic() + self._ban_duration
                        logger.warning(
                            "Banned proxy %s:%s for %ds after %d failures",
                            entry.url.split("@")[1].split(":")[0],
                            entry.url.split("@")[1].split(":")[1],
                            self._ban_duration,
                            entry.consecutive_failures,
                        )
                        entry.consecutive_failures = 0
                    break

    @property
    def pool_size(self) -> int:
        return len(self._entries)

    @property
    def active_count(self) -> int:
        now = time.monotonic()
        return sum(1 for e in self._entries if e.banned_until <= now)
