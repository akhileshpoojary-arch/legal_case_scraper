"""
Proxy rotator for Webshare-format proxy lists.

Parses ip:port:user:pass lines, tracks per-proxy success rates,
and temporarily bans proxies that fail.
"""

from __future__ import annotations

import asyncio
import logging
import random
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
    total_requests: int = 0
    total_successes: int = 0

    @property
    def success_rate(self) -> float:
        if self.total_requests < 5:
            return 0.5  # neutral until enough data
        return self.total_successes / self.total_requests


class ProxyRotator:
    """Thread-safe proxy pool with success-weighted selection and auto-banning."""

    # How many candidates to sample before weighting (power-of-k load balancing).
    # Keeps selection cheap and well-spread even for very large proxy lists.
    _SAMPLE_SIZE = 64

    def __init__(
        self,
        proxy_file: Path | str,
        max_failures: int = 10,
        ban_duration: int = 300,
    ) -> None:
        self._max_failures = max_failures
        self._ban_duration = ban_duration
        self._lock = asyncio.Lock()
        self._entries = self._load(Path(proxy_file))
        # O(1) lookup by URL
        self._url_index: dict[str, int] = {
            e.url: i for i, e in enumerate(self._entries)
        }
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

        # Shuffle on load to distribute across workers
        random.shuffle(entries)
        return entries

    def _find_entry(self, proxy_url: str) -> _ProxyEntry | None:
        """O(1) proxy lookup by URL."""
        idx = self._url_index.get(proxy_url)
        if idx is not None and idx < len(self._entries):
            return self._entries[idx]
        return None

    async def get_proxy(self) -> str | None:
        """Return a usable proxy URL, favouring ones with a better success rate.

        Picks via "power-of-k": sample a handful of healthy proxies, then choose
        one with probability weighted by its success rate (with a small floor so
        unproven proxies still get explored). This sends more traffic to
        reliable IPs without overloading any single one, and stays cheap even
        for very large pools.
        """
        if not self._entries:
            return None

        async with self._lock:
            now = time.monotonic()
            usable = [e for e in self._entries if e.banned_until <= now]
            if not usable:
                # All banned — revive the one with the best historical record.
                logger.warning("All proxies banned; unbanning best-performing one")
                best = max(self._entries, key=lambda e: e.success_rate)
                best.banned_until = 0.0
                best.consecutive_failures = 0
                return best.url

            # Prefer proxies with no recent consecutive failures.
            healthy = [e for e in usable if e.consecutive_failures == 0] or usable

            k = min(len(healthy), self._SAMPLE_SIZE)
            sample = random.sample(healthy, k) if k < len(healthy) else healthy
            weights = [max(0.05, e.success_rate) for e in sample]
            chosen = random.choices(sample, weights=weights, k=1)[0]
            return chosen.url

    async def report_success(self, proxy_url: str) -> None:
        """Reset failure count and track success."""
        async with self._lock:
            entry = self._find_entry(proxy_url)
            if entry:
                entry.consecutive_failures = 0
                entry.total_requests += 1
                entry.total_successes += 1

    async def report_failure(self, proxy_url: str) -> None:
        """Increment failure count; ban proxy if threshold exceeded."""
        async with self._lock:
            entry = self._find_entry(proxy_url)
            if not entry:
                return
            entry.consecutive_failures += 1
            entry.total_requests += 1
            if entry.consecutive_failures >= self._max_failures:
                entry.banned_until = time.monotonic() + self._ban_duration
                # Log sanitized proxy info (hide credentials)
                try:
                    host_port = entry.url.split("@")[1] if "@" in entry.url else entry.url
                except Exception:
                    host_port = "unknown"
                logger.warning(
                    "Banned proxy %s for %ds after %d failures (success_rate=%.1f%%)",
                    host_port,
                    self._ban_duration,
                    entry.consecutive_failures,
                    entry.success_rate * 100,
                )
                entry.consecutive_failures = 0

    @property
    def pool_size(self) -> int:
        return len(self._entries)

    @property
    def active_count(self) -> int:
        now = time.monotonic()
        return sum(1 for e in self._entries if e.banned_until <= now)

    def stats_summary(self) -> str:
        """Return summary for periodic logging."""
        now = time.monotonic()
        active = sum(1 for e in self._entries if e.banned_until <= now)
        if not self._entries:
            return "no proxies"
        banned = len(self._entries) - active
        avg_rate = sum(e.success_rate for e in self._entries) / len(self._entries)
        max_recent_failures = max(e.consecutive_failures for e in self._entries)
        return (
            f"active={active}/{len(self._entries)} banned={banned} "
            f"avg_success={avg_rate:.1%} max_recent_failures={max_recent_failures}"
        )
