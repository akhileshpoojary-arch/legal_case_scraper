"""
HTTP client abstraction — switch between aiohttp, httpx, requests, urllib3.

Usage:
    client = create_http_client("aiohttp")
    response = await client.post(url, data=payload, headers=headers, cookies=cookies)
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger("legal_scraper.http")


class BaseHTTPClient(ABC):
    """Unified interface for all HTTP backends."""

    @abstractmethod
    async def post(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> dict | list | None:
        """POST request, return parsed JSON or None on failure."""
        ...

    @abstractmethod
    async def get(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> dict | list | None:
        """GET request, return parsed JSON or None on failure."""
        ...

    async def post_text(
        self,
        url: str,
        *,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> str:
        """POST request, return raw response text. Override in subclasses."""
        raise NotImplementedError

    async def get_text(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> str:
        """GET request, return raw response text. Override in subclasses."""
        raise NotImplementedError

    async def get_bytes(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> bytes | None:
        """GET request, return raw bytes (for images/binary). Override in subclasses."""
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources."""
        ...


class AiohttpClient(BaseHTTPClient):
    """Async HTTP client using aiohttp."""

    def __init__(self, proxy: str | None = None) -> None:
        self._session = None
        self._proxy: str | None = None
        self._proxy_auth: Any = None
        if proxy:
            self._proxy, self._proxy_auth = self._parse_proxy(proxy)

    @staticmethod
    def _parse_proxy(proxy_url: str) -> tuple[str, Any]:
        """Extract BasicAuth from http://user:pass@host:port format."""
        import aiohttp
        from urllib.parse import urlparse

        parsed = urlparse(proxy_url)
        auth = None
        if parsed.username and parsed.password:
            auth = aiohttp.BasicAuth(parsed.username, parsed.password)
        clean_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        return clean_url, auth

    async def _ensure_session(self) -> Any:
        if self._session is None:
            import aiohttp

            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=20,
                ssl=False,
                enable_cleanup_closed=True,
            )
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    async def post(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> dict | list | None:
        import aiohttp

        session = await self._ensure_session()
        try:
            if data is not None:
                form = aiohttp.FormData()
                for k, v in data.items():
                    form.add_field(k, str(v))
                post_data: Any = form
            else:
                post_data = None

            async with session.post(
                url,
                data=post_data if data else None,
                json=json_data,
                headers=headers,
                cookies=cookies,
                timeout=aiohttp.ClientTimeout(total=timeout),
                proxy=self._proxy,
                proxy_auth=self._proxy_auth,
            ) as resp:
                resp.raise_for_status()
                text = (await resp.text()).strip()
                if not text or text in ("null", "[]", "{}"):
                    return None
                return json.loads(text)
        except Exception as exc:
            logger.debug("aiohttp POST %s failed: %s", url, exc)
            raise

    async def get(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> dict | list | None:
        import aiohttp

        session = await self._ensure_session()
        try:
            async with session.get(
                url,
                params=params,
                headers=headers,
                cookies=cookies,
                timeout=aiohttp.ClientTimeout(total=timeout),
                proxy=self._proxy,
                proxy_auth=self._proxy_auth,
            ) as resp:
                resp.raise_for_status()
                text = (await resp.text()).strip()
                if not text or text in ("null", "[]", "{}"):
                    return None
                return json.loads(text)
        except Exception as exc:
            logger.debug("aiohttp GET %s failed: %s", url, exc)
            raise

    async def post_text(
        self,
        url: str,
        *,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> str:
        """POST returning raw text (for HTML/BOM-prefixed responses)."""
        import aiohttp

        session = await self._ensure_session()
        try:
            # Support both dict (form data) and raw string payloads
            if isinstance(data, dict):
                form = aiohttp.FormData()
                for k, v in data.items():
                    form.add_field(k, str(v))
                post_data: Any = form
            elif isinstance(data, str):
                post_data = data
            else:
                post_data = None

            async with session.post(
                url,
                data=post_data,
                headers=headers,
                cookies=cookies,
                timeout=aiohttp.ClientTimeout(total=timeout),
                proxy=self._proxy,
                proxy_auth=self._proxy_auth,
            ) as resp:
                resp.raise_for_status()
                return await resp.text()
        except Exception as exc:
            logger.debug("aiohttp POST_TEXT %s failed: %s", url, exc)
            raise

    async def get_text(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> str:
        """GET returning raw text."""
        import aiohttp

        session = await self._ensure_session()
        try:
            async with session.get(
                url,
                params=params,
                headers=headers,
                cookies=cookies,
                timeout=aiohttp.ClientTimeout(total=timeout),
                proxy=self._proxy,
                proxy_auth=self._proxy_auth,
            ) as resp:
                resp.raise_for_status()
                return await resp.text()
        except Exception as exc:
            logger.debug("aiohttp GET_TEXT %s failed: %s", url, exc)
            raise

    async def get_bytes(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> bytes | None:
        """GET returning raw bytes — for captcha images and other binary content."""
        import aiohttp as _aiohttp

        session = await self._ensure_session()
        try:
            async with session.get(
                url,
                headers=headers,
                cookies=cookies,
                timeout=_aiohttp.ClientTimeout(total=timeout),
                proxy=self._proxy,
                proxy_auth=self._proxy_auth,
            ) as resp:
                resp.raise_for_status()
                return await resp.read()
        except Exception as exc:
            logger.debug("aiohttp GET_BYTES %s failed: %s", url, exc)
            raise

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None


class HttpxClient(BaseHTTPClient):
    """Async HTTP client using httpx."""

    def __init__(self) -> None:
        self._client = None

    async def _ensure_client(self) -> Any:
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(verify=False, timeout=30.0)
        return self._client

    async def post(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> dict | list | None:
        client = await self._ensure_client()
        try:
            resp = await client.post(
                url,
                data=data,
                json=json_data,
                headers=headers,
                cookies=cookies,
                timeout=timeout,
            )
            resp.raise_for_status()
            text = resp.text.strip()
            if not text or text in ("null", "[]", "{}"):
                return None
            return json.loads(text)
        except Exception as exc:
            logger.debug("httpx POST %s failed: %s", url, exc)
            raise

    async def get(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> dict | list | None:
        client = await self._ensure_client()
        try:
            resp = await client.get(
                url,
                params=params,
                headers=headers,
                cookies=cookies,
                timeout=timeout,
            )
            resp.raise_for_status()
            text = resp.text.strip()
            if not text or text in ("null", "[]", "{}"):
                return None
            return json.loads(text)
        except Exception as exc:
            logger.debug("httpx GET %s failed: %s", url, exc)
            raise

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None


class RequestsClient(BaseHTTPClient):
    """Sync HTTP client wrapped in async interface (using requests)."""

    def __init__(self) -> None:
        import requests

        self._session = requests.Session()

    async def post(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> dict | list | None:
        try:
            resp = self._session.post(
                url,
                data=data,
                json=json_data,
                headers=headers,
                cookies=cookies,
                timeout=timeout,
            )
            resp.raise_for_status()
            text = resp.text.strip()
            if not text or text in ("null", "[]", "{}"):
                return None
            return json.loads(text)
        except Exception as exc:
            logger.debug("requests POST %s failed: %s", url, exc)
            raise

    async def get(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> dict | list | None:
        try:
            resp = self._session.get(
                url,
                params=params,
                headers=headers,
                cookies=cookies,
                timeout=timeout,
            )
            resp.raise_for_status()
            text = resp.text.strip()
            if not text or text in ("null", "[]", "{}"):
                return None
            return json.loads(text)
        except Exception as exc:
            logger.debug("requests GET %s failed: %s", url, exc)
            raise

    async def close(self) -> None:
        self._session.close()


def create_http_client(
    client_type: str = "aiohttp",
    proxy: str | None = None,
) -> BaseHTTPClient:
    """
    Factory — returns the requested HTTP client backend.

    Supports: "aiohttp", "httpx", "requests"
    """
    if client_type == "aiohttp":
        # logger.info("Using HTTP client: aiohttp (proxy=%s)", bool(proxy))
        return AiohttpClient(proxy=proxy)

    clients: dict[str, type[BaseHTTPClient]] = {
        "httpx": HttpxClient,
        "requests": RequestsClient,
    }

    if client_type not in clients:
        raise ValueError(
            f"Unknown HTTP client: {client_type!r}. "
            f"Supported: ['aiohttp', 'httpx', 'requests']"
        )

    # logger.info("Using HTTP client: %s", client_type)
    return clients[client_type]()
