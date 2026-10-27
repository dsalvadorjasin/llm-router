import asyncio
import logging
import time

import httpx

from .config import upstream_cooldown_s, upstream_timeout_s, upstream_urls

logger = logging.getLogger("llm-router.upstream")

_MAX_ATTEMPTS = 2


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 timeout_s: float | None = None,
                 cooldown_s: float | None = None):
        self._urls = list(urls or upstream_urls())
        self._next = 0
        self._cooldown_s = upstream_cooldown_s() if cooldown_s is None else cooldown_s
        self._failed_at: dict[str, float] = {}
        self._timeout_s = upstream_timeout_s() if timeout_s is None else timeout_s
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(self._timeout_s),
                                         transport=transport)

    def _in_cooldown(self, url: str, now: float) -> bool:
        failed_at = self._failed_at.get(url)
        return failed_at is not None and now - failed_at < self._cooldown_s

    def _candidates(self) -> list[str]:
        """Round-robin over healthy URLs; URLs in cooldown are only a last resort."""
        now = time.monotonic()
        healthy = [u for u in self._urls if not self._in_cooldown(u, now)]
        # Oldest failure first: the likeliest to have recovered.
        cooling = sorted((u for u in self._urls if self._in_cooldown(u, now)),
                         key=self._failed_at.__getitem__)
        if healthy:
            start = self._next % len(healthy)
            healthy = healthy[start:] + healthy[:start]
        self._next += 1
        return ((healthy + cooling) * _MAX_ATTEMPTS)[:_MAX_ATTEMPTS]

    async def forward(self, payload: dict) -> tuple[int, dict]:
        last_error: httpx.HTTPError | None = None
        for url in self._candidates():
            start = time.monotonic()
            try:
                # httpx timeouts are per phase (e.g. between body chunks);
                # this caps the whole attempt.
                async with asyncio.timeout(self._timeout_s):
                    resp = await self._client.post(f"{url}/v1/completions", json=payload)
            except (httpx.TransportError, TimeoutError) as exc:
                if not isinstance(exc, httpx.HTTPError):
                    exc = httpx.ReadTimeout(f"upstream exceeded {self._timeout_s}s")
                self._failed_at[url] = time.monotonic()
                logger.warning(
                    "upstream url=%s error=%s duration_ms=%.1f cooldown_s=%s",
                    url, type(exc).__name__, (time.monotonic() - start) * 1000,
                    self._cooldown_s,
                )
                last_error = exc
                continue
            self._failed_at.pop(url, None)
            logger.info("upstream url=%s status=%s duration_ms=%.1f",
                        url, resp.status_code, (time.monotonic() - start) * 1000)
            return resp.status_code, resp.json()
        assert last_error is not None
        raise last_error

    async def aclose(self) -> None:
        await self._client.aclose()
