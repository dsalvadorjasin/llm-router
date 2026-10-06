from itertools import cycle

import httpx

from .config import (
    hedge_settings,
    hedging_enabled,
    latency_aware_enabled,
    routing_config,
    upstream_urls,
)
from .hedging import Hedger, HedgeSettings
from .routing import LatencyAwareRouter


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        urls = urls or upstream_urls()
        self._urls = cycle(urls)
        self._client = httpx.AsyncClient(timeout=None, transport=transport)
        self._router = LatencyAwareRouter(urls, routing_config()) if latency_aware_enabled() else None
        # With both enabled the router picks the primary and hedge targets and
        # the hedger drives timing/failover, feeding latencies back to the router.
        self._hedger = (Hedger(urls, self._client, HedgeSettings(**hedge_settings()),
                               router=self._router)
                        if hedging_enabled() else None)

    @property
    def fails_over(self) -> bool:
        return self._hedger is not None or self._router is not None

    async def forward(self, payload: dict) -> tuple[int, dict]:
        if self._hedger is not None:
            return await self._hedger.forward(payload)
        if self._router is not None:
            return await self._router.forward(self._client, payload)
        url = next(self._urls)
        resp = await self._client.post(f"{url}/v1/completions", json=payload)
        return resp.status_code, resp.json()

    async def aclose(self) -> None:
        await self._client.aclose()
