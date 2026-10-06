from itertools import cycle

import httpx

from .config import latency_aware_enabled, routing_config, upstream_urls
from .routing import LatencyAwareRouter


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        urls = urls or upstream_urls()
        self._urls = cycle(urls)
        self._router = LatencyAwareRouter(urls, routing_config()) if latency_aware_enabled() else None
        self._client = httpx.AsyncClient(timeout=None, transport=transport)

    async def forward(self, payload: dict) -> tuple[int, dict]:
        if self._router is not None:
            return await self._router.forward(self._client, payload)
        url = next(self._urls)
        resp = await self._client.post(f"{url}/v1/completions", json=payload)
        return resp.status_code, resp.json()

    async def aclose(self) -> None:
        await self._client.aclose()
