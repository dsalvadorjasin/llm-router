from itertools import cycle

import httpx

from .config import hedge_settings, hedging_enabled, upstream_urls
from .hedging import Hedger, HedgeSettings


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        urls = urls or upstream_urls()
        self._urls = cycle(urls)
        self._client = httpx.AsyncClient(timeout=None, transport=transport)
        self._hedger = (Hedger(urls, self._client, HedgeSettings(**hedge_settings()))
                        if hedging_enabled() else None)

    async def forward(self, payload: dict) -> tuple[int, dict]:
        if self._hedger is not None:
            return await self._hedger.forward(payload)
        url = next(self._urls)
        resp = await self._client.post(f"{url}/v1/completions", json=payload)
        return resp.status_code, resp.json()

    async def aclose(self) -> None:
        await self._client.aclose()
