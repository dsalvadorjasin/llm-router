from itertools import cycle

import httpx

from .config import upstream_urls
from .middleware.logging import upstream_info


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._urls = cycle(urls or upstream_urls())
        self._client = httpx.AsyncClient(timeout=None, transport=transport)

    async def forward(self, payload: dict) -> tuple[int, dict]:
        url = next(self._urls)
        resp = await self._client.post(f"{url}/v1/completions", json=payload)
        info = upstream_info.get()
        if info is not None:
            info["upstream_url"] = url
            info["hedged"] = False
        return resp.status_code, resp.json()

    async def aclose(self) -> None:
        await self._client.aclose()
