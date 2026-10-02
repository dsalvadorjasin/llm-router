import logging
from itertools import cycle

import httpx

from .config import upstream_urls

logger = logging.getLogger("llm-router.upstream")


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._urls = cycle(urls or upstream_urls())
        self._client = httpx.AsyncClient(timeout=None, transport=transport)

    async def forward(self, payload: dict,
                      request_id: str | None = None) -> tuple[int, dict, str]:
        url = next(self._urls)
        resp = await self._client.post(f"{url}/v1/completions", json=payload)
        logger.info(
            "upstream url=%s status=%s request_id=%s",
            url,
            resp.status_code,
            request_id,
        )
        return resp.status_code, resp.json(), url

    async def aclose(self) -> None:
        await self._client.aclose()
