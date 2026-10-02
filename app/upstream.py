import logging
from itertools import cycle

import httpx

from .config import upstream_timeout, upstream_urls

log = logging.getLogger(__name__)

_TIMEOUT_DETAIL = {
    httpx.ConnectTimeout: "upstream connect timeout",
    httpx.ReadTimeout: "upstream read timeout",
    httpx.WriteTimeout: "upstream write timeout",
    httpx.PoolTimeout: "upstream pool timeout",
}


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 timeout: httpx.Timeout | None = None):
        self._urls = cycle(urls or upstream_urls())
        self._client = httpx.AsyncClient(timeout=timeout or upstream_timeout(),
                                         transport=transport)

    async def forward(self, payload: dict) -> tuple[int, dict]:
        url = next(self._urls)
        try:
            resp = await self._client.post(f"{url}/v1/completions", json=payload)
        except httpx.TimeoutException as exc:
            detail = _TIMEOUT_DETAIL.get(type(exc), "upstream timeout")
            log.warning("%s: %s", detail, url)
            return 504, {"detail": detail}
        return resp.status_code, resp.json()

    async def aclose(self) -> None:
        await self._client.aclose()
