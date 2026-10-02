import logging
from itertools import cycle
from urllib.parse import urlsplit, urlunsplit

import httpx

from .config import upstream_urls

logger = logging.getLogger("llm-router.upstream")


def redact_url(url: str) -> str:
    """Strip any userinfo (``user:pass@``) so the URL is safe to log or return."""
    parts = urlsplit(url)
    return urlunsplit(parts._replace(netloc=parts.netloc.rpartition("@")[2]))


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._urls = cycle(urls or upstream_urls())
        self._client = httpx.AsyncClient(timeout=None, transport=transport)

    async def forward(self, payload: dict,
                      request_id: str | None = None) -> tuple[int, dict, str]:
        url = next(self._urls)
        safe_url = redact_url(url)
        try:
            resp = await self._client.post(f"{url}/v1/completions", json=payload)
        except httpx.HTTPError as exc:
            logger.warning(
                "upstream url=%s error=%s request_id=%s",
                safe_url,
                type(exc).__name__,
                request_id,
            )
            raise
        logger.info(
            "upstream url=%s status=%s request_id=%s",
            safe_url,
            resp.status_code,
            request_id,
        )
        return resp.status_code, resp.json(), safe_url

    async def aclose(self) -> None:
        await self._client.aclose()
