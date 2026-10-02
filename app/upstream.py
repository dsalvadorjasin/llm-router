import logging
from itertools import count

import httpx

from .config import upstream_urls

log = logging.getLogger("app.upstream")

_RETRIABLE_ERRORS = (httpx.TransportError,)


class UpstreamPool:
    """Round-robin pool that fails over to the next distinct replica.

    A request is retried on the next replica when an attempt times out, hits a
    connection/transport error, returns a 5xx, or returns an unparseable body.
    At most ``max_attempts`` distinct replicas are tried (default: all of them).
    When every attempt fails, a JSON 503 is returned if no replica produced a
    response, otherwise a JSON 502. ``forward`` never raises for upstream failures.
    """

    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 timeout: httpx.Timeout | float | None = None,
                 max_attempts: int | None = None):
        self._urls = list(urls or upstream_urls())
        if not self._urls:
            raise ValueError("at least one upstream URL is required")
        n = len(self._urls)
        self._max_attempts = n if max_attempts is None else max(1, min(max_attempts, n))
        self._next = count()
        self._client = httpx.AsyncClient(timeout=timeout, transport=transport)

    async def forward(self, payload: dict) -> tuple[int, dict]:
        start = next(self._next)
        responded = False
        last_error = "no attempts made"
        for attempt in range(self._max_attempts):
            url = self._urls[(start + attempt) % len(self._urls)]
            try:
                resp = await self._client.post(f"{url}/v1/completions", json=payload)
            except _RETRIABLE_ERRORS as exc:
                last_error = f"{url}: {type(exc).__name__}"
                log.warning("upstream attempt %d failed: %s", attempt + 1, last_error)
                continue

            responded = True
            body = _parse_body(resp)
            if resp.status_code >= 500 or (body is None and resp.status_code < 400):
                last_error = f"{url}: status {resp.status_code}" + (
                    "" if body is not None else " with invalid body")
                log.warning("upstream attempt %d failed: %s", attempt + 1, last_error)
                continue
            if body is None:
                return 502, {"detail": "invalid upstream response"}
            return resp.status_code, body

        if responded:
            return 502, {"detail": "upstream error", "last_error": last_error}
        return 503, {"detail": "all upstreams unavailable", "last_error": last_error}

    async def aclose(self) -> None:
        await self._client.aclose()


def _parse_body(resp: httpx.Response) -> dict | None:
    try:
        body = resp.json()
    except ValueError:
        return None
    return body if isinstance(body, dict) else None
