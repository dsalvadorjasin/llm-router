import logging
import time
from collections.abc import Callable

import httpx

from .config import upstream_timeout, upstream_urls

log = logging.getLogger("app.upstream")

FAILURE_COOLDOWN_S = 1.0

_RETRIABLE_ERRORS = (httpx.TransportError,)

_TIMEOUT_DETAIL = {
    httpx.ConnectTimeout: "upstream connect timeout",
    httpx.ReadTimeout: "upstream read timeout",
    httpx.WriteTimeout: "upstream write timeout",
    httpx.PoolTimeout: "upstream pool timeout",
}


class UpstreamPool:
    """Load-aware pool that fails over to the next distinct replica.

    Each request starts on the upstream with the fewest in-flight requests;
    ties are broken round-robin so an idle fleet is still spread evenly.
    Upstreams that recently failed (transport error or 5xx) are skipped for
    ``failure_cooldown_s`` unless every upstream is cooling down, in which case
    all of them are eligible again so the fleet is never starved.

    A request is retried on another replica when an attempt times out, hits a
    connection/transport error, returns a 5xx, or returns an unparseable body.
    At most ``max_attempts`` distinct replicas are tried (default: all of them).
    When every attempt fails, a JSON 504 is returned if every attempt timed
    out, a JSON 503 if no replica produced a response, otherwise a JSON 502.
    ``forward`` never raises for upstream failures.
    """

    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 timeout: httpx.Timeout | None = None,
                 max_attempts: int | None = None,
                 failure_cooldown_s: float = FAILURE_COOLDOWN_S,
                 clock: Callable[[], float] = time.monotonic):
        self._urls = list(urls or upstream_urls())
        if not self._urls:
            raise ValueError("at least one upstream URL is required")
        n = len(self._urls)
        self._max_attempts = n if max_attempts is None else max(1, min(max_attempts, n))
        self._inflight = [0] * n
        self._failed_until = [float("-inf")] * n
        self._next = 0
        self._cooldown = failure_cooldown_s
        self._clock = clock
        self._client = httpx.AsyncClient(timeout=timeout or upstream_timeout(),
                                         transport=transport)

    def inflight(self) -> dict[str, int]:
        return dict(zip(self._urls, self._inflight))

    def _pick(self, exclude: set[int]) -> int:
        n = len(self._urls)
        now = self._clock()
        untried = [i for i in range(n) if i not in exclude]
        candidates = [i for i in untried if self._failed_until[i] <= now] or untried
        start = self._next
        best = min(candidates, key=lambda i: (self._inflight[i], (i - start) % n))
        if not exclude:
            self._next = (best + 1) % n
        return best

    async def forward(self, payload: dict) -> tuple[int, dict]:
        tried: set[int] = set()
        responded = False
        timeouts: list[str] = []
        last_error = "no attempts made"
        for attempt in range(self._max_attempts):
            idx = self._pick(tried)
            tried.add(idx)
            url = self._urls[idx]
            self._inflight[idx] += 1
            try:
                resp = await self._client.post(f"{url}/v1/completions", json=payload)
            except _RETRIABLE_ERRORS as exc:
                self._mark_failed(idx)
                if isinstance(exc, httpx.TimeoutException):
                    timeouts.append(_TIMEOUT_DETAIL.get(type(exc), "upstream timeout"))
                last_error = f"{url}: {type(exc).__name__}"
                log.warning("upstream attempt %d failed: %s", attempt + 1, last_error)
                continue
            finally:
                self._inflight[idx] -= 1

            responded = True
            body = _parse_body(resp)
            if resp.status_code >= 500:
                self._mark_failed(idx)
            if resp.status_code >= 500 or (body is None and resp.status_code < 400):
                last_error = f"{url}: status {resp.status_code}" + (
                    "" if body is not None else " with invalid body")
                log.warning("upstream attempt %d failed: %s", attempt + 1, last_error)
                continue
            if body is None:
                return 502, {"detail": "invalid upstream response"}
            return resp.status_code, body

        if timeouts and len(timeouts) == self._max_attempts:
            return 504, {"detail": timeouts[-1]}
        if responded:
            return 502, {"detail": "upstream error", "last_error": last_error}
        return 503, {"detail": "all upstreams unavailable", "last_error": last_error}

    def _mark_failed(self, idx: int) -> None:
        self._failed_until[idx] = self._clock() + self._cooldown

    async def aclose(self) -> None:
        await self._client.aclose()


def _parse_body(resp: httpx.Response) -> dict | None:
    try:
        body = resp.json()
    except ValueError:
        return None
    return body if isinstance(body, dict) else None
