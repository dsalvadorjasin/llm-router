import time
from collections.abc import Callable

import httpx

from .config import upstream_urls

FAILURE_COOLDOWN_S = 1.0


class UpstreamPool:
    """Routes each request to the upstream with the fewest in-flight requests.

    Ties are broken round-robin so an idle fleet is still spread evenly.
    Upstreams that recently failed (transport error or 5xx) are skipped for
    ``failure_cooldown_s`` unless every upstream is cooling down, in which case
    all of them are eligible again so the fleet is never starved.
    """

    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 failure_cooldown_s: float = FAILURE_COOLDOWN_S,
                 clock: Callable[[], float] = time.monotonic):
        self._urls = list(urls or upstream_urls())
        if not self._urls:
            raise ValueError("UpstreamPool needs at least one upstream URL")
        self._inflight = [0] * len(self._urls)
        self._failed_until = [float("-inf")] * len(self._urls)
        self._next = 0
        self._cooldown = failure_cooldown_s
        self._clock = clock
        self._client = httpx.AsyncClient(timeout=None, transport=transport)

    def inflight(self) -> dict[str, int]:
        return dict(zip(self._urls, self._inflight))

    def _pick(self) -> int:
        n = len(self._urls)
        now = self._clock()
        candidates = [i for i in range(n) if self._failed_until[i] <= now] or range(n)
        start = self._next
        best = min(candidates, key=lambda i: (self._inflight[i], (i - start) % n))
        self._next = (best + 1) % n
        return best

    async def forward(self, payload: dict) -> tuple[int, dict]:
        idx = self._pick()
        url = self._urls[idx]
        self._inflight[idx] += 1
        try:
            resp = await self._client.post(f"{url}/v1/completions", json=payload)
        except httpx.HTTPError:
            self._mark_failed(idx)
            raise
        finally:
            self._inflight[idx] -= 1
        if resp.status_code >= 500:
            self._mark_failed(idx)
        return resp.status_code, resp.json()

    def _mark_failed(self, idx: int) -> None:
        self._failed_until[idx] = self._clock() + self._cooldown

    async def aclose(self) -> None:
        await self._client.aclose()
