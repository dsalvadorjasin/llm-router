import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from .config import upstream_timeout, upstream_urls

log = logging.getLogger("app.upstream")

FAILURE_COOLDOWN_S = 1.0
LATENCY_FLOOR_S = 0.05
LATENCY_EWMA_ALPHA = 0.3

_RETRIABLE_ERRORS = (httpx.RequestError,)

_TIMEOUT_DETAIL = {
    httpx.ConnectTimeout: "upstream connect timeout",
    httpx.ReadTimeout: "upstream read timeout",
    httpx.WriteTimeout: "upstream write timeout",
    httpx.PoolTimeout: "upstream pool timeout",
}


class HedgeBudget:
    """Token bucket bounding hedged attempts to a fraction of requests.

    Every request deposits ``ratio`` tokens (capped at ``burst``); every hedge
    spends one, so over time hedges are at most ``ratio`` x requests plus a
    one-off burst.
    """

    def __init__(self, ratio: float, burst: float):
        self._ratio = ratio
        self._burst = burst
        self._tokens = burst

    def deposit(self) -> None:
        self._tokens = min(self._burst, self._tokens + self._ratio)

    def try_spend(self) -> bool:
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False


@dataclass
class _Outcome:
    status: int | None = None
    body: dict | None = None
    error: str | None = None
    timeout_detail: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


class UpstreamPool:
    """Latency- and load-aware pool with failover and optional hedging.

    Each attempt goes to the upstream with the lowest expected cost,
    ``(in_flight + 1) * max(latency_ewma, latency_floor_s)``, excluding any
    upstream that already failed for this request; ties are broken
    round-robin so an idle or uniform fleet is still spread evenly. Upstreams
    that recently failed (transport error or 5xx) are skipped for
    ``failure_cooldown_s`` unless every remaining upstream is cooling down.

    A request is retried on another replica when an attempt times out, hits a
    connection/transport error, returns a 5xx, or returns an unparseable body.
    With ``hedge_delay_s`` set, an attempt that has not answered within that
    delay is hedged: another replica is tried while the first keeps running and
    the first valid answer wins (the others are cancelled). Because the
    in-flight attempt counts towards its replica's cost, a hedge normally goes
    to another replica, but may reuse a fast replica rather than a much slower
    one. Hedges are bounded by a :class:`HedgeBudget`; failover after an error
    is not.

    At most ``max_attempts`` attempts are made per request (default: the
    number of upstreams). When every attempt fails, a JSON 503 is returned if no
    replica produced a response, otherwise
    a JSON 502. ``forward`` never raises for upstream failures.
    """

    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 timeout: httpx.Timeout | None = None,
                 max_attempts: int | None = None,
                 failure_cooldown_s: float = FAILURE_COOLDOWN_S,
                 clock: Callable[[], float] = time.monotonic,
                 hedge_delay_s: float | None = None,
                 hedge_budget: HedgeBudget | None = None,
                 latency_floor_s: float = LATENCY_FLOOR_S,
                 latency_alpha: float = LATENCY_EWMA_ALPHA):
        self._urls = list(urls or upstream_urls())
        if not self._urls:
            raise ValueError("at least one upstream URL is required")
        n = len(self._urls)
        self._max_attempts = n if max_attempts is None else max(1, min(max_attempts, n))
        self._inflight = [0] * n
        self._failed_until = [float("-inf")] * n
        self._latency = [0.0] * n
        self._next = 0
        self._cooldown = failure_cooldown_s
        self._clock = clock
        self._hedge_delay = hedge_delay_s if hedge_delay_s and hedge_delay_s > 0 else None
        self._budget = hedge_budget or HedgeBudget(ratio=0.2, burst=10.0)
        self._floor = latency_floor_s
        self._alpha = latency_alpha
        self._stats = {k: 0 for k in (
            "requests", "attempts", "hedges", "hedges_denied", "hedge_wins",
            "failovers", "failed_attempts", "cancelled_attempts", "exhausted")}
        self._client = httpx.AsyncClient(timeout=timeout or upstream_timeout(),
                                         transport=transport)

    def inflight(self) -> dict[str, int]:
        return dict(zip(self._urls, self._inflight))

    def stats(self) -> dict:
        return {
            **self._stats,
            "hedge_delay_ms": None if self._hedge_delay is None else self._hedge_delay * 1000,
            "upstreams": [
                {"url": url, "in_flight": self._inflight[i],
                 "latency_ewma_ms": round(self._latency[i] * 1000, 1),
                 "cooling_down": self._failed_until[i] > self._clock()}
                for i, url in enumerate(self._urls)
            ],
        }

    def _cost(self, i: int) -> float:
        return (self._inflight[i] + 1) * max(self._latency[i], self._floor)

    def _pick(self, exclude: set[int], primary: bool) -> int | None:
        n = len(self._urls)
        now = self._clock()
        allowed = [i for i in range(n) if i not in exclude]
        if not allowed:
            return None
        candidates = [i for i in allowed if self._failed_until[i] <= now] or allowed
        start = self._next
        best = min(candidates, key=lambda i: (self._cost(i), (i - start) % n))
        if primary:
            self._next = (best + 1) % n
        return best

    def _observe_latency(self, idx: int, seconds: float) -> None:
        prev = self._latency[idx]
        self._latency[idx] = seconds if prev == 0.0 else prev + self._alpha * (seconds - prev)

    async def _attempt(self, idx: int, payload: dict) -> _Outcome:
        url = self._urls[idx]
        self._stats["attempts"] += 1
        self._inflight[idx] += 1
        started = self._clock()
        try:
            resp = await self._client.post(f"{url}/v1/completions", json=payload)
        except _RETRIABLE_ERRORS as exc:
            self._mark_failed(idx)
            detail = (_TIMEOUT_DETAIL.get(type(exc), "upstream timeout")
                      if isinstance(exc, httpx.TimeoutException) else None)
            return _Outcome(error=f"{url}: {type(exc).__name__}", timeout_detail=detail)
        except asyncio.CancelledError:
            # A cancelled (losing) attempt took at least this long: only let it
            # raise the estimate, never lower it.
            elapsed = self._clock() - started
            if elapsed > self._latency[idx]:
                self._observe_latency(idx, elapsed)
            self._stats["cancelled_attempts"] += 1
            raise
        finally:
            self._inflight[idx] -= 1

        body = _parse_body(resp)
        if resp.status_code >= 500:
            self._mark_failed(idx)
        if resp.status_code >= 500 or (body is None and resp.status_code < 400):
            return _Outcome(status=resp.status_code, error=f"{url}: status {resp.status_code}" + (
                "" if body is not None else " with invalid body"))
        self._observe_latency(idx, self._clock() - started)
        if body is None:
            return _Outcome(status=502, body={"detail": "invalid upstream response"})
        return _Outcome(status=resp.status_code, body=body)

    async def forward(self, payload: dict) -> tuple[int, dict]:
        self._stats["requests"] += 1
        self._budget.deposit()
        failed: set[int] = set()
        attempts = 0
        pending: dict[asyncio.Task[_Outcome], int] = {}
        hedges: set[asyncio.Task[_Outcome]] = set()
        failures: list[_Outcome] = []
        hedging = self._hedge_delay is not None

        def launch() -> asyncio.Task[_Outcome] | None:
            nonlocal attempts
            idx = self._pick(failed, primary=attempts == 0)
            if idx is None:
                return None
            attempts += 1
            task = asyncio.ensure_future(self._attempt(idx, payload))
            pending[task] = idx
            return task

        try:
            launch()
            while pending:
                can_hedge = (hedging and attempts < self._max_attempts
                             and len(failed) < len(self._urls))
                done, _ = await asyncio.wait(
                    pending, timeout=self._hedge_delay if can_hedge else None,
                    return_when=asyncio.FIRST_COMPLETED)
                if not done:
                    if self._budget.try_spend() and (hedge := launch()) is not None:
                        self._stats["hedges"] += 1
                        hedges.add(hedge)
                    else:
                        self._stats["hedges_denied"] += 1
                        hedging = False
                    continue
                for task in done:
                    failed_idx = pending.pop(task)
                    outcome = task.result()
                    if outcome.ok:
                        if task in hedges:
                            self._stats["hedge_wins"] += 1
                        return outcome.status, outcome.body
                    failures.append(outcome)
                    failed.add(failed_idx)
                    self._stats["failed_attempts"] += 1
                    log.warning("upstream attempt failed (%d/%d): %s",
                                len(failures), self._max_attempts, outcome.error)
                    if attempts < self._max_attempts and launch() is not None:
                        self._stats["failovers"] += 1
        finally:
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        self._stats["exhausted"] += 1
        last_error = failures[-1].error if failures else "no attempts made"
        log.warning("upstream attempts exhausted after %d tries: %s", len(failures), last_error)
        timeouts = [f.timeout_detail for f in failures if f.timeout_detail]
        if timeouts and len(timeouts) == len(failures):
            return 503, {"detail": timeouts[-1]}
        if any(f.status is not None for f in failures):
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
