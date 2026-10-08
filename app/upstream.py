import asyncio
import time
from collections import deque
from contextvars import ContextVar
from itertools import cycle

import httpx

from . import config
from .config import upstream_urls

# Request-scoped log fields: the logging middleware puts a fresh dict here before
# calling the endpoint and forward() fills it in (the endpoint's task context is a
# copy, but it shares the same dict object).
upstream_log_fields: ContextVar[dict | None] = ContextVar("upstream_log_fields", default=None)

_ADAPTIVE_MIN_SAMPLES = 20
_RETRY_RATIO_SLACK = 0.2


class _Health:
    """Time-decayed failure ratio of one upstream. Only orders retries; never excludes."""

    def __init__(self, halflife_s: float):
        self._halflife_s = halflife_s
        self._fail = 0.0
        self._total = 0.0
        self._stamp: float | None = None

    def _decay(self, now: float) -> None:
        if self._stamp is not None and self._halflife_s > 0:
            factor = 0.5 ** ((now - self._stamp) / self._halflife_s)
            self._fail *= factor
            self._total *= factor
        self._stamp = now

    def record(self, ok: bool, now: float) -> None:
        self._decay(now)
        self._total += 1.0
        if not ok:
            self._fail += 1.0

    def failure_ratio(self, now: float) -> float:
        self._decay(now)
        return self._fail / self._total if self._total > 1e-9 else 0.0


def _body(resp: httpx.Response) -> dict:
    try:
        body = resp.json()
    except ValueError:
        return {"detail": resp.text or "upstream error"}
    return body if isinstance(body, dict) else {"detail": body}


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._url_list = list(urls or upstream_urls())
        self._order = cycle(range(len(self._url_list)))
        self._max_timeout_s = config.attempt_timeout_ms() / 1000
        self._min_timeout_s = min(config.min_attempt_timeout_ms() / 1000, self._max_timeout_s)
        self._budget_s = config.request_budget_ms() / 1000
        self._max_attempts = config.max_attempts()
        self._adaptive = config.adaptive_timeout()
        self._quantile = config.adaptive_timeout_quantile()
        self._multiplier = config.adaptive_timeout_multiplier()
        self._latencies: deque[float] = deque(maxlen=200)
        halflife_s = config.failure_halflife_ms() / 1000
        self._health = [_Health(halflife_s) for _ in self._url_list]
        self._suspect_ratio = config.suspect_failure_ratio()
        self._suspect_factor = min(1.0, config.suspect_timeout_factor())
        self._probe_every = config.suspect_probe_every()
        self._suspect_attempts = [0] * len(self._url_list)
        # Per-attempt timeouts are enforced with asyncio.wait_for; the client
        # timeout is only a backstop just past the request budget.
        client_timeout = httpx.Timeout(max(self._budget_s, self._max_timeout_s) + 1.0)
        self._client = httpx.AsyncClient(timeout=client_timeout, transport=transport)

    def attempt_timeout(self) -> float:
        """Current per-attempt timeout in seconds (before budget clamping)."""
        if not self._adaptive or len(self._latencies) < _ADAPTIVE_MIN_SAMPLES:
            return self._max_timeout_s
        samples = sorted(self._latencies)
        q = samples[min(len(samples) - 1, int(self._quantile * len(samples)))]
        return min(self._max_timeout_s, max(self._min_timeout_s, q * self._multiplier))

    def _suspect_timeout(self, idx: int, timeout: float) -> float | None:
        """Short timeout for an upstream that has mostly been failing, or None.

        Every Nth attempt to it still gets the full timeout so a recovered
        upstream is noticed; short-timeout misses are inconclusive and are not
        recorded as failures.
        """
        if self._suspect_factor >= 1.0:
            return None
        if self._health[idx].failure_ratio(time.monotonic()) < self._suspect_ratio:
            return None
        self._suspect_attempts[idx] += 1
        if self._suspect_attempts[idx] % self._probe_every == 0:
            return None
        return timeout * self._suspect_factor

    def _pick_retry(self, failed: int, tried: list[int]) -> int:
        n = len(self._url_list)
        if n == 1:
            return failed
        now = time.monotonic()
        candidates = [(failed + step) % n for step in range(1, n)]
        ratios = {i: self._health[i].failure_ratio(now) for i in candidates}
        best = min(ratios.values())
        # Next in round-robin order, preferring upstreams that have not been
        # failing much more than the best candidate, then ones not yet tried.
        return min(
            candidates,
            key=lambda i: (ratios[i] > best + _RETRY_RATIO_SLACK, i in tried, candidates.index(i)),
        )

    async def forward(self, payload: dict) -> tuple[int, dict]:
        fields = upstream_log_fields.get()
        if fields is not None:
            fields["hedged"] = False
        deadline = time.monotonic() + self._budget_s
        idx = next(self._order)
        tried: list[int] = []
        failure: tuple[int, dict, str | None] | None = None

        for attempt in range(self._max_attempts):
            if attempt:
                idx = self._pick_retry(idx, tried)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            is_last = attempt == self._max_attempts - 1
            timeout = remaining if is_last else min(self.attempt_timeout(), remaining)
            short = None if is_last else self._suspect_timeout(idx, timeout)
            url = self._url_list[idx]
            tried.append(idx)
            start = time.monotonic()
            try:
                resp = await asyncio.wait_for(
                    self._client.post(f"{url}/v1/completions", json=payload), short or timeout
                )
            except (asyncio.TimeoutError, httpx.TimeoutException):
                if short is None:
                    self._health[idx].record(False, time.monotonic())
                failure = (504, {"detail": "upstream timeout"}, None)
                continue
            except httpx.TransportError as exc:
                self._health[idx].record(False, time.monotonic())
                failure = (502, {"detail": f"upstream unavailable: {type(exc).__name__}"}, None)
                continue
            now = time.monotonic()
            if resp.status_code >= 500:
                self._health[idx].record(False, now)
                failure = (resp.status_code, _body(resp), url)
                continue
            self._health[idx].record(True, now)
            if resp.status_code < 300:
                self._latencies.append(now - start)
            if fields is not None:
                fields["upstream_url"] = url
                fields["attempts"] = len(tried)
            return resp.status_code, _body(resp)

        status, body, url = failure or (504, {"detail": "request budget exhausted"}, None)
        if fields is not None:
            fields["upstream_url"] = url or "-"
            fields["attempts"] = len(tried)
        return status, body

    async def aclose(self) -> None:
        await self._client.aclose()
