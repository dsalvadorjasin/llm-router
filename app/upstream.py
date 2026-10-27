import asyncio
import logging
import statistics
import time
from collections import deque

import httpx

from .config import (hedge_delay_ms, hedging_enabled, upstream_cooldown_s, upstream_timeout_s,
                     upstream_urls)

logger = logging.getLogger("llm-router.upstream")

_FAILOVER_ATTEMPTS = 2
_HEDGE_ATTEMPTS = 3
_MAX_TRIES_PER_URL = 2
_SAMPLES = 32
_HEDGE_DELAY_MIN_S = 0.05
_HEDGE_DELAY_MAX_S = 0.25
_HEDGE_QUANTILE = 0.8
_PROBE_EVERY = 50


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 timeout_s: float | None = None,
                 cooldown_s: float | None = None,
                 hedging: bool | None = None):
        self._urls = list(urls or upstream_urls())
        self._next = 0
        self._cooldown_s = upstream_cooldown_s() if cooldown_s is None else cooldown_s
        self._hedging = hedging_enabled() if hedging is None else hedging
        fixed_ms = hedge_delay_ms()
        self._fixed_delay_s = fixed_ms / 1000 if fixed_ms is not None else None
        self._failed_at: dict[str, float] = {}
        self._latencies: dict[str, deque[float]] = {u: deque(maxlen=_SAMPLES) for u in self._urls}
        self._sampled_at: dict[str, float] = {}
        self._probes = 0
        self._timeout_s = upstream_timeout_s() if timeout_s is None else timeout_s
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(self._timeout_s),
                                         transport=transport)

    def _in_cooldown(self, url: str, now: float) -> bool:
        failed_at = self._failed_at.get(url)
        return failed_at is not None and now - failed_at < self._cooldown_s

    def _score(self, url: str) -> float | None:
        samples = self._latencies.get(url)
        return statistics.median(samples) if samples else None

    def _record(self, url: str, elapsed_s: float) -> None:
        self._latencies[url].append(elapsed_s)
        self._sampled_at[url] = time.monotonic()

    def _hedge_delay(self) -> float | None:
        if not self._hedging:
            return None
        if self._fixed_delay_s is not None:
            return self._fixed_delay_s
        # Hedge once a request is slower than ~80% of the best replica's recent
        # requests, so roughly one in five requests gets a second copy.
        quantiles = [
            sorted(samples)[int(_HEDGE_QUANTILE * (len(samples) - 1))]
            for samples in self._latencies.values()
            if samples
        ]
        if not quantiles:
            return _HEDGE_DELAY_MAX_S
        return min(max(min(quantiles), _HEDGE_DELAY_MIN_S), _HEDGE_DELAY_MAX_S)

    def _candidates(self) -> list[str]:
        """Attempt order: round-robin over the fast healthy URLs (each up to
        twice, so a hedge prefers a fast replica again over a slow one), then
        slower healthy ones, then URLs in cooldown (oldest failure first)."""
        now = time.monotonic()
        healthy = [u for u in self._urls if not self._in_cooldown(u, now)]
        cooling = sorted((u for u in self._urls if self._in_cooldown(u, now)),
                         key=self._failed_at.__getitem__)
        if not self._hedging:
            start = self._next % len(healthy) if healthy else 0
            self._next += 1
            return healthy[start:] + healthy[:start] + cooling
        known = [s for s in map(self._score, healthy) if s is not None]
        cutoff = 1.5 * min(known) + 0.02 if known else float("inf")
        fast = [u for u in healthy if (self._score(u) or 0.0) <= cutoff]
        slow = sorted((u for u in healthy if u not in fast), key=self._score)
        if fast:
            start = self._next % len(fast)
            fast = fast[start:] + fast[:start]
        self._next += 1
        lead: list[str] = []
        if slow and self._next % _PROBE_EVERY == 0:
            # Occasionally lead with a slow replica so its stats stay fresh
            # (the hedge covers its latency); rotate by least recently sampled.
            lead = [min(slow, key=lambda u: self._sampled_at.get(u, 0.0))]
            slow.remove(lead[0])
        return lead + (fast + fast + slow + slow + cooling + cooling)

    async def _attempt(self, url: str, payload: dict) -> tuple[int, dict]:
        try:
            # httpx timeouts are per phase (e.g. between body chunks);
            # this caps the whole attempt.
            async with asyncio.timeout(self._timeout_s):
                resp = await self._client.post(f"{url}/v1/completions", json=payload)
        except TimeoutError:
            raise httpx.ReadTimeout(f"upstream exceeded {self._timeout_s}s") from None
        return resp.status_code, resp.json()

    async def forward(self, payload: dict) -> tuple[int, dict]:
        """Send to the first candidate; if it hasn't answered after the hedge
        delay (or it failed), also send to the next one. First answer wins."""
        plan = self._candidates()
        max_attempts = _HEDGE_ATTEMPTS if self._hedging else _FAILOVER_ATTEMPTS
        if not self._hedging:
            plan = (plan * _MAX_TRIES_PER_URL)[:max_attempts]
        delay = self._hedge_delay()
        failed: set[str] = set()
        pending: dict[asyncio.Task, tuple[str, float]] = {}
        last_error: httpx.HTTPError | None = None
        fallback: tuple[int, dict] | None = None

        tried = 0
        failures = 0
        remaining = iter(plan)

        def next_url() -> str | None:
            # Failed attempts don't count, so failover still reaches the
            # remaining replicas after the hedges time out.
            if tried - failures >= max_attempts:
                return None
            return next((u for u in remaining if u not in failed), None)

        def launch_next(url: str) -> None:
            nonlocal tried
            tried += 1
            if tried > 1:
                logger.info("upstream hedge url=%s attempt=%d", url, tried)
            pending[asyncio.create_task(self._attempt(url, payload))] = (url, time.monotonic())

        launch_next(next_url())
        upcoming = next_url()
        try:
            while pending:
                done, _ = await asyncio.wait(
                    pending, timeout=delay if upcoming else None,
                    return_when=asyncio.FIRST_COMPLETED)
                if not done:
                    launch_next(upcoming)
                    upcoming = next_url()
                    continue
                for task in sorted(done, key=lambda t: t.exception() is not None):
                    url, start = pending.pop(task)
                    elapsed = time.monotonic() - start
                    exc = task.exception()
                    if exc is None:
                        status, body = task.result()
                        self._failed_at.pop(url, None)
                        self._record(url, elapsed)
                        logger.info("upstream url=%s status=%s duration_ms=%.1f attempts=%d",
                                    url, status, elapsed * 1000, tried)
                        if status >= 500 and pending:
                            # Another attempt is still in flight and may succeed.
                            fallback = (status, body)
                            continue
                        return status, body
                    if not isinstance(exc, httpx.TransportError):
                        if fallback is not None:
                            return fallback
                        raise exc
                    self._failed_at[url] = time.monotonic()
                    failed.add(url)
                    failures += 1
                    logger.warning(
                        "upstream url=%s error=%s duration_ms=%.1f cooldown_s=%s",
                        url, type(exc).__name__, elapsed * 1000, self._cooldown_s,
                    )
                    last_error = exc
                    if upcoming is None or upcoming in failed:
                        upcoming = next_url()
                    if upcoming and not pending:
                        launch_next(upcoming)
                        upcoming = next_url()
        finally:
            await self._cancel_losers(pending, delay)
        if fallback is not None:
            return fallback
        assert last_error is not None
        raise last_error

    async def _cancel_losers(self, pending: dict[asyncio.Task, tuple[str, float]],
                             delay: float | None) -> None:
        now = time.monotonic()
        for task, (url, start) in pending.items():
            task.cancel()
            # A loser's elapsed time is a lower bound on its latency; only keep
            # it when it outlasted the hedge delay, otherwise a slow replica
            # used as a late hedge would look fast.
            if delay is not None and now - start >= delay:
                self._record(url, now - start)
        await asyncio.gather(*pending, return_exceptions=True)

    async def aclose(self) -> None:
        await self._client.aclose()
