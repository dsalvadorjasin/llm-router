"""Upstream pool: latency-ranked replica selection with hedged requests.

Each request goes to the fastest replica tier (round-robin within the tier).
If it hasn't answered within a hedge delay (~the fastest replica's median
latency), a backup attempt goes to the next best replica; the first non-5xx
answer wins and the other attempts are cancelled. 5xx and transport errors
fail over immediately. Set LLM_HEDGING=0 for plain round-robin.
"""
import asyncio
import logging
import statistics
import time
from collections import deque

import httpx

from .config import (
    hedge_delay_cap_s,
    hedge_max_attempts,
    hedging_enabled,
    request_timeout_s,
    upstream_urls,
)

logger = logging.getLogger("llm-router.upstream")

_WINDOW = 32
_MIN_HEDGE_DELAY_S = 0.05
_FAILURE_SAMPLE_S = 1.0
_STALE_AFTER_S = 5.0
_PROBE_EVERY = 50
_PROBE_TIMEOUT_S = 10.0


class _Replica:
    def __init__(self, url: str):
        self.url = url
        # Completed latencies, plus lower bounds from attempts cancelled after
        # running longer than the hedge delay.
        self.samples: deque[float] = deque(maxlen=_WINDOW)
        self.pending: set[float] = set()
        self.last_completed = 0.0

    def score(self, now: float) -> float:
        if self.samples:
            return statistics.median(self.samples)
        # No history yet: an attempt still pending is at least that slow.
        return now - min(self.pending) if self.pending else 0.0


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._replicas = [_Replica(u) for u in (urls or upstream_urls())]
        self._rr = 0
        self._requests = 0
        self._background: set[asyncio.Task] = set()
        self._client = httpx.AsyncClient(timeout=None, transport=transport)

    # -- selection -------------------------------------------------------

    def _ranked(self, exclude: set[_Replica]) -> list[_Replica]:
        now = time.monotonic()
        return sorted((r for r in self._replicas if r not in exclude),
                      key=lambda r: r.score(now))

    def _fast_tier(self, candidates: list[_Replica]) -> list[_Replica]:
        now = time.monotonic()
        best = candidates[0].score(now)
        limit = best * 1.5 + 0.02
        return [r for r in candidates if r.score(now) <= limit]

    def _next_round_robin(self, tier: list[_Replica]) -> _Replica:
        n = len(self._replicas)
        for i in range(n):
            idx = (self._rr + i) % n
            if self._replicas[idx] in tier:
                self._rr = idx + 1
                return self._replicas[idx]
        return tier[0]

    def _pick(self, tried: list[_Replica], failed: set[_Replica]) -> _Replica | None:
        candidates = self._ranked(failed)
        if not candidates:
            return None
        tier = self._fast_tier(candidates)
        untried = [r for r in tier if r not in tried]
        if not tried:
            return self._next_round_robin(tier)
        # Prefer a fresh fast replica; otherwise retry the best one rather than
        # falling through to a known-slow replica.
        return untried[0] if untried else candidates[0]

    def _hedge_delay(self) -> float:
        now = time.monotonic()
        scored = [r.score(now) for r in self._replicas if r.samples]
        cap = hedge_delay_cap_s()
        if not scored:
            return cap
        return min(cap, max(_MIN_HEDGE_DELAY_S, min(scored)))

    # -- attempts --------------------------------------------------------

    async def _post(self, url: str, payload: dict) -> tuple[int, dict]:
        resp = await self._client.post(f"{url}/v1/completions", json=payload)
        return resp.status_code, resp.json()

    async def _attempt(self, replica: _Replica, payload: dict,
                       informative_after: float = 0.0) -> tuple[int, dict]:
        start = time.monotonic()
        replica.pending.add(start)
        try:
            status, body = await self._post(replica.url, payload)
        except asyncio.CancelledError:
            # A cancelled attempt is only a lower bound. Below the hedge delay
            # it says nothing about the replica, and recording it would make a
            # slow replica look fast.
            elapsed = time.monotonic() - start
            if elapsed >= informative_after:
                replica.samples.append(elapsed)
            raise
        except (httpx.HTTPError, ValueError):
            replica.samples.append(max(_FAILURE_SAMPLE_S, time.monotonic() - start))
            raise
        finally:
            replica.pending.discard(start)
        elapsed = time.monotonic() - start
        if status >= 500:
            replica.samples.append(max(_FAILURE_SAMPLE_S, elapsed))
        else:
            replica.samples.append(elapsed)
            replica.last_completed = time.monotonic()
        return status, body

    def _maybe_probe(self, payload: dict) -> None:
        """Refresh stats of replicas that no longer get traffic, so a replica
        that recovers can rejoin the fast tier."""
        self._requests += 1
        if self._requests % _PROBE_EVERY:
            return
        now = time.monotonic()
        stale = [r for r in self._replicas
                 if not r.pending and now - r.last_completed > _STALE_AFTER_S]
        if not stale:
            return
        replica = min(stale, key=lambda r: r.last_completed)
        task = asyncio.create_task(
            asyncio.wait_for(self._attempt(replica, payload), _PROBE_TIMEOUT_S))
        self._background.add(task)
        task.add_done_callback(self._probe_done)

    def _probe_done(self, task: asyncio.Task) -> None:
        self._background.discard(task)
        if not task.cancelled():
            task.exception()  # probes are best-effort; don't log "never retrieved"

    async def forward(self, payload: dict) -> tuple[int, dict]:
        if not hedging_enabled():
            replica = self._replicas[self._rr % len(self._replicas)]
            self._rr += 1
            return await self._post(replica.url, payload)
        self._maybe_probe(payload)
        try:
            return await asyncio.wait_for(self._hedged(payload), request_timeout_s())
        except TimeoutError:
            return 504, {"detail": "upstream timeout"}

    async def _hedged(self, payload: dict) -> tuple[int, dict]:
        max_attempts = hedge_max_attempts()
        tried: list[_Replica] = []
        failed: set[_Replica] = set()
        inflight: dict[asyncio.Task, _Replica] = {}
        last_error: tuple[int, dict] = (502, {"detail": "no upstream available"})

        def launch() -> bool:
            replica = self._pick(tried, failed)
            if replica is None:
                return False
            tried.append(replica)
            attempt = self._attempt(replica, payload, informative_after=self._hedge_delay())
            inflight[asyncio.create_task(attempt)] = replica
            return True

        launch()
        try:
            while inflight:
                can_hedge = len(tried) < max_attempts
                done, _ = await asyncio.wait(
                    inflight, timeout=self._hedge_delay() if can_hedge else None,
                    return_when=asyncio.FIRST_COMPLETED)
                if not done:
                    launch()
                    continue
                for task in done:
                    replica = inflight.pop(task)
                    try:
                        status, body = task.result()
                    except (httpx.HTTPError, ValueError) as exc:
                        logger.warning("upstream %s failed: %r", replica.url, exc)
                        failed.add(replica)
                        last_error = (502, {"detail": "upstream unavailable"})
                        continue
                    if status < 500:
                        return status, body
                    failed.add(replica)
                    last_error = (status, body)
                if not inflight and len(tried) < max_attempts:
                    launch()
            return last_error
        finally:
            for task in inflight:
                task.cancel()

    async def aclose(self) -> None:
        for task in list(self._background):
            task.cancel()
        await self._client.aclose()
