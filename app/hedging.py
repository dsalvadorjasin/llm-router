"""Hedged forwarding across upstream replicas.

The primary attempt goes to the next replica in round-robin order (replicas that
are currently much slower than the best one are moved to the back). If no valid
response arrives within the hedge delay, a duplicate is sent to the next replica
in the plan; errors fail over immediately. The first valid response wins and the
remaining in-flight attempts are cancelled.

The hedge delay is a quantile of recent attempt latencies on healthy replicas
(or a fixed value). Slow replicas are re-tried as primary once their stats go
stale, so a replica that recovers is picked up again.

When a LatencyAwareRouter is passed in, it replaces the built-in replica health
logic: the router ranks replicas (primary and hedge targets), and every attempt
outcome is fed back into it.
"""
import asyncio
import re
import time
from collections import deque
from dataclasses import dataclass

import httpx

_SIGNATURE = re.compile(r"^[0-9a-f]{64}$")


@dataclass
class HedgeSettings:
    fixed_delay_ms: float | None = None
    initial_delay_ms: float = 200.0
    min_delay_ms: float = 50.0
    max_delay_ms: float = 400.0
    delay_quantile: float = 0.5
    max_hedged_attempts: int = 3
    attempt_timeout_s: float = 5.0
    slow_factor: float = 2.0
    probe_interval_s: float = 2.0
    ewma_alpha: float = 0.2
    min_samples: int = 20
    window: int = 300


@dataclass
class _ReplicaStats:
    ewma_ms: float | None = None
    last_sample: float = 0.0


@dataclass
class _Attempt:
    url: str
    started: float
    task: asyncio.Task
    done: bool = False


def is_valid(status: int, body) -> bool:
    if status != 200 or not isinstance(body, dict):
        return False
    completion = body.get("completion")
    if not isinstance(completion, str) or not completion:
        return False
    sig = body.get("signature")
    return sig is None or (isinstance(sig, str) and bool(_SIGNATURE.match(sig)))


class Hedger:
    def __init__(self, urls: list[str], client: httpx.AsyncClient,
                 settings: HedgeSettings | None = None, clock=time.monotonic,
                 router=None):
        self._urls = list(urls)
        self._router = router
        self._client = client
        self._s = settings or HedgeSettings()
        self._clock = clock
        self._cursor = 0
        self._stats = {u: _ReplicaStats() for u in self._urls}
        self._samples: deque[float] = deque(maxlen=self._s.window)

    # ---- replica health -------------------------------------------------
    def _slow(self, now: float) -> set[str]:
        fresh = {u: st.ewma_ms for u, st in self._stats.items()
                 if st.ewma_ms is not None and now - st.last_sample <= self._s.probe_interval_s}
        if len(fresh) < 2:
            return set()
        best = min(fresh.values())
        limit = best * self._s.slow_factor + 50.0
        return {u for u, ms in fresh.items() if ms > limit}

    def _record(self, url: str, elapsed_ms: float, censored: bool) -> None:
        if self._router is not None:
            idx = self._router.index(url)
            if censored:
                self._router.record_censored(idx, elapsed_ms)
            else:
                self._router.record_success(idx, elapsed_ms)
            if self._router.is_fast(url):
                self._samples.append(elapsed_ms)
            return
        st = self._stats[url]
        if st.ewma_ms is None:
            st.ewma_ms = elapsed_ms
        elif censored:
            # a cancelled attempt only tells us the latency was at least elapsed_ms
            if elapsed_ms > st.ewma_ms:
                st.ewma_ms += self._s.ewma_alpha * (elapsed_ms - st.ewma_ms)
        elif elapsed_ms * self._s.slow_factor < st.ewma_ms:
            # clearly faster than its estimate (e.g. recovered): adopt the new value
            st.ewma_ms = elapsed_ms
        else:
            st.ewma_ms += self._s.ewma_alpha * (elapsed_ms - st.ewma_ms)
        st.last_sample = self._clock()
        if url not in self._slow(st.last_sample):
            self._samples.append(elapsed_ms)

    def _record_failure(self, url: str, elapsed_ms: float = 0.0) -> None:
        if self._router is not None:
            self._router.record_failure(self._router.index(url), elapsed_ms,
                                        self._router.attempt_timeout_ms())
            return
        st = self._stats[url]
        penalty_ms = self._s.attempt_timeout_s * 1000
        st.ewma_ms = penalty_ms if st.ewma_ms is None else max(st.ewma_ms, penalty_ms)
        st.last_sample = self._clock()

    def hedge_delay_s(self) -> float:
        s = self._s
        if s.fixed_delay_ms is not None:
            return s.fixed_delay_ms / 1000
        if len(self._samples) < s.min_samples:
            return s.initial_delay_ms / 1000
        ordered = sorted(self._samples)
        idx = min(len(ordered) - 1, int(s.delay_quantile * len(ordered)))
        return min(s.max_delay_ms, max(s.min_delay_ms, ordered[idx])) / 1000

    def plan(self) -> tuple[list[str], int]:
        """Replicas to try in order, and how many of them may be hedged on a timer."""
        n = len(self._urls)
        start = self._cursor % n
        self._cursor += 1
        if self._router is not None:
            healthy, degraded = self._router.rank()
        else:
            rotation = self._urls[start:] + self._urls[:start]
            slow = self._slow(self._clock())
            healthy = [u for u in rotation if u not in slow]
            degraded = [u for u in rotation if u in slow]
        if not healthy:
            healthy, degraded = degraded, []
        hedged = [healthy[i % len(healthy)] for i in range(max(1, self._s.max_hedged_attempts))]
        return hedged + degraded + healthy, len(hedged)

    # ---- forwarding -----------------------------------------------------
    async def _post(self, url: str, payload: dict) -> tuple[int, dict]:
        replica = self._router._replicas[self._router.index(url)] if self._router else None
        if replica is not None:
            replica.outstanding += 1
        try:
            resp = await asyncio.wait_for(
                self._client.post(f"{url}/v1/completions", json=payload,
                                  timeout=self._s.attempt_timeout_s),
                timeout=self._s.attempt_timeout_s)
        finally:
            if replica is not None:
                replica.outstanding -= 1
        try:
            body = resp.json()
        except ValueError:
            body = {"detail": "invalid upstream response"}
        return resp.status_code, body

    async def forward(self, payload: dict) -> tuple[int, dict]:
        plan, hedge_limit = self.plan()
        delay = self.hedge_delay_s()
        attempts: dict[asyncio.Task, _Attempt] = {}
        next_idx = 0
        last_launch = 0.0
        last: tuple[int, dict] | None = None

        def launch() -> None:
            nonlocal next_idx, last_launch
            url = plan[next_idx]
            next_idx += 1
            last_launch = self._clock()
            task = asyncio.ensure_future(self._post(url, payload))
            attempts[task] = _Attempt(url, last_launch, task)

        launch()
        try:
            while True:
                pending = [t for t, a in attempts.items() if not a.done]
                if not pending:
                    break
                timeout = None
                if next_idx < hedge_limit:
                    timeout = max(0.0, last_launch + delay - self._clock())
                done, _ = await asyncio.wait(pending, timeout=timeout,
                                             return_when=asyncio.FIRST_COMPLETED)
                if not done:
                    launch()
                    continue
                for task in done:
                    a = attempts[task]
                    a.done = True
                    elapsed_ms = (self._clock() - a.started) * 1000
                    try:
                        status, body = task.result()
                    except Exception as exc:
                        status, body = 502, {"detail": f"upstream error: {type(exc).__name__}"}
                    if is_valid(status, body):
                        self._record(a.url, elapsed_ms, censored=False)
                        return status, body
                    self._record_failure(a.url, elapsed_ms)
                    last = (status, body)
                    if next_idx < len(plan):
                        launch()
        finally:
            now = self._clock()
            for task, a in attempts.items():
                if not a.done:
                    task.cancel()
                    self._record(a.url, (now - a.started) * 1000, censored=True)
        return last if last is not None else (502, {"detail": "no upstream available"})
