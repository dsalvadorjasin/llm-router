"""Upstream replica pool with hedged requests.

A request goes to the replica with the best score (estimated latency x
in-flight load). If it hasn't produced a valid response within the hedge
delay (adaptive: a percentile of recent attempt latencies, clamped), a
duplicate is sent to another replica. The first valid response wins and the
other attempts are cancelled. Failed attempts fail over immediately.
"""
import asyncio
import itertools
import logging
import statistics
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field

import httpx

from .config import HedgeSettings, hedge_settings, upstream_urls

log = logging.getLogger("app.upstream")


@dataclass
class _Replica:
    url: str
    ewma: float | None = None
    last_sample: float = 0.0
    pending: dict[int, float] = field(default_factory=dict)


@dataclass
class _Outcome:
    status: int | None
    body: object
    error: Exception | None


def _is_well_formed(status: int | None, body: object) -> bool:
    return (
        status is not None
        and 200 <= status < 300
        and isinstance(body, dict)
        and isinstance(body.get("completion"), str)
        and body["completion"] != ""
        and isinstance(body.get("signature"), str)
        and body["signature"] != ""
    )


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 settings: HedgeSettings | None = None,
                 clock=time.monotonic):
        self._settings = settings or hedge_settings()
        s = self._settings
        self._replicas = [_Replica(u) for u in (urls or upstream_urls())]
        self._clock = clock
        self._rr = 0
        self._ids = itertools.count()
        self._samples: deque[float] = deque(maxlen=max(1, s.window))
        self._signatures: OrderedDict[tuple, tuple[str, int]] = OrderedDict()
        self._divergent = False
        self.stats = {"requests": 0, "hedges": 0, "failovers": 0, "cancelled": 0,
                      "signature_mismatches": 0}
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(s.attempt_timeout_s, connect=s.connect_timeout_s),
            limits=httpx.Limits(max_connections=1000, max_keepalive_connections=200),
            transport=transport,
        )

    # ---- latency model -------------------------------------------------

    def hedge_delay(self) -> float:
        s = self._settings
        delay = s.delay_ms
        if s.adaptive and len(self._samples) >= s.min_samples:
            ordered = sorted(self._samples)
            idx = min(len(ordered) - 1, int(s.percentile * len(ordered)))
            delay = ordered[idx] * 1000.0
        return min(max(delay, s.min_delay_ms), s.max_delay_ms) / 1000.0

    def _prior(self, exclude: int | None = None) -> float | None:
        measured = [r.ewma for i, r in enumerate(self._replicas)
                    if r.ewma is not None and i != exclude]
        return statistics.median(measured) if measured else None

    def _estimate(self, idx: int, now: float) -> float:
        r = self._replicas[idx]
        prior = self._prior(exclude=idx)
        if r.ewma is None:
            # Neutral prior for unmeasured replicas: never look faster than the others.
            est = prior if prior is not None else self.hedge_delay()
        elif prior is not None and self._settings.idle_decay_half_life_s > 0:
            age = max(0.0, now - r.last_sample)
            w = 0.5 ** (age / self._settings.idle_decay_half_life_s)
            est = prior + (r.ewma - prior) * w
        else:
            est = r.ewma
        if r.pending:
            est = max(est, now - min(r.pending.values()))
        return est

    def _score(self, idx: int, now: float) -> float:
        return self._estimate(idx, now) * (1 + len(self._replicas[idx].pending))

    def _observe(self, idx: int, seconds: float, now: float, *, success: bool) -> None:
        r = self._replicas[idx]
        a = self._settings.ewma_alpha
        r.ewma = seconds if r.ewma is None else (1 - a) * r.ewma + a * seconds
        r.last_sample = now
        if success:
            self._samples.append(seconds)

    def _pick(self, avoid: set[int], pinned: int | None = None) -> int:
        if pinned is not None and pinned not in avoid:
            return pinned
        n = len(self._replicas)
        candidates = [i for i in range(n) if i not in avoid] or list(range(n))
        now = self._clock()
        start = self._rr
        self._rr = (self._rr + 1) % n
        return min(candidates, key=lambda i: (self._score(i, now), (i - start) % n))

    # ---- attempts ------------------------------------------------------

    async def _attempt(self, idx: int, payload: dict) -> _Outcome:
        try:
            resp = await self._client.post(f"{self._replicas[idx].url}/v1/completions",
                                           json=payload)
        except httpx.HTTPError as exc:
            return _Outcome(None, None, exc)
        try:
            body = resp.json()
        except ValueError:
            body = None
        return _Outcome(resp.status_code, body, None)

    def _signature_key(self, payload: dict) -> tuple | None:
        if not self._settings.signature_guard:
            return None
        return (payload.get("prompt"), payload.get("max_tokens"))

    def _remember(self, key: tuple | None, signature: str, idx: int) -> None:
        if key is None or key in self._signatures:
            return
        self._signatures[key] = (signature, idx)
        while len(self._signatures) > max(1, self._settings.signature_memo_size):
            self._signatures.popitem(last=False)

    async def forward(self, payload: dict) -> tuple[int, dict]:
        if not self._settings.enabled:
            return await self._forward_simple(payload)

        s = self._settings
        self.stats["requests"] += 1
        key = self._signature_key(payload)
        known = self._signatures.get(key) if key is not None else None
        if known is not None:
            self._signatures.move_to_end(key)
        expected_sig = known[0] if known else None
        # Replicas only disagree on signatures if we've seen it happen; then pin
        # repeated prompts to the replica that first served them.
        pinned = known[1] if (known and self._divergent) else None

        tasks: dict[asyncio.Task, tuple[int, int, float]] = {}
        tried: set[int] = set()
        failed: set[int] = set()
        attempts = 0
        last_error: _Outcome | None = None
        mismatched: tuple[int, dict] | None = None

        def launch() -> None:
            nonlocal attempts
            avoid = failed | tried if pinned is None else failed
            idx = self._pick(avoid, pinned)
            tried.add(idx)
            attempts += 1
            aid = next(self._ids)
            start = self._clock()
            self._replicas[idx].pending[aid] = start
            tasks[asyncio.create_task(self._attempt(idx, payload))] = (idx, aid, start)

        launch()
        try:
            while tasks:
                can_launch = attempts < s.max_attempts
                done, _ = await asyncio.wait(
                    tasks, timeout=self.hedge_delay() if can_launch else None,
                    return_when=asyncio.FIRST_COMPLETED)
                if not done:
                    self.stats["hedges"] += 1
                    launch()
                    continue
                for task in done:
                    idx, aid, start = tasks.pop(task)
                    now = self._clock()
                    self._replicas[idx].pending.pop(aid, None)
                    out = task.result()
                    elapsed = now - start
                    if _is_well_formed(out.status, out.body):
                        sig = out.body["signature"]
                        self._observe(idx, elapsed, now, success=True)
                        if expected_sig is not None and sig != expected_sig:
                            self.stats["signature_mismatches"] += 1
                            if not self._divergent:
                                log.warning("replica signatures diverge; pinning repeated prompts")
                            self._divergent = True
                            if pinned is None:
                                pinned = known[1]
                            mismatched = mismatched or (out.status, out.body)
                            continue
                        self._remember(key, sig, idx)
                        return out.status, out.body
                    self._observe(idx, max(elapsed, s.max_delay_ms / 1000.0), now,
                                  success=False)
                    failed.add(idx)
                    last_error = out
                    log.info("upstream attempt failed replica=%s status=%s error=%r",
                             self._replicas[idx].url, out.status, out.error)
                if attempts < s.max_attempts:
                    self.stats["failovers"] += 1
                    launch()
        finally:
            await self._cancel(tasks)

        if mismatched is not None:
            # Availability over consistency once no matching response is reachable.
            return mismatched
        return self._error_response(last_error)

    async def _cancel(self, tasks: dict[asyncio.Task, tuple[int, int, float]]) -> None:
        if not tasks:
            return
        now = self._clock()
        for task, (idx, aid, start) in tasks.items():
            task.cancel()
            r = self._replicas[idx]
            r.pending.pop(aid, None)
            self.stats["cancelled"] += 1
            # A cancelled attempt is a lower bound on that replica's latency.
            elapsed = now - start
            if r.ewma is None or elapsed > r.ewma:
                self._observe(idx, elapsed, now, success=False)
        await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def _error_response(out: _Outcome | None) -> tuple[int, dict]:
        if out is not None and out.status is not None:
            body = out.body if isinstance(out.body, dict) else {"detail": "invalid upstream response"}
            if 200 <= out.status < 300:
                return 502, {"detail": "invalid upstream response"}
            return out.status, body
        if out is not None and isinstance(out.error, httpx.TimeoutException):
            return 504, {"detail": "upstream timeout"}
        return 502, {"detail": "upstream unavailable"}

    async def _forward_simple(self, payload: dict) -> tuple[int, dict]:
        """Round-robin with failover on transport errors and 5xx (hedging off)."""
        n = len(self._replicas)
        last: _Outcome | None = None
        for _ in range(min(self._settings.max_attempts, n)):
            idx = self._rr
            self._rr = (self._rr + 1) % n
            out = await self._attempt(idx, payload)
            if out.status is not None and out.status < 500:
                body = out.body if isinstance(out.body, dict) else {"detail": "invalid upstream response"}
                return out.status, body
            last = out
        return self._error_response(last)

    async def aclose(self) -> None:
        await self._client.aclose()
