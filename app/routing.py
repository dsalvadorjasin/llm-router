"""Latency-aware replica selection with bounded, retried attempts.

Policies (``ROUTER_POLICY``):

* ``ewma`` (default): score each replica by an exponentially weighted moving
  average of its attempt latency. Failed / timed-out attempts are recorded as
  a penalised sample. A replica's excess over the pool's best score decays
  with a half-life while it receives no traffic, so a slow or failing replica
  is only ever deprioritised, never excluded: it drifts back into the tie band
  and gets probed again.
* ``least_outstanding``: score by the number of in-flight attempts.
* ``round_robin``: every replica always ties.

Replicas whose score is within the tie band of the best are equivalent and
picked in round-robin order, which spreads load across healthy replicas.

Each attempt is bounded by an adaptive timeout (a multiple of a high quantile
of recent successful attempt latencies, clamped to [min, max]); on timeout,
transport error or non-200 the replica is penalised and the request is retried
on a different replica. The last attempt gets a longer, still bounded timeout.
"""
import asyncio
import math
import time
from collections import deque
from dataclasses import dataclass, field

import httpx

POLICIES = ("ewma", "least_outstanding", "round_robin")


@dataclass(frozen=True)
class RoutingConfig:
    policy: str = "ewma"
    max_attempts: int = 4
    attempt_timeout_ms: float = 250.0
    attempt_timeout_min_ms: float = 50.0
    attempt_timeout_max_ms: float = 1000.0
    timeout_quantile: float = 0.99
    timeout_multiplier: float = 1.03
    final_timeout_ms: float = 10000.0
    ewma_alpha: float = 0.3
    failure_penalty: float = 2.0
    decay_half_life_s: float = 10.0
    tie_ratio: float = 1.5
    tie_abs_ms: float = 10.0
    window: int = 500
    min_samples: int = 20


@dataclass
class _Replica:
    url: str
    ewma_ms: float | None = None
    last_update: float = 0.0
    outstanding: int = 0


@dataclass
class LatencyAwareRouter:
    urls: list[str]
    config: RoutingConfig = field(default_factory=RoutingConfig)
    clock: callable = time.monotonic

    def __post_init__(self) -> None:
        if self.config.policy not in POLICIES:
            raise ValueError(f"unknown ROUTER_POLICY {self.config.policy!r}")
        self._replicas = [_Replica(u) for u in self.urls]
        self._cursor = 0
        self._ok_latencies: deque[float] = deque(maxlen=self.config.window)

    # -- scoring ---------------------------------------------------------
    def _scores(self, now: float) -> list[float]:
        cfg = self.config
        if cfg.policy == "round_robin":
            return [0.0] * len(self._replicas)
        if cfg.policy == "least_outstanding":
            return [float(r.outstanding) for r in self._replicas]
        known = [r.ewma_ms for r in self._replicas if r.ewma_ms is not None]
        best = min(known) if known else 0.0
        scores = []
        for r in self._replicas:
            if r.ewma_ms is None:
                scores.append(best)
                continue
            age = max(0.0, now - r.last_update)
            decay = 0.5 ** (age / cfg.decay_half_life_s) if cfg.decay_half_life_s > 0 else 1.0
            scores.append(best + (r.ewma_ms - best) * decay)
        return scores

    def _tied(self, score: float, best: float) -> bool:
        cfg = self.config
        if self.config.policy == "least_outstanding":
            return score <= best
        return score <= best * cfg.tie_ratio or score - best <= cfg.tie_abs_ms

    def pick(self, exclude: set[int] = frozenset()) -> int:
        n = len(self._replicas)
        candidates = [i for i in range(n) if i not in exclude] or list(range(n))
        scores = self._scores(self.clock())
        best = min(scores[i] for i in candidates)
        for step in range(n):
            i = (self._cursor + step) % n
            if i in candidates and self._tied(scores[i], best):
                self._cursor = (i + 1) % n
                return i
        raise AssertionError("unreachable")

    # -- feedback --------------------------------------------------------
    def _record(self, idx: int, sample_ms: float) -> None:
        r = self._replicas[idx]
        now = self.clock()
        if r.ewma_ms is None:
            r.ewma_ms = sample_ms
        else:
            a = self.config.ewma_alpha
            r.ewma_ms = a * sample_ms + (1 - a) * r.ewma_ms
        r.last_update = now

    def record_success(self, idx: int, latency_ms: float) -> None:
        self._ok_latencies.append(latency_ms)
        self._record(idx, latency_ms)

    def record_failure(self, idx: int, elapsed_ms: float, timeout_ms: float) -> None:
        self._record(idx, max(elapsed_ms, timeout_ms) * self.config.failure_penalty)

    def attempt_timeout_ms(self) -> float:
        cfg = self.config
        if len(self._ok_latencies) < cfg.min_samples:
            return cfg.attempt_timeout_ms
        ordered = sorted(self._ok_latencies)
        k = min(len(ordered) - 1, max(0, math.ceil(cfg.timeout_quantile * len(ordered)) - 1))
        t = ordered[k] * cfg.timeout_multiplier
        return min(cfg.attempt_timeout_max_ms, max(cfg.attempt_timeout_min_ms, t))

    # -- request path ----------------------------------------------------
    async def forward(self, client: httpx.AsyncClient, payload: dict) -> tuple[int, dict]:
        cfg = self.config
        attempts = max(1, cfg.max_attempts)
        last: tuple[int, dict] = (502, {"detail": "no upstream replica available"})
        prev: set[int] = set()
        for attempt in range(attempts):
            idx = self.pick(exclude=prev)
            final = attempt == attempts - 1
            timeout_ms = cfg.final_timeout_ms if final else self.attempt_timeout_ms()
            replica = self._replicas[idx]
            replica.outstanding += 1
            start = self.clock()
            try:
                resp = await asyncio.wait_for(
                    client.post(f"{replica.url}/v1/completions", json=payload),
                    timeout=timeout_ms / 1000,
                )
                body = resp.json()
            except (asyncio.TimeoutError, httpx.HTTPError, ValueError) as exc:
                elapsed = (self.clock() - start) * 1000
                self.record_failure(idx, elapsed, timeout_ms)
                last = (504 if isinstance(exc, asyncio.TimeoutError) else 502,
                        {"detail": f"upstream error: {type(exc).__name__}"})
                prev = {idx}
                continue
            finally:
                replica.outstanding -= 1
            elapsed = (self.clock() - start) * 1000
            if resp.status_code == 200:
                self.record_success(idx, elapsed)
                return resp.status_code, body
            self.record_failure(idx, elapsed, timeout_ms)
            last = (resp.status_code, body)
            prev = {idx}
        return last
