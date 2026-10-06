"""Replica selection strategies for the upstream pool.

`LatencyAwareBalancer` scores each replica by an EWMA of its observed response
latency scaled by its in-flight load (`1 + outstanding_weight * outstanding`)
and sends each request to the cheapest replica. Replicas are never excluded: one that has not been tried
for `probe_interval_s` gets the next request, so slow or failing replicas are
re-measured and win traffic back once they recover.
"""
import random
import time
from dataclasses import dataclass
from typing import Callable, Protocol


class Balancer(Protocol):
    def acquire(self) -> int: ...

    def release(self, index: int, latency_s: float, ok: bool) -> None: ...


class RoundRobinBalancer:
    def __init__(self, size: int):
        self._size = size
        self._next = 0

    def acquire(self) -> int:
        index = self._next
        self._next = (self._next + 1) % self._size
        return index

    def release(self, index: int, latency_s: float, ok: bool) -> None:
        pass


@dataclass
class ReplicaStats:
    ewma_s: float | None = None
    outstanding: int = 0
    last_dispatch: float = float("-inf")


class LatencyAwareBalancer:
    def __init__(self, size: int, *, alpha: float = 0.3, probe_interval_s: float = 5.0,
                 error_penalty_s: float = 5.0, outstanding_weight: float = 1.0,
                 clock: Callable[[], float] = time.monotonic,
                 rng: random.Random | None = None):
        if size < 1:
            raise ValueError("at least one replica is required")
        if not 0 < alpha <= 1:
            raise ValueError("alpha must be in (0, 1]")
        self._alpha = alpha
        self._probe_interval_s = probe_interval_s
        self._error_penalty_s = error_penalty_s
        self._outstanding_weight = outstanding_weight
        self._clock = clock
        self._rng = rng or random.Random()
        self.stats = [ReplicaStats() for _ in range(size)]

    def acquire(self) -> int:
        now = self._clock()
        index = self._pick(now)
        stats = self.stats[index]
        stats.outstanding += 1
        stats.last_dispatch = now
        return index

    def release(self, index: int, latency_s: float, ok: bool) -> None:
        stats = self.stats[index]
        stats.outstanding = max(0, stats.outstanding - 1)
        sample = latency_s if ok else max(latency_s, self._error_penalty_s)
        if stats.ewma_s is None:
            stats.ewma_s = sample
        else:
            stats.ewma_s += self._alpha * (sample - stats.ewma_s)

    def _pick(self, now: float) -> int:
        unmeasured = [i for i, s in enumerate(self.stats)
                      if s.ewma_s is None and s.outstanding == 0]
        if unmeasured:
            return unmeasured[0]

        stale = [i for i, s in enumerate(self.stats)
                 if now - s.last_dispatch >= self._probe_interval_s]
        if stale:
            return min(stale, key=lambda i: self.stats[i].last_dispatch)

        costs = [self._cost(s) for s in self.stats]
        best = min(costs)
        candidates = [i for i, c in enumerate(costs) if c == best]
        return candidates[0] if len(candidates) == 1 else self._rng.choice(candidates)

    def _cost(self, stats: ReplicaStats) -> float:
        ewma = stats.ewma_s if stats.ewma_s is not None else 0.0
        return ewma * (1 + self._outstanding_weight * stats.outstanding)
