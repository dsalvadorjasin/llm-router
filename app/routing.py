"""Latency-aware upstream selection.

Each upstream is scored as ``ewma_latency * (1 + inflight) + failure_penalty``;
lowest wins. Upstreams with no completed sample and nothing in flight are tried
first; once something is in flight to an unmeasured upstream it is scored by
its oldest pending attempt's elapsed time. Failure penalties decay
exponentially, and a small share of selections probes the least-recently
successful upstream, so every upstream stays reachable. Ties break in
round-robin order.
"""
import itertools
import math
import random
import time
from collections.abc import Callable

from . import config

_NEGLIGIBLE_PENALTY_S = 1e-3


class _Stats:
    __slots__ = ("ewma", "pending", "penalty", "penalty_at", "last_success")

    def __init__(self) -> None:
        self.ewma: float | None = None
        self.pending: dict[int, float] = {}
        self.penalty = 0.0
        self.penalty_at = 0.0
        self.last_success = -math.inf


class LatencyRouter:
    def __init__(
        self,
        urls: list[str],
        *,
        mode: str | None = None,
        alpha: float | None = None,
        penalty_ms: float | None = None,
        penalty_halflife_ms: float | None = None,
        probe_ratio: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        rng: Callable[[], float] = random.random,
    ):
        if not urls:
            raise ValueError("at least one upstream URL is required")
        self.urls = list(urls)
        self.mode = mode if mode is not None else config.routing_mode()
        self.alpha = alpha if alpha is not None else config.ewma_alpha()
        self.penalty_s = (penalty_ms if penalty_ms is not None
                          else config.failure_penalty_ms()) / 1000
        self.halflife_s = (penalty_halflife_ms if penalty_halflife_ms is not None
                           else config.failure_penalty_halflife_ms()) / 1000
        self.probe_ratio = probe_ratio if probe_ratio is not None else config.probe_ratio()
        self._clock = clock
        self._rng = rng
        self._stats = {u: _Stats() for u in self.urls}
        self._cursor = 0
        self._tokens = itertools.count()

    @property
    def latency_aware(self) -> bool:
        return self.mode != "roundrobin"

    def _rotation(self) -> list[str]:
        n = len(self.urls)
        return [self.urls[(self._cursor + i) % n] for i in range(n)]

    def penalty(self, url: str, now: float | None = None) -> float:
        s = self._stats[url]
        if s.penalty <= 0:
            return 0.0
        now = self._clock() if now is None else now
        if self.halflife_s <= 0:
            return 0.0
        return s.penalty * 0.5 ** (max(0.0, now - s.penalty_at) / self.halflife_s)

    def score(self, url: str, now: float | None = None) -> tuple[int, float]:
        """(tier, score): tier 0 = unmeasured, idle and unpenalised (tried first)."""
        now = self._clock() if now is None else now
        s = self._stats[url]
        penalty = self.penalty(url, now)
        inflight = len(s.pending)
        if s.ewma is None:
            if inflight == 0:
                if penalty < _NEGLIGIBLE_PENALTY_S:
                    return 0, 0.0
                return 1, penalty
            oldest = now - min(s.pending.values())
            return 1, oldest * (1 + inflight) + penalty
        return 1, s.ewma * (1 + inflight) + penalty

    def rank(self) -> list[str]:
        """All upstreams best-first; pure (no cursor advance, no probing)."""
        rotation = self._rotation()
        if not self.latency_aware:
            return rotation
        now = self._clock()
        order = {u: i for i, u in enumerate(rotation)}
        return sorted(rotation, key=lambda u: (*self.score(u, now), order[u]))

    def plan(self) -> list[str]:
        """Attempt order for one request: ranking, plus occasional probe at the front."""
        ranked = self.rank()
        self._cursor = (self._cursor + 1) % len(self.urls)
        if self.latency_aware and len(ranked) > 1 and self._rng() < self.probe_ratio:
            target = min(ranked, key=lambda u: (self._stats[u].last_success, ranked.index(u)))
            ranked.remove(target)
            ranked.insert(0, target)
        return ranked

    def start(self, url: str) -> int:
        token = next(self._tokens)
        self._stats[url].pending[token] = self._clock()
        return token

    def finish(self, url: str, token: int, ok: bool) -> float:
        """Record the end of an attempt; returns its elapsed seconds."""
        now = self._clock()
        s = self._stats[url]
        started = s.pending.pop(token, now)
        elapsed = now - started
        if ok:
            s.ewma = elapsed if s.ewma is None else (
                self.alpha * elapsed + (1 - self.alpha) * s.ewma)
            s.last_success = now
        else:
            s.penalty = self.penalty(url, now) + self.penalty_s
            s.penalty_at = now
        return elapsed

    def abandon(self, url: str, token: int) -> None:
        """Drop a cancelled attempt without recording a sample or a penalty."""
        self._stats[url].pending.pop(token, None)

    def snapshot(self) -> dict[str, dict]:
        now = self._clock()
        return {
            u: {"ewma_ms": None if s.ewma is None else s.ewma * 1000,
                "inflight": len(s.pending),
                "penalty_ms": self.penalty(u, now) * 1000}
            for u, s in self._stats.items()
        }
