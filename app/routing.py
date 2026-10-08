"""Latency-aware upstream selection.

Each upstream is scored as ``ewma_latency * (1 + inflight) + failure_penalty``;
lowest wins. Upstreams with no completed sample and nothing in flight are tried
first; once something is in flight to an unmeasured upstream it is scored by
its oldest pending attempt's elapsed time. Failure penalties decay
exponentially, and a small share of selections probes the least-recently
successful upstream, so every upstream stays reachable. Ties break in
round-robin order.

Attempts that end without a response (cancelled hedge losers, per-attempt
timeouts) only give a lower bound on latency: it can raise a measured EWMA,
never lower it, and is never used to seed an unmeasured one. An unmeasured
upstream whose lower bound already exceeds the best measured EWMA is ranked
after every measured upstream until a probe measures it.
"""
import itertools
import math
import random
import time
from collections.abc import Callable

from . import config

_NEGLIGIBLE_PENALTY_S = 1e-3


class _Stats:
    __slots__ = ("ewma", "floor", "pending", "penalty", "penalty_at", "last_success")

    def __init__(self) -> None:
        self.ewma: float | None = None
        self.floor = 0.0
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
        timeout_penalty_ms: float | None = None,
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
        self.timeout_penalty_s = (timeout_penalty_ms if timeout_penalty_ms is not None
                                  else config.timeout_penalty_ms()) / 1000
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

    def _best_ewma(self) -> float:
        return min((s.ewma for s in self._stats.values() if s.ewma is not None),
                   default=math.inf)

    def score(self, url: str, now: float | None = None) -> tuple[int, float]:
        """(tier, score): tier 0 = unmeasured, idle and unpenalised (tried first);
        tier 2 = unmeasured but known to be slower than the best measured upstream."""
        now = self._clock() if now is None else now
        s = self._stats[url]
        penalty = self.penalty(url, now)
        inflight = len(s.pending)
        if s.ewma is None:
            if inflight == 0:
                if s.floor > 0 and s.floor >= self._best_ewma():
                    return 2, s.floor + penalty
                if penalty < _NEGLIGIBLE_PENALTY_S:
                    return 0, 0.0
                return 1, penalty
            oldest = max(now - min(s.pending.values()), s.floor)
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

    def select(self) -> tuple[list[str], str | None]:
        """Ranking for one request (advances the cursor) and an optional probe target:
        the least-recently-successful upstream, when it isn't already ranked first."""
        ranked = self.rank()
        self._cursor = (self._cursor + 1) % len(self.urls)
        probe = None
        if self.latency_aware and len(ranked) > 1 and self._rng() < self.probe_ratio:
            target = min(ranked, key=lambda u: (self._stats[u].last_success, ranked.index(u)))
            if target != ranked[0]:
                probe = target
        return ranked, probe

    def plan(self) -> list[str]:
        """Attempt order for one request: ranking, plus occasional probe at the front."""
        ranked, probe = self.select()
        if probe is not None:
            ranked.remove(probe)
            ranked.insert(0, probe)
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
            s.floor = 0.0
            s.last_success = now
        else:
            self._add_penalty(s, url, now, self.penalty_s)
        return elapsed

    def abandon(self, url: str, token: int, lower_bound: bool = False) -> None:
        """Drop an attempt that ended without a response, with no failure penalty.

        With ``lower_bound`` its elapsed time is recorded as a lower bound on the
        upstream's latency (raises a measured EWMA, never lowers or seeds it).
        """
        now = self._clock()
        s = self._stats[url]
        started = s.pending.pop(token, None)
        if not lower_bound or started is None:
            return
        elapsed = now - started
        if s.ewma is None:
            s.floor = max(s.floor, elapsed)
        elif elapsed > s.ewma:
            s.ewma = self.alpha * elapsed + (1 - self.alpha) * s.ewma

    def timeout(self, url: str, token: int) -> None:
        """A per-attempt timeout: a latency lower bound plus the (optional) timeout penalty."""
        self.abandon(url, token, lower_bound=True)
        if self.timeout_penalty_s > 0:
            now = self._clock()
            self._add_penalty(self._stats[url], url, now, self.timeout_penalty_s)

    def _add_penalty(self, s: _Stats, url: str, now: float, amount: float) -> None:
        s.penalty = self.penalty(url, now) + amount
        s.penalty_at = now

    def snapshot(self) -> dict[str, dict]:
        now = self._clock()
        return {
            u: {"ewma_ms": None if s.ewma is None else s.ewma * 1000,
                "floor_ms": s.floor * 1000,
                "inflight": len(s.pending),
                "penalty_ms": self.penalty(u, now) * 1000}
            for u, s in self._stats.items()
        }
