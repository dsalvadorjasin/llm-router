import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from itertools import count

import httpx

from . import config

log = logging.getLogger("app.upstream")


@dataclass
class _Replica:
    url: str
    ewma: float | None = None
    last_sample: float = 0.0
    failing: bool = False
    last_pick: float = 0.0
    pending: dict[int, float] = field(default_factory=dict)

    @property
    def inflight(self) -> int:
        return len(self.pending)


class _UpstreamError(Exception):
    def __init__(self, status: int, body: dict):
        super().__init__(body.get("detail"))
        self.status = status
        self.body = body


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None, *,
                 strategy: str | None = None,
                 ewma_alpha: float | None = None,
                 ewma_half_life_s: float | None = None,
                 explore_rate: float | None = None,
                 probe_interval_s: float | None = None,
                 connect_timeout_s: float | None = None,
                 attempt_timeout_s: float | None = None,
                 max_attempts: int | None = None,
                 rng: random.Random | None = None):
        urls = urls or config.upstream_urls()
        self.strategy = strategy or config.routing_strategy()
        if self.strategy not in config.STRATEGIES:
            raise ValueError(f"unknown routing strategy {self.strategy!r}")
        self._alpha = config.ewma_alpha() if ewma_alpha is None else ewma_alpha
        self._half_life = (config.ewma_half_life_s()
                           if ewma_half_life_s is None else ewma_half_life_s)
        self._explore = config.explore_rate() if explore_rate is None else explore_rate
        self._probe_interval = (config.probe_interval_s()
                                if probe_interval_s is None else probe_interval_s)
        self._attempt_timeout = (config.attempt_timeout_s()
                                 if attempt_timeout_s is None else attempt_timeout_s)
        self._max_attempts = max(1, config.max_attempts() if max_attempts is None else max_attempts)
        connect = config.connect_timeout_s() if connect_timeout_s is None else connect_timeout_s
        self._replicas = [_Replica(u) for u in urls]
        self._rr = 0
        self._ids = count()
        self._rng = rng or random.Random()
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._attempt_timeout, connect=connect),
            transport=transport,
        )

    async def forward(self, payload: dict) -> tuple[int, dict]:
        tried: set[int] = set()
        last: tuple[int, dict] = (502, {"detail": "no upstream replica available"})
        for _ in range(min(self._max_attempts, len(self._replicas))):
            idx = self._pick(exclude=tried)
            tried.add(idx)
            try:
                return await self._attempt(idx, payload)
            except _UpstreamError as exc:
                last = (exc.status, exc.body)
            except (httpx.HTTPError, TimeoutError, ValueError) as exc:
                status = 504 if isinstance(exc, (TimeoutError, httpx.TimeoutException)) else 502
                last = (status, {"detail": f"upstream error: {type(exc).__name__}"})
            log.warning("upstream %s failed (%s), %s", self._replicas[idx].url,
                        last[0], "retrying" if len(tried) < len(self._replicas) else "giving up")
        return last

    async def _attempt(self, idx: int, payload: dict) -> tuple[int, dict]:
        replica = self._replicas[idx]
        req_id = next(self._ids)
        start = time.monotonic()
        replica.last_pick = start
        replica.pending[req_id] = start
        ok = False
        try:
            async with asyncio.timeout(self._attempt_timeout):
                resp = await self._client.post(f"{replica.url}/v1/completions", json=payload)
                body = resp.json()
            if resp.status_code >= 500:
                raise _UpstreamError(resp.status_code, body if isinstance(body, dict) else {})
            ok = True
            return resp.status_code, body
        finally:
            replica.pending.pop(req_id, None)
            self._record(replica, time.monotonic() - start, ok)

    def _record(self, replica: _Replica, elapsed: float, ok: bool) -> None:
        now = time.monotonic()
        idle = now - replica.last_sample if replica.last_sample else 0.0
        replica.last_sample = now
        if not ok:
            sample = max(elapsed, self._attempt_timeout)
            replica.failing = True
        elif replica.failing or replica.ewma is None:
            replica.failing = False
            replica.ewma = elapsed
            return
        else:
            sample = elapsed
        if replica.ewma is None:
            replica.ewma = sample
            return
        # Stale history decays with time, so a replica that was only probed
        # occasionally is re-scored mostly from its latest sample.
        keep = (1 - self._alpha)
        if self._half_life > 0:
            keep *= 0.5 ** (idle / self._half_life)
        replica.ewma = (1 - keep) * sample + keep * replica.ewma

    def _pick(self, exclude: set[int]) -> int:
        candidates = [i for i in range(len(self._replicas)) if i not in exclude]
        if not candidates:
            raise RuntimeError("no replicas configured")
        if self.strategy == "round_robin":
            n = len(self._replicas)
            for _ in range(n):
                idx = self._rr % n
                self._rr += 1
                if idx in candidates:
                    return idx
        now = time.monotonic()
        # Cold start / stale replicas: send one request at a time to any replica
        # with no sample yet or not picked within the probe interval, so a
        # recovered replica is always re-measured.
        idle = [i for i in candidates if self._replicas[i].inflight == 0 and (
            self._replicas[i].ewma is None
            or now - self._replicas[i].last_pick >= self._probe_interval)]
        if idle:
            return min(idle, key=lambda i: self._replicas[i].last_pick)
        if len(candidates) == 1 or self._rng.random() < self._explore:
            return self._rng.choice(candidates)
        a, b = self._rng.sample(candidates, 2)
        return a if self._score(a, now) <= self._score(b, now) else b

    def _score(self, idx: int, now: float) -> float:
        r = self._replicas[idx]
        if r.ewma is None:
            # Unsampled replica with a probe in flight: it is at least as slow
            # as that probe has been pending.
            latency = now - min(r.pending.values(), default=now)
        else:
            latency = r.ewma
        return latency * (r.inflight + 1)

    def snapshot(self) -> list[dict]:
        return [{"url": r.url, "ewma_s": r.ewma, "inflight": r.inflight, "failing": r.failing}
                for r in self._replicas]

    async def aclose(self) -> None:
        await self._client.aclose()
