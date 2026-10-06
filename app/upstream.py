"""Replica selection and hedged dispatch for upstream completions."""

import asyncio
import logging
import random
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from itertools import count

import httpx

from . import config
from .config import HedgeSettings, hedge_settings, upstream_urls

log = logging.getLogger("app.upstream")


@dataclass
class _Replica:
    url: str
    ewma: float | None = None
    floor: float = 0.0
    last_sample: float = 0.0
    failing: bool = False
    last_pick: float = 0.0
    pending: dict[int, float] = field(default_factory=dict)

    @property
    def inflight(self) -> int:
        return len(self.pending)


@dataclass
class _Outcome:
    status: int | None
    body: object
    error: Exception | None
    finished_at: float


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
    def __init__(
        self,
        urls: list[str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        settings: HedgeSettings | None = None,
        *,
        strategy: str | None = None,
        ewma_alpha: float | None = None,
        ewma_half_life_s: float | None = None,
        explore_rate: float | None = None,
        probe_interval_s: float | None = None,
        connect_timeout_s: float | None = None,
        attempt_timeout_s: float | None = None,
        max_attempts: int | None = None,
        rng: random.Random | None = None,
        clock=time.monotonic,
    ):
        self._settings = settings or hedge_settings()
        self.strategy = strategy or config.routing_strategy()
        if self.strategy not in config.STRATEGIES:
            raise ValueError(f"unknown routing strategy {self.strategy!r}")
        self._alpha = config.ewma_alpha() if ewma_alpha is None else ewma_alpha
        self._half_life = (
            config.ewma_half_life_s()
            if ewma_half_life_s is None
            else ewma_half_life_s
        )
        self._explore = (
            config.explore_rate() if explore_rate is None else explore_rate
        )
        self._probe_interval = (
            config.probe_interval_s()
            if probe_interval_s is None
            else probe_interval_s
        )
        self._connect_timeout = (
            config.connect_timeout_s()
            if connect_timeout_s is None
            else connect_timeout_s
        )
        self._attempt_timeout = (
            config.attempt_timeout_s()
            if attempt_timeout_s is None
            else attempt_timeout_s
        )
        self._max_attempts_override = (
            None if max_attempts is None else max(1, max_attempts)
        )
        self._replicas = [_Replica(url) for url in (urls or upstream_urls())]
        self._rr = 0
        self._ids = count()
        self._rng = rng or random.Random()
        self._clock = clock
        self._samples: deque[float] = deque(
            maxlen=max(1, self._settings.window)
        )
        self._signatures: OrderedDict[tuple, tuple[str, int]] = OrderedDict()
        self._divergent = False
        self._stats_logged = False
        self.stats = {
            "requests": 0,
            "hedges": 0,
            "failovers": 0,
            "cancelled": 0,
            "signature_mismatches": 0,
        }
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                self._attempt_timeout, connect=self._connect_timeout
            ),
            limits=httpx.Limits(
                max_connections=1000, max_keepalive_connections=200
            ),
            transport=transport,
        )

    def _attempt_limit(self, hedged: bool) -> int:
        if self._max_attempts_override is not None:
            return self._max_attempts_override
        if hedged:
            return max(1, self._settings.max_attempts)
        return config.max_attempts()

    def hedge_delay(self) -> float:
        settings = self._settings
        delay = settings.delay_ms
        if settings.adaptive and len(self._samples) >= settings.min_samples:
            ordered = sorted(self._samples)
            index = min(
                len(ordered) - 1, int(settings.percentile * len(ordered))
            )
            delay = ordered[index] * 1000.0
        return (
            min(max(delay, settings.min_delay_ms), settings.max_delay_ms) / 1000.0
        )

    def _update_ewma(
        self, replica: _Replica, sample: float, now: float
    ) -> None:
        if replica.ewma is None:
            replica.ewma = sample
            replica.last_sample = now
            return
        idle = max(0.0, now - replica.last_sample)
        keep = 1.0 - self._alpha
        if self._half_life > 0:
            keep *= 0.5 ** (idle / self._half_life)
        replica.ewma = (1.0 - keep) * sample + keep * replica.ewma
        replica.last_sample = now

    def _record(
        self,
        replica: _Replica,
        elapsed: float,
        ok: bool,
        *,
        sample_delay: bool = True,
        cancelled: bool = False,
    ) -> None:
        now = self._clock()
        if cancelled:
            if replica.ewma is None:
                replica.floor = max(replica.floor, elapsed)
            elif elapsed > replica.ewma:
                self._update_ewma(replica, elapsed, now)
            return

        replica.floor = 0.0
        if not ok:
            self._update_ewma(
                replica, max(elapsed, self._attempt_timeout), now
            )
            replica.failing = True
            return

        if replica.failing or replica.ewma is None:
            replica.failing = False
            replica.ewma = elapsed
            replica.last_sample = now
        else:
            self._update_ewma(replica, elapsed, now)
        if sample_delay:
            self._samples.append(elapsed)

    def _score(self, idx: int, now: float) -> float:
        replica = self._replicas[idx]
        if replica.ewma is None:
            latency = max(
                replica.floor,
                now - min(replica.pending.values(), default=now),
            )
        else:
            latency = replica.ewma
        return latency * (replica.inflight + 1)

    def _pick(self, exclude: set[int]) -> int:
        candidates = [
            idx for idx in range(len(self._replicas)) if idx not in exclude
        ]
        if not candidates:
            raise RuntimeError("no replicas configured")
        if self.strategy == "round_robin":
            count_replicas = len(self._replicas)
            for _ in range(count_replicas):
                idx = self._rr % count_replicas
                self._rr += 1
                if idx in candidates:
                    return idx

        now = self._clock()
        idle = [
            idx
            for idx in candidates
            if self._replicas[idx].inflight == 0
            and (
                self._replicas[idx].ewma is None
                or now - self._replicas[idx].last_pick >= self._probe_interval
            )
        ]
        if idle:
            return min(idle, key=lambda idx: self._replicas[idx].last_pick)
        if len(candidates) == 1 or self._rng.random() < self._explore:
            return self._rng.choice(candidates)
        first, second = self._rng.sample(candidates, 2)
        return (
            first
            if self._score(first, now) <= self._score(second, now)
            else second
        )

    async def _attempt(self, idx: int, payload: dict) -> _Outcome:
        try:
            async with asyncio.timeout(self._attempt_timeout):
                response = await self._client.post(
                    f"{self._replicas[idx].url}/v1/completions",
                    json=payload,
                )
                try:
                    body = response.json()
                except ValueError as exc:
                    return _Outcome(
                        response.status_code, None, exc, self._clock()
                    )
        except (httpx.HTTPError, TimeoutError) as exc:
            return _Outcome(None, None, exc, self._clock())
        return _Outcome(
            response.status_code, body, None, self._clock()
        )

    def _signature_key(self, payload: dict) -> tuple | None:
        if not self._settings.signature_guard:
            return None
        return (payload.get("prompt"), payload.get("max_tokens"))

    def _remember(self, key: tuple | None, signature: str, idx: int) -> None:
        if key is None or key in self._signatures:
            return
        self._signatures[key] = (signature, idx)
        while len(self._signatures) > max(
            1, self._settings.signature_memo_size
        ):
            self._signatures.popitem(last=False)

    @staticmethod
    def _response_body(outcome: _Outcome) -> dict:
        return (
            outcome.body
            if isinstance(outcome.body, dict)
            else {"detail": "invalid upstream response"}
        )

    @staticmethod
    def _error_response(outcome: _Outcome | None) -> tuple[int, dict]:
        if outcome is not None and outcome.status is not None:
            if (
                outcome.error is None
                and outcome.status >= 500
                and isinstance(outcome.body, dict)
            ):
                return outcome.status, outcome.body
            return 502, {"detail": "invalid upstream response"}
        if outcome is not None and isinstance(
            outcome.error, (TimeoutError, httpx.TimeoutException)
        ):
            return 504, {
                "detail": f"upstream error: {type(outcome.error).__name__}"
            }
        if outcome is not None and outcome.error is not None:
            return 502, {
                "detail": f"upstream error: {type(outcome.error).__name__}"
            }
        return 502, {"detail": "invalid upstream response"}

    async def _forward_simple(self, payload: dict) -> tuple[int, dict]:
        tried: set[int] = set()
        last: _Outcome | None = None
        limit = min(self._attempt_limit(False), len(self._replicas))
        for _ in range(limit):
            idx = self._pick(exclude=tried)
            tried.add(idx)
            replica = self._replicas[idx]
            attempt_id = next(self._ids)
            start = self._clock()
            replica.last_pick = start
            replica.pending[attempt_id] = start
            try:
                outcome = await self._attempt(idx, payload)
            finally:
                replica.pending.pop(attempt_id, None)
            elapsed = max(0.0, outcome.finished_at - start)
            if (
                outcome.error is None
                and outcome.status is not None
                and outcome.status < 500
            ):
                malformed_completion = (
                    200 <= outcome.status < 300
                    and not (
                        isinstance(outcome.body, dict)
                        and isinstance(outcome.body.get("completion"), str)
                        and outcome.body["completion"] != ""
                    )
                )
                if not malformed_completion:
                    self._record(replica, elapsed, ok=True, sample_delay=False)
                    return outcome.status, self._response_body(outcome)
            self._record(replica, elapsed, ok=False)
            last = outcome
            if limit > len(tried):
                self.stats["failovers"] += 1
        return self._error_response(last)

    def _hedged_pick(
        self, tried: set[int], failed: set[int], pinned: int | None
    ) -> int:
        if pinned is not None and pinned not in failed:
            return pinned
        exclude = tried | failed
        candidates = [
            idx for idx in range(len(self._replicas)) if idx not in exclude
        ]
        if not candidates:
            exclude = failed
            candidates = [
                idx for idx in range(len(self._replicas)) if idx not in exclude
            ]
        if not candidates:
            candidates = list(range(len(self._replicas)))
        return self._pick(set(range(len(self._replicas))) - set(candidates))

    async def _cancel(
        self, tasks: dict[asyncio.Task, tuple[int, int, float]]
    ) -> None:
        if not tasks:
            return
        for task, (idx, attempt_id, start) in tasks.items():
            task.cancel()
            replica = self._replicas[idx]
            replica.pending.pop(attempt_id, None)
            self.stats["cancelled"] += 1
            elapsed = max(0.0, self._clock() - start)
            self._record(
                replica,
                elapsed,
                ok=True,
                sample_delay=False,
                cancelled=True,
            )
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _forward_hedged(self, payload: dict) -> tuple[int, dict]:
        settings = self._settings
        max_attempts = self._attempt_limit(True)
        key = self._signature_key(payload)
        known = self._signatures.get(key) if key is not None else None
        if known is not None:
            self._signatures.move_to_end(key)
        expected_signature = known[0] if known else None
        pinned = known[1] if known and self._divergent else None

        tasks: dict[asyncio.Task, tuple[int, int, float]] = {}
        tried: set[int] = set()
        failed: set[int] = set()
        attempts = 0
        last_error: _Outcome | None = None
        mismatched: tuple[int, dict] | None = None

        def launch() -> None:
            nonlocal attempts
            if (
                attempts > 0
                and settings.reuse_replicas
                and pinned is None
            ):
                candidates = [
                    idx
                    for idx in range(len(self._replicas))
                    if idx not in failed
                ]
                if not candidates:
                    candidates = list(range(len(self._replicas)))
                candidates = [
                    idx
                    for idx in candidates
                    if self._replicas[idx].ewma is not None or idx not in tried
                ]
                if not candidates:
                    idx = self._hedged_pick(tried, failed, pinned)
                else:
                    now = self._clock()
                    idx = min(
                        candidates,
                        key=lambda candidate: (
                            self._score(candidate, now),
                            candidate in tried,
                            self._replicas[candidate].last_pick,
                        ),
                    )
            else:
                idx = self._hedged_pick(tried, failed, pinned)
            tried.add(idx)
            attempts += 1
            attempt_id = next(self._ids)
            start = self._clock()
            replica = self._replicas[idx]
            replica.last_pick = start
            replica.pending[attempt_id] = start
            task = asyncio.create_task(self._attempt(idx, payload))
            tasks[task] = (idx, attempt_id, start)

        launch()
        try:
            while tasks:
                timeout = self.hedge_delay() if attempts < max_attempts else None
                done, _ = await asyncio.wait(
                    tasks,
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    self.stats["hedges"] += 1
                    launch()
                    continue

                winner: tuple[int, dict] | None = None
                client_error: tuple[int, dict] | None = None
                ordered: list[
                    tuple[float, int, asyncio.Task, _Outcome, int, float]
                ] = []
                for task in done:
                    idx, attempt_id, start = tasks.pop(task)
                    replica = self._replicas[idx]
                    replica.pending.pop(attempt_id, None)
                    outcome = task.result()
                    ordered.append(
                        (
                            outcome.finished_at,
                            attempt_id,
                            task,
                            outcome,
                            idx,
                            start,
                        )
                    )
                ordered.sort(key=lambda item: (item[0], item[1]))
                for _, _, _, outcome, idx, start in ordered:
                    elapsed = max(0.0, outcome.finished_at - start)
                    replica = self._replicas[idx]
                    if (
                        outcome.error is None
                        and outcome.status is not None
                        and 400 <= outcome.status < 500
                    ):
                        self._record(
                            replica, elapsed, ok=True, sample_delay=False
                        )
                        if winner is None and client_error is None:
                            client_error = (
                                outcome.status,
                                self._response_body(outcome),
                            )
                        continue

                    if _is_well_formed(outcome.status, outcome.body):
                        self._record(replica, elapsed, ok=True)
                        signature = outcome.body["signature"]
                        if (
                            expected_signature is not None
                            and signature != expected_signature
                        ):
                            self.stats["signature_mismatches"] += 1
                            if not self._divergent:
                                log.warning(
                                    "replica signatures diverge; pinning repeated prompts"
                                )
                            self._divergent = True
                            if pinned is None and known is not None:
                                pinned = known[1]
                            mismatched = mismatched or (
                                outcome.status,
                                outcome.body,
                            )
                        elif winner is None and client_error is None:
                            self._remember(key, signature, idx)
                            winner = (outcome.status, outcome.body)
                        continue

                    self._record(replica, elapsed, ok=False)
                    failed.add(idx)
                    last_error = outcome
                    log.info(
                        "upstream attempt failed replica=%s status=%s error=%r",
                        replica.url,
                        outcome.status,
                        outcome.error,
                    )

                if winner is not None:
                    return winner
                if client_error is not None:
                    return client_error
                if attempts < max_attempts:
                    self.stats["failovers"] += 1
                    launch()
        finally:
            await self._cancel(tasks)

        if mismatched is not None:
            return mismatched
        return self._error_response(last_error)

    async def forward(self, payload: dict) -> tuple[int, dict]:
        self.stats["requests"] += 1
        if not self._settings.enabled:
            return await self._forward_simple(payload)
        return await self._forward_hedged(payload)

    def snapshot(self) -> list[dict]:
        return [
            {
                "url": replica.url,
                "ewma_s": replica.ewma,
                "inflight": replica.inflight,
                "failing": replica.failing,
            }
            for replica in self._replicas
        ]

    async def aclose(self) -> None:
        if not self._stats_logged:
            log.info("upstream pool stats: %s", self.stats)
            self._stats_logged = True
        await self._client.aclose()
