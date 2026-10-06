import asyncio
import hashlib
import json
import logging
import random
import time
from collections import Counter, OrderedDict
from itertools import cycle

import httpx

from .config import HedgeConfig, hedge_config, upstream_urls

log = logging.getLogger("app.upstream")


def is_valid_completion(status: int, body: object) -> bool:
    """A response a hedge race may accept as the winner."""
    if status != 200 or not isinstance(body, dict):
        return False
    completion = body.get("completion")
    if not isinstance(completion, str) or not completion:
        return False
    signature = body.get("signature", "")
    return "signature" not in body or (isinstance(signature, str) and bool(signature))


class UpstreamPool:
    """Forwards completions to a replica fleet.

    With hedging enabled (the default), each request starts on one replica
    and, if no valid response has arrived by each offset in
    `HedgeConfig.delays_ms`, launches a duplicate on the replica expected to
    answer fastest. The first valid response wins and the losers are
    cancelled. Failed attempts (transport errors, 5xx, malformed bodies)
    trigger the next attempt immediately. With hedging disabled the pool
    keeps its original behaviour: plain round-robin, one attempt, no timeout.
    """

    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 hedge: HedgeConfig | None = None,
                 rng: random.Random | None = None):
        self._url_list = list(urls or upstream_urls())
        self._urls = cycle(self._url_list)
        self._hedge = hedge if hedge is not None else hedge_config()
        timeout = httpx.Timeout(self._hedge.attempt_timeout_s) if self._hedge.enabled else None
        self._client = httpx.AsyncClient(timeout=timeout, transport=transport)
        self._rng = rng or random.Random()
        self._ewma_ms: list[float | None] = [None] * len(self._url_list)
        # prompt key -> [replica index, confirmed by a valid response]
        self._affinity: OrderedDict[str, list] = OrderedDict()
        self.stats: Counter[str] = Counter()

    @property
    def hedge(self) -> HedgeConfig:
        return self._hedge

    def replica_latency_ms(self) -> dict[str, float | None]:
        return dict(zip(self._url_list, self._ewma_ms))

    async def forward(self, payload: dict) -> tuple[int, dict]:
        if not self._hedge.enabled:
            url = next(self._urls)
            resp = await self._client.post(f"{url}/v1/completions", json=payload)
            return resp.status_code, resp.json()
        return await self._hedged_forward(payload)

    async def aclose(self) -> None:
        if self._hedge.enabled and self.stats:
            log.info("hedge stats %s replica_latency_ms %s", dict(self.stats),
                     {u: None if v is None else round(v, 1)
                      for u, v in self.replica_latency_ms().items()})
        await self._client.aclose()

    # -- hedging ---------------------------------------------------------------

    async def _hedged_forward(self, payload: dict) -> tuple[int, dict]:
        cfg = self._hedge
        self.stats["requests"] += 1
        start = time.monotonic()
        uses = [0] * len(self._url_list)
        key, pinned, reserved = self._affinity_lookup(payload, uses)
        tasks: dict[asyncio.Task, tuple[int, int]] = {}
        last: tuple[int, dict] | None = None
        confirmed = False

        def launch() -> None:
            idx = pinned if pinned is not None else self._pick(uses, first=not tasks and not any(uses))
            uses[idx] += 1
            self.stats["attempts"] += 1
            tasks[asyncio.ensure_future(self._attempt(idx, payload))] = (idx, sum(uses))

        try:
            async with asyncio.timeout(cfg.deadline_s):
                launch()
                launched = 1
                while tasks:
                    wait = None
                    if launched < cfg.max_attempts:
                        due = start + cfg.delays_ms[launched - 1] / 1000
                        wait = max(0.0, due - time.monotonic())
                    done, _ = await asyncio.wait(tasks, timeout=wait,
                                                 return_when=asyncio.FIRST_COMPLETED)
                    if not done:
                        self.stats["hedges"] += 1
                        launch()
                        launched += 1
                        continue
                    failed = False
                    for task in done:
                        idx, ordinal = tasks.pop(task)
                        status, body = task.result()
                        if is_valid_completion(status, body):
                            self.stats[f"wins_attempt_{ordinal}"] += 1
                            confirmed = True
                            self._affinity_confirm(key, idx)
                            return status, body
                        if 400 <= status < 500:
                            # client errors are deterministic; duplicating them is pointless
                            return status, body
                        failed = True
                        last = (status, body)
                    if failed and launched < cfg.max_attempts:
                        self.stats["retries"] += 1
                        launch()
                        launched += 1
        except TimeoutError:
            self.stats["deadline_exceeded"] += 1
            last = (504, {"detail": "upstream deadline exceeded"})
        finally:
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if reserved and not confirmed:
                self._affinity_release(key, pinned)
        self.stats["exhausted"] += 1
        return last or (502, {"detail": "no upstream replica returned a valid response"})

    async def _attempt(self, idx: int, payload: dict) -> tuple[int, dict]:
        url = self._url_list[idx]
        t0 = time.monotonic()
        try:
            resp = await self._client.post(f"{url}/v1/completions", json=payload)
        except asyncio.CancelledError:
            self._observe(idx, (time.monotonic() - t0) * 1000, censored=True)
            raise
        except httpx.TimeoutException:
            self._observe_failure(idx, (time.monotonic() - t0) * 1000)
            return 504, {"detail": "upstream timeout"}
        except httpx.HTTPError as exc:
            self._observe_failure(idx, (time.monotonic() - t0) * 1000)
            return 502, {"detail": f"upstream error: {type(exc).__name__}"}
        elapsed_ms = (time.monotonic() - t0) * 1000
        try:
            body = resp.json()
        except ValueError:
            body = {"detail": "upstream returned a non-JSON body"}
        if is_valid_completion(resp.status_code, body) or 400 <= resp.status_code < 500:
            self._observe(idx, elapsed_ms)
        else:
            self._observe_failure(idx, elapsed_ms)
        return resp.status_code, body

    def _score(self, idx: int) -> float:
        ewma = self._ewma_ms[idx]
        return 0.0 if ewma is None else ewma

    def _pick(self, uses: list[int], first: bool) -> int:
        n = len(self._url_list)
        if first:
            if n == 1:
                return 0
            if self._rng.random() < self._hedge.explore:
                return self._rng.randrange(n)
            a, b = self._rng.sample(range(n), 2)
            return a if self._score(a) <= self._score(b) else b
        # prefer replicas not yet tried for this request unless they are much slower
        return min(range(n), key=lambda i: (self._score(i) * (1 + uses[i]), uses[i], self._rng.random()))

    def _observe(self, idx: int, elapsed_ms: float, censored: bool = False) -> None:
        current = self._ewma_ms[idx]
        # a cancelled attempt only tells us its latency is at least `elapsed_ms`
        if censored and current is not None and elapsed_ms <= current:
            return
        if current is None:
            self._ewma_ms[idx] = elapsed_ms
        else:
            a = self._hedge.ewma_alpha
            self._ewma_ms[idx] = (1 - a) * current + a * elapsed_ms

    def _observe_failure(self, idx: int, elapsed_ms: float) -> None:
        self.stats["failures"] += 1
        current = self._ewma_ms[idx]
        self._observe(idx, max(elapsed_ms, 1000.0, 2 * (current or 0.0)))

    # -- per-prompt affinity (LLM_HEDGE_AFFINITY=replica) ---------------------

    @staticmethod
    def _affinity_key(payload: dict) -> str:
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    def _affinity_lookup(self, payload: dict, uses: list[int]) -> tuple[str | None, int | None, bool]:
        if self._hedge.affinity != "replica":
            return None, None, False
        key = self._affinity_key(payload)
        entry = self._affinity.get(key)
        if entry is not None:
            self._affinity.move_to_end(key)
            return key, entry[0], False
        # reserve before any await so simultaneous same-prompt requests share the replica
        idx = self._pick(uses, first=True)
        self._affinity[key] = [idx, False]
        while len(self._affinity) > self._hedge.affinity_max_keys:
            self._affinity.popitem(last=False)
        return key, idx, True

    def _affinity_confirm(self, key: str | None, idx: int) -> None:
        entry = self._affinity.get(key) if key is not None else None
        if entry is not None and entry[0] == idx:
            entry[1] = True

    def _affinity_release(self, key: str | None, idx: int | None) -> None:
        entry = self._affinity.get(key) if key is not None else None
        if entry is not None and entry[0] == idx and not entry[1]:
            del self._affinity[key]
