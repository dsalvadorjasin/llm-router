"""Upstream pool: latency-aware routing + hedging inside a per-request budget.

One request is served as a small race:

* The router (``app.routing.LatencyRouter``) ranks upstreams; the best one gets
  the first attempt. With ``LLM_ROUTING=roundrobin`` the first attempt rotates.
* While nothing has completed, another duplicate (hedge) is fired every
  ``LLM_HEDGE_DELAY_MS`` to the currently best-ranked upstream that has not
  failed in this request (it may be one already in flight, so a later attempt
  is never forced onto the slowest upstream).
* Each attempt has its own timeout (adaptive, ``LLM_ATTEMPT_TIMEOUT_MS`` cap);
  a timeout, transport error, 5xx or invalid 2xx body fails over immediately.
  Attempts never outlive the ``LLM_REQUEST_BUDGET_MS`` deadline, the last one
  gets whatever budget is left, and at most ``LLM_MAX_ATTEMPTS`` are made.
* The first final response (2xx, or a client 4xx passed through) wins; all
  other attempts are cancelled and their bodies are never read.
* Some requests also send a background probe to the least-recently-successful
  upstream; its result only updates routing stats, its body is discarded.
"""
import asyncio
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from . import config
from .config import upstream_urls
from .request_context import request_log_fields, set_log_field
from .routing import LatencyRouter

# Name used by the timeout/retry strategy for the per-request log-field ContextVar.
upstream_log_fields = request_log_fields

_ADAPTIVE_MIN_SAMPLES = 20
_RETRY_RATIO_SLACK = 0.2


class _Health:
    """Time-decayed failure ratio of one upstream. Only orders retries; never excludes."""

    def __init__(self, halflife_s: float):
        self._halflife_s = halflife_s
        self._fail = 0.0
        self._total = 0.0
        self._stamp: float | None = None

    def _decay(self, now: float) -> None:
        if self._stamp is not None and self._halflife_s > 0:
            factor = 0.5 ** ((now - self._stamp) / self._halflife_s)
            self._fail *= factor
            self._total *= factor
        self._stamp = now

    def record(self, ok: bool, now: float) -> None:
        self._decay(now)
        self._total += 1.0
        if not ok:
            self._fail += 1.0

    def failure_ratio(self, now: float) -> float:
        self._decay(now)
        return self._fail / self._total if self._total > 1e-9 else 0.0


@dataclass
class _Outcome:
    url: str
    kind: str  # "final" | "status" (5xx) | "timeout" | "transport" | "invalid"
    status: int | None = None
    body: dict | None = None


def _body(resp: httpx.Response) -> dict | None:
    try:
        body = resp.json()
    except ValueError:
        if resp.status_code < 300:
            return None
        return {"detail": resp.text[:200] or "upstream error"}
    return body if isinstance(body, dict) else {"detail": body}


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 *,
                 router: LatencyRouter | None = None,
                 max_attempts: int | None = None,
                 attempt_timeout_ms: float | None = None,
                 clock: Callable[[], float] = time.monotonic,
                 rng: Callable[[], float] | None = None):
        self._url_list = list(urls or upstream_urls())
        self._index = {u: i for i, u in enumerate(self._url_list)}
        if router is None:
            kwargs = {"clock": clock}
            if rng is not None:
                kwargs["rng"] = rng
            router = LatencyRouter(self._url_list, **kwargs)
        self.router = router

        self._max_attempts = max(1, max_attempts if max_attempts is not None
                                 else config.max_attempts())
        timeout_ms = (attempt_timeout_ms if attempt_timeout_ms is not None
                      else config.attempt_timeout_ms())
        self._max_timeout_s = timeout_ms / 1000 if timeout_ms > 0 else None
        self._min_timeout_s = config.min_attempt_timeout_ms() / 1000
        if self._max_timeout_s is not None:
            self._min_timeout_s = min(self._min_timeout_s, self._max_timeout_s)
        self._budget_s = config.request_budget_ms() / 1000
        self._adaptive = config.adaptive_timeout()
        self._quantile = config.adaptive_timeout_quantile()
        self._multiplier = config.adaptive_timeout_multiplier()
        self._latencies: deque[float] = deque(maxlen=200)

        halflife_s = config.failure_halflife_ms() / 1000
        self._health = [_Health(halflife_s) for _ in self._url_list]
        self._suspect_ratio = config.suspect_failure_ratio()
        self._suspect_factor = min(1.0, config.suspect_timeout_factor())
        self._probe_every = config.suspect_probe_every()
        self._suspect_attempts = [0] * len(self._url_list)

        self._hedging = config.hedging_enabled()
        self._hedge_delay_s = config.hedge_delay_ms() / 1000
        self._probe_timeout_s = config.probe_timeout_ms() / 1000
        self._background: set[asyncio.Task] = set()

        # Attempt timeouts are enforced with asyncio.wait_for; the client
        # timeout is only a backstop just past the longest of them.
        backstop = max(self._budget_s, self._max_timeout_s or 0.0, self._probe_timeout_s) + 1.0
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(backstop), transport=transport)

    # --- timeouts (strategy A) ---------------------------------------------

    def attempt_timeout(self) -> float | None:
        """Current per-attempt timeout in seconds (before budget clamping)."""
        if self._max_timeout_s is None:
            return None
        if not self._adaptive or len(self._latencies) < _ADAPTIVE_MIN_SAMPLES:
            return self._max_timeout_s
        samples = sorted(self._latencies)
        q = samples[min(len(samples) - 1, int(self._quantile * len(samples)))]
        return min(self._max_timeout_s, max(self._min_timeout_s, q * self._multiplier))

    def _suspect_timeout(self, idx: int, timeout: float) -> float | None:
        """Short timeout for an upstream that has mostly been failing, or None.

        Every Nth attempt to it still gets the full timeout so a recovered
        upstream is noticed; short-timeout misses are inconclusive and are not
        recorded as failures.
        """
        if self._suspect_factor >= 1.0:
            return None
        if self._health[idx].failure_ratio(time.monotonic()) < self._suspect_ratio:
            return None
        self._suspect_attempts[idx] += 1
        if self._suspect_attempts[idx] % self._probe_every == 0:
            return None
        return timeout * self._suspect_factor

    # --- target selection ----------------------------------------------------

    def _pick_retry(self, failed: int, tried: list[int]) -> int:
        """Round-robin mode: next in rotation, skipping much-failing and tried upstreams."""
        n = len(self._url_list)
        if n == 1:
            return failed
        now = time.monotonic()
        candidates = [(failed + step) % n for step in range(1, n)]
        ratios = {i: self._health[i].failure_ratio(now) for i in candidates}
        best = min(ratios.values())
        return min(
            candidates,
            key=lambda i: (ratios[i] > best + _RETRY_RATIO_SLACK, i in tried, candidates.index(i)),
        )

    def _next_target(self, last: str, tried: list[int], failed: set[str]) -> str:
        if not self.router.latency_aware:
            return self._url_list[self._pick_retry(self._index[last], tried)]
        ranked = self.router.rank()
        candidates = [u for u in ranked if u not in failed]
        return (candidates or ranked)[0]

    # --- attempts ------------------------------------------------------------

    async def _attempt(self, url: str, payload: dict, timeout: float | None,
                       counted: bool) -> _Outcome:
        idx = self._index[url]
        token = self.router.start(url)
        start = time.monotonic()
        try:
            resp = await asyncio.wait_for(
                self._client.post(f"{url}/v1/completions", json=payload), timeout)
        except asyncio.CancelledError:
            self.router.abandon(url, token, lower_bound=True)
            raise
        except (TimeoutError, httpx.TimeoutException):
            self.router.timeout(url, token)
            if counted:
                self._health[idx].record(False, time.monotonic())
            return _Outcome(url, "timeout")
        except httpx.HTTPError:
            self.router.finish(url, token, ok=False)
            self._health[idx].record(False, time.monotonic())
            return _Outcome(url, "transport")
        except BaseException:
            self.router.abandon(url, token)
            raise
        now = time.monotonic()
        body = _body(resp)
        if body is None or resp.status_code >= 500:
            self.router.finish(url, token, ok=False)
            self._health[idx].record(False, now)
            if body is None:
                return _Outcome(url, "invalid")
            return _Outcome(url, "status", resp.status_code, body)
        self.router.finish(url, token, ok=True)
        self._health[idx].record(True, now)
        if resp.status_code < 300:
            self._latencies.append(now - start)
        return _Outcome(url, "final", resp.status_code, body)

    async def _probe(self, url: str, payload: dict) -> None:
        """Background measurement of one upstream; the response is discarded."""
        idx = self._index[url]
        token = self.router.start(url)
        try:
            resp = await asyncio.wait_for(
                self._client.post(f"{url}/v1/completions", json=payload), self._probe_timeout_s)
            ok = resp.status_code < 500 and _body(resp) is not None
        except asyncio.CancelledError:
            self.router.abandon(url, token)
            raise
        except Exception:
            ok = False
        self.router.finish(url, token, ok=ok)
        self._health[idx].record(ok, time.monotonic())

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    # --- the request ---------------------------------------------------------

    async def forward(self, payload: dict) -> tuple[int, dict]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._budget_s
        ranked, probe = self.router.select()
        if probe is not None:
            self._spawn(self._probe(probe, payload))
            ranked = [u for u in ranked if u != probe] or ranked

        pending: dict[asyncio.Task, str] = {}
        tried: list[int] = []
        failed: set[str] = set()
        hedged = False
        last_status: _Outcome | None = None
        last_kind = "timeout"

        def launch(url: str) -> None:
            idx = self._index[url]
            tried.append(idx)
            remaining = deadline - loop.time()
            is_last = len(tried) == self._max_attempts
            timeout = self.attempt_timeout()
            timeout = remaining if is_last or timeout is None else min(timeout, remaining)
            short = None if is_last else self._suspect_timeout(idx, timeout)
            task = asyncio.create_task(
                self._attempt(url, payload, short or timeout, counted=short is None))
            pending[task] = url

        def can_launch() -> bool:
            return len(tried) < self._max_attempts and deadline - loop.time() > 0

        launch(ranked[0])
        last_url = ranked[0]
        next_hedge_at = loop.time() + self._hedge_delay_s
        try:
            while pending:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                wait_s = remaining
                if self._hedging and len(tried) < self._max_attempts:
                    wait_s = min(wait_s, max(0.0, next_hedge_at - loop.time()))
                done, _ = await asyncio.wait(
                    pending.keys(), timeout=wait_s, return_when=asyncio.FIRST_COMPLETED)
                if not done:
                    if self._hedging and can_launch() and loop.time() >= next_hedge_at - 1e-3:
                        last_url = self._next_target(last_url, tried, failed)
                        launch(last_url)
                        hedged = True
                        next_hedge_at = loop.time() + self._hedge_delay_s
                    continue
                for task in done:
                    url = pending.pop(task)
                    outcome = task.result()
                    if outcome.kind == "final":
                        self._record(url, hedged)
                        return outcome.status, outcome.body
                    failed.add(url)
                    last_kind = outcome.kind
                    if outcome.kind == "status":
                        last_status = outcome
                if can_launch():
                    last_url = self._next_target(last_url, tried, failed)
                    launch(last_url)
                    next_hedge_at = loop.time() + self._hedge_delay_s
        finally:
            for task in pending:
                task.cancel()
                self._background.add(task)
                task.add_done_callback(self._background.discard)

        if last_status is not None:
            self._record(last_status.url, hedged)
            return last_status.status, last_status.body
        self._record("-", hedged)
        if last_kind == "timeout":
            return 504, {"detail": "upstream timeout"}
        return 502, {"detail": "upstream unavailable"}

    @staticmethod
    def _record(url: str, hedged: bool) -> None:
        set_log_field("upstream_url", url)
        set_log_field("hedged", hedged)

    async def aclose(self) -> None:
        for task in list(self._background):
            task.cancel()
        await self._client.aclose()
