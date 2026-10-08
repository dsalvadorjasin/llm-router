"""Combined behaviour: latency ranking + hedging inside a request budget."""
import asyncio
import time

import httpx

from app.request_context import request_log_fields
from app.routing import LatencyRouter
from app.upstream import UpstreamPool, _Outcome, _success_first

U1, U2, U3 = "http://u1:9000", "http://u2:9000", "http://u3:9000"


def _host(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


def _transport(delays: dict, hits: list[str], first_only: bool = False) -> httpx.MockTransport:
    """delays maps upstream -> seconds to sleep (only on its first call when first_only)."""
    seen: set[str] = set()

    async def handler(request: httpx.Request) -> httpx.Response:
        host = _host(request)
        hits.append(host)
        first = host not in seen
        seen.add(host)
        if first or not first_only:
            await asyncio.sleep(delays.get(host, 0.0))
        return httpx.Response(200, json={"completion": "ok", "from": host})

    return httpx.MockTransport(handler)


def _env(monkeypatch, **values):
    base = {"LLM_ADAPTIVE_TIMEOUT": 0, "LLM_ATTEMPT_TIMEOUT_MS": 2000,
            "LLM_REQUEST_BUDGET_MS": 3000, "LLM_HEDGE_DELAY_MS": 30, "LLM_HEDGING": 1}
    for key, value in {**base, **values}.items():
        monkeypatch.setenv(key, str(value))


def _seeded_router(urls: list[str], ewmas: dict[str, float], **kwargs) -> LatencyRouter:
    now = [0.0]
    router = LatencyRouter(urls, clock=lambda: now[0], probe_ratio=0.0, **kwargs)
    for url, ewma in ewmas.items():
        token = router.start(url)
        now[0] += ewma
        router.finish(url, token, ok=True)
        now[0] -= ewma
    return router


def _run(pool: UpstreamPool, settle_s: float = 0.05):
    async def go():
        fields: dict = {}
        request_log_fields.set(fields)
        start = time.monotonic()
        result = await pool.forward({"prompt": "p"})
        elapsed = time.monotonic() - start
        await asyncio.sleep(settle_s)
        await pool.aclose()
        return result, elapsed, fields

    return asyncio.run(go())


def test_cancelled_hedge_loser_does_not_seed_cold_ewma(monkeypatch):
    _env(monkeypatch)
    hits: list[str] = []
    router = LatencyRouter([U1, U2], probe_ratio=0.0)
    pool = UpstreamPool([U1, U2], _transport({U1: 0.3}, hits), router=router)
    (status, body), _, fields = _run(pool)
    assert (status, body, fields["hedged"]) == (200, {"completion": "ok", "from": U2}, True)
    snap = router.snapshot()
    assert snap[U1]["ewma_ms"] is None and snap[U1]["floor_ms"] > 0
    assert snap[U1]["penalty_ms"] == 0 and snap[U1]["inflight"] == 0
    # the loser's short elapsed time must not make it look fastest
    assert router.rank()[0] == U2


def test_ranking_orders_primary_and_hedge_targets(monkeypatch):
    _env(monkeypatch)
    hits: list[str] = []
    router = _seeded_router([U1, U2, U3], {U3: 0.1, U1: 0.15, U2: 0.5})
    pool = UpstreamPool([U1, U2, U3], _transport({U3: 0.3}, hits), router=router)
    (status, body), _, fields = _run(pool)
    assert hits == [U3, U1]
    assert (status, body, fields) == (200, {"completion": "ok", "from": U1}, {"upstream_url": U1, "hedged": True})


def test_later_hedge_reuses_best_upstream_instead_of_slowest(monkeypatch):
    _env(monkeypatch, LLM_MAX_ATTEMPTS=3)
    hits: list[str] = []
    router = _seeded_router([U1, U2, U3], {U1: 0.1, U2: 0.12, U3: 1.2})
    transport = _transport({U1: 0.5, U2: 0.5, U3: 0.0}, hits, first_only=True)
    pool = UpstreamPool([U1, U2, U3], transport, router=router)
    (status, body), elapsed, _ = _run(pool)
    assert hits == [U1, U2, U1]
    assert (status, body) == (200, {"completion": "ok", "from": U1})
    assert elapsed < 0.3


def test_hedges_never_run_past_the_request_budget(monkeypatch):
    _env(monkeypatch, LLM_REQUEST_BUDGET_MS=200, LLM_MAX_ATTEMPTS=20)
    hits: list[str] = []
    router = LatencyRouter([U1, U2, U3], probe_ratio=0.0)
    pool = UpstreamPool([U1, U2, U3], _transport({U1: 1, U2: 1, U3: 1}, hits), router=router)
    (status, body), elapsed, fields = _run(pool)
    assert status == 504 and "detail" in body
    assert elapsed < 0.35
    assert 2 <= len(hits) <= 8
    assert fields == {"upstream_url": "-", "hedged": True}
    assert all(s["inflight"] == 0 and s["penalty_ms"] == 0 for s in router.snapshot().values())


def test_failover_is_clamped_to_remaining_budget(monkeypatch):
    _env(monkeypatch, LLM_REQUEST_BUDGET_MS=300, LLM_MAX_ATTEMPTS=2, LLM_HEDGING=0)
    hits: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        hits.append(_host(request))
        if _host(request) == U1:
            return httpx.Response(503, json={"detail": "busy"})
        await asyncio.sleep(1.0)
        return httpx.Response(200, json={"completion": "late", "from": "late"})

    router = LatencyRouter([U1, U2], probe_ratio=0.0)
    pool = UpstreamPool([U1, U2], httpx.MockTransport(handler), router=router)
    (status, body), elapsed, _ = _run(pool)
    assert hits == [U1, U2]
    assert (status, body) == (503, {"detail": "busy"})
    assert 0.25 < elapsed < 0.45


def test_probe_updates_stats_but_its_body_is_never_returned(monkeypatch):
    _env(monkeypatch)
    hits: list[str] = []
    router = _seeded_router([U1, U2, U3], {U1: 0.1, U2: 0.12, U3: 1.2}, rng=lambda: 0.0)
    router.probe_ratio = 1.0
    router._stats[U3].last_success = -1.0  # least recently successful
    pool = UpstreamPool([U1, U2, U3], _transport({U3: 0.05}, hits), router=router)
    (status, body), _, fields = _run(pool, settle_s=0.15)
    assert (status, body) == (200, {"completion": "ok", "from": U1})
    assert fields == {"upstream_url": U1, "hedged": False}
    assert sorted(hits) == [U1, U3]
    assert router.snapshot()[U3]["ewma_ms"] < 1200  # probe sample folded in


def test_success_wins_over_client_error_completing_in_same_wait():
    async def go():
        loop = asyncio.get_running_loop()

        def done(outcome):
            fut = loop.create_future()
            fut.set_result(outcome)
            return fut

        error = done(_Outcome(U1, "final", 400, {"detail": "bad"}))
        success = done(_Outcome(U2, "final", 200, {"completion": "ok"}))
        return sorted([error, success], key=_success_first)[0] is success

    assert asyncio.run(go())
