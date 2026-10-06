"""All latency strategies enabled together (the defaults): cache -> hedger -> router."""
import asyncio

import httpx
from fastapi.testclient import TestClient

from app import main
from app.upstream import UpstreamPool

SIG = "b" * 64
URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]


def _host(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


def _ok(url):
    return httpx.Response(200, json={"completion": f"from {url}", "signature": SIG})


def _clear_strategy_env(monkeypatch):
    for name in ("LLM_HEDGING", "ROUTER_LATENCY_AWARE", "ROUTER_RESPONSE_CACHE"):
        monkeypatch.delenv(name, raising=False)


def test_defaults_enable_router_and_hedger_together(monkeypatch):
    _clear_strategy_env(monkeypatch)
    pool = UpstreamPool(urls=URLS)
    assert pool._router is not None and pool._hedger is not None
    assert pool._hedger._router is pool._router
    assert pool.fails_over


def test_each_strategy_can_be_disabled_independently(monkeypatch):
    _clear_strategy_env(monkeypatch)
    monkeypatch.setenv("LLM_HEDGING", "0")
    pool = UpstreamPool(urls=URLS)
    assert pool._hedger is None and pool._router is not None
    monkeypatch.setenv("LLM_HEDGING", "1")
    monkeypatch.setenv("ROUTER_LATENCY_AWARE", "0")
    pool = UpstreamPool(urls=URLS)
    assert pool._router is None and pool._hedger is not None and pool._hedger._router is None
    monkeypatch.setenv("LLM_HEDGING", "0")
    pool = UpstreamPool(urls=URLS)
    assert pool._router is None and pool._hedger is None and not pool.fails_over


def test_router_steers_primary_and_hedges_away_from_slow_replica(monkeypatch):
    _clear_strategy_env(monkeypatch)
    monkeypatch.setenv("LLM_HEDGE_DELAY_MS", "20")
    hits: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        url = _host(request)
        hits.append(url)
        await asyncio.sleep(0.3 if url == URLS[2] else 0.0)
        return _ok(url)

    async def run():
        pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))
        router = pool._router
        for _ in range(3):
            router.record_success(0, 100.0)
            router.record_success(1, 100.0)
            router.record_success(2, 1300.0)
        results = [await pool.forward({"prompt": "p"}) for _ in range(6)]
        outstanding = [r.outstanding for r in router._replicas]
        await pool.aclose()
        return results, outstanding

    results, outstanding = asyncio.run(run())
    assert all(status == 200 for status, _ in results)
    assert URLS[2] not in hits
    assert hits == [URLS[0], URLS[1]] * 3
    assert outstanding == [0, 0, 0]


def test_hedge_outcomes_feed_the_router(monkeypatch):
    _clear_strategy_env(monkeypatch)
    monkeypatch.setenv("LLM_HEDGE_DELAY_MS", "20")

    async def handler(request: httpx.Request) -> httpx.Response:
        url = _host(request)
        if url == URLS[0]:
            await asyncio.sleep(5)
        if url == URLS[1]:
            return httpx.Response(503, json={"detail": "overloaded"})
        return _ok(url)

    async def run():
        pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))
        result = await pool.forward({"prompt": "p"})
        router = pool._router
        scores = router._scores(router.clock())
        await pool.aclose()
        return result, scores

    (status, body), scores = asyncio.run(run())
    assert status == 200 and body["completion"] == f"from {URLS[2]}"
    # u1 was hedged away from (censored sample), u2 failed (penalised), u3 won
    assert scores[2] < scores[0] and scores[2] < scores[1]


def test_rank_puts_tied_replicas_first_in_rotation_order(monkeypatch):
    _clear_strategy_env(monkeypatch)
    router = UpstreamPool(urls=URLS)._router
    assert router.rank() == (URLS, [])
    assert router.rank() == ([URLS[1], URLS[2], URLS[0]], [])
    router.record_success(0, 100.0)
    router.record_success(1, 100.0)
    router.record_success(2, 1000.0)
    preferred, others = router.rank()
    assert URLS[2] not in preferred and others == [URLS[2]]


def test_generate_endpoint_cache_in_front_of_hedged_pool(monkeypatch, tmp_path):
    _clear_strategy_env(monkeypatch)
    monkeypatch.setenv("APP_DB_PATH", str(tmp_path / "app.db"))
    hits: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        url = _host(request)
        hits.append(url)
        if url == URLS[0]:
            return httpx.Response(500, json={"detail": "boom"})
        return _ok(url)

    with TestClient(main.app) as client:
        main.app.state.pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))
        first = client.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
        second = client.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
    assert first.status_code == 200 and second.status_code == 200
    assert first.json() == second.json() == {"completion": f"from {URLS[1]}", "signature": SIG}
    # one miss: u1 failed, pool failed over to u2; the repeat was served from cache
    assert hits == [URLS[0], URLS[1]]
