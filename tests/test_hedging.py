import asyncio

import httpx
import pytest

from app.hedging import Hedger, HedgeSettings, is_valid
from app.upstream import UpstreamPool

SIG = "a" * 64
URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]


def _host(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


def _transport(behaviour, hits):
    """behaviour(url, n) -> (delay_s, response) where n counts calls to that url."""
    counts: dict[str, int] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        url = _host(request)
        counts[url] = counts.get(url, 0) + 1
        hits.append(url)
        delay, resp = behaviour(url, counts[url])
        await asyncio.sleep(delay)
        return resp

    return httpx.MockTransport(handler)


def _ok(url="x"):
    return httpx.Response(200, json={"completion": f"from {url}", "signature": SIG})


def _hedger(behaviour, hits, **settings):
    client = httpx.AsyncClient(transport=_transport(behaviour, hits))
    return Hedger(URLS, client, HedgeSettings(**settings)), client


def _run(coro):
    return asyncio.run(coro)


def test_is_valid():
    assert is_valid(200, {"completion": "x", "signature": SIG})
    assert is_valid(200, {"completion": "x"})
    assert not is_valid(200, {"completion": "", "signature": SIG})
    assert not is_valid(200, {"completion": "x", "signature": "nope"})
    assert not is_valid(503, {"completion": "x", "signature": SIG})
    assert not is_valid(200, ["x"])


def test_hedge_fires_when_primary_is_slow():
    hits: list[str] = []

    def behaviour(url, n):
        return (5.0 if url == URLS[0] else 0.0), _ok(url)

    async def run():
        hedger, client = _hedger(behaviour, hits, fixed_delay_ms=20)
        loop = asyncio.get_running_loop()
        start = loop.time()
        status, body = await hedger.forward({"prompt": "p"})
        took = loop.time() - start
        await client.aclose()
        return status, body, took

    status, body, took = _run(run())
    assert status == 200 and body["completion"] == f"from {URLS[1]}"
    assert hits == [URLS[0], URLS[1]]
    assert took < 1.0


def test_error_fails_over_immediately_without_waiting_for_hedge_delay():
    hits: list[str] = []

    def behaviour(url, n):
        if url == URLS[0]:
            return 0.0, httpx.Response(503, json={"detail": "model overloaded"})
        return 0.0, _ok(url)

    async def run():
        hedger, client = _hedger(behaviour, hits, fixed_delay_ms=10_000)
        loop = asyncio.get_running_loop()
        start = loop.time()
        result = await hedger.forward({"prompt": "p"})
        took = loop.time() - start
        await client.aclose()
        return result, took

    (status, body), took = _run(run())
    assert status == 200 and body["completion"] == f"from {URLS[1]}"
    assert took < 1.0


def test_transport_error_and_invalid_body_are_retried_elsewhere():
    hits: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        url = _host(request)
        hits.append(url)
        if url == URLS[0]:
            raise httpx.ConnectError("boom", request=request)
        if url == URLS[1]:
            return httpx.Response(200, json={"completion": "x", "signature": "bad"})
        return _ok(url)

    async def run():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        hedger = Hedger(URLS, client, HedgeSettings(fixed_delay_ms=10_000))
        result = await hedger.forward({"prompt": "p"})
        await client.aclose()
        return result

    status, body = _run(run())
    assert status == 200 and body["completion"] == f"from {URLS[2]}"
    assert hits == URLS


def test_all_replicas_failing_returns_last_upstream_error():
    hits: list[str] = []

    def behaviour(url, n):
        return 0.0, httpx.Response(503, json={"detail": "model overloaded"})

    async def run():
        hedger, client = _hedger(behaviour, hits, fixed_delay_ms=10)
        result = await hedger.forward({"prompt": "p"})
        await client.aclose()
        return result

    status, body = _run(run())
    assert status == 503 and body == {"detail": "model overloaded"}
    assert set(hits) == set(URLS)


def test_per_attempt_timeout_is_bounded():
    hits: list[str] = []

    def behaviour(url, n):
        return (5.0 if url == URLS[0] else 0.0), _ok(url)

    async def run():
        hedger, client = _hedger(behaviour, hits, fixed_delay_ms=10_000,
                                 max_hedged_attempts=1, attempt_timeout_s=0.05)
        result = await hedger.forward({"prompt": "p"})
        await client.aclose()
        return result

    status, body = _run(run())
    assert status == 200 and body["completion"] == f"from {URLS[1]}"


def test_slow_replica_is_deprioritised_but_probed_again_when_stale():
    now = [0.0]
    hedger = Hedger(URLS, httpx.AsyncClient(), HedgeSettings(probe_interval_s=2.0),
                    clock=lambda: now[0])
    for _ in range(5):
        hedger._record(URLS[0], 120, censored=False)
        hedger._record(URLS[1], 130, censored=False)
        hedger._record(URLS[2], 1300, censored=False)
    primaries = [hedger.plan()[0][0] for _ in range(6)]
    assert URLS[2] not in primaries
    plan, hedge_limit = hedger.plan()
    assert URLS[2] not in plan[:hedge_limit] and URLS[2] in plan

    now[0] = 10.0  # its stats are stale: it gets a primary turn again
    primaries = [hedger.plan()[0][0] for _ in range(3)]
    assert URLS[2] in primaries


def test_recovered_replica_is_trusted_again():
    hedger = Hedger(URLS, httpx.AsyncClient(), HedgeSettings())
    hedger._record(URLS[0], 120, censored=False)
    hedger._record(URLS[1], 120, censored=False)
    hedger._record_failure(URLS[2])
    assert hedger._slow(hedger._clock()) == {URLS[2]}
    hedger._record(URLS[2], 125, censored=False)
    assert hedger._slow(hedger._clock()) == set()


def test_adaptive_delay_tracks_recent_latencies():
    hedger = Hedger(URLS, httpx.AsyncClient(),
                    HedgeSettings(initial_delay_ms=200, min_delay_ms=50, max_delay_ms=400,
                                  delay_quantile=0.75, min_samples=20))
    assert hedger.hedge_delay_s() == pytest.approx(0.2)
    for i in range(40):
        hedger._record(URLS[i % 2], 100 + i, censored=False)
    assert 0.12 <= hedger.hedge_delay_s() <= 0.14
    for i in range(300):
        hedger._record(URLS[i % 3], 10_000, censored=False)
    assert hedger.hedge_delay_s() == pytest.approx(0.4)


def test_pool_uses_hedging_by_default_and_env_disables_it(monkeypatch):
    monkeypatch.delenv("LLM_HEDGING", raising=False)
    assert UpstreamPool(urls=URLS)._hedger is not None
    monkeypatch.setenv("LLM_HEDGING", "0")
    assert UpstreamPool(urls=URLS)._hedger is None


def test_pool_hedges_slow_primary(monkeypatch):
    monkeypatch.setenv("LLM_HEDGE_DELAY_MS", "20")
    hits: list[str] = []

    def behaviour(url, n):
        return (5.0 if url == URLS[0] else 0.0), _ok(url)

    async def run():
        pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    status, body = _run(run())
    assert status == 200 and body["completion"] == f"from {URLS[1]}"
