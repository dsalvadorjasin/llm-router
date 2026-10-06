import asyncio

import httpx
import pytest

from app.balancer import LatencyAwareBalancer, RoundRobinBalancer
from app.config import RoutingSettings, routing_settings, upstream_urls
from app.upstream import UpstreamPool

ROUND_ROBIN = RoutingSettings(strategy="round_robin")


def test_upstream_urls_default(monkeypatch):
    monkeypatch.delenv("LLM_SERVICE_URLS", raising=False)
    assert upstream_urls() == [
        "http://localhost:9001",
        "http://localhost:9002",
        "http://localhost:9003",
    ]


def test_upstream_urls_env(monkeypatch):
    monkeypatch.setenv("LLM_SERVICE_URLS", "http://a:1, http://b:2")
    assert upstream_urls() == ["http://a:1", "http://b:2"]


def _recording_transport(hits: list[str]) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        hits.append(f"{request.url.scheme}://{request.url.host}:{request.url.port}")
        return httpx.Response(200, json={"completion": "ok"})

    return httpx.MockTransport(handler)


def test_round_robin_and_passthrough():
    hits: list[str] = []
    urls = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]
    pool = UpstreamPool(urls=urls, transport=_recording_transport(hits), settings=ROUND_ROBIN)

    async def run():
        results = [await pool.forward({"prompt": "p"}) for _ in range(4)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert hits == ["http://u1:9000", "http://u2:9000", "http://u3:9000", "http://u1:9000"]
    assert all(r == (200, {"completion": "ok"}) for r in results)


def test_routing_settings_default_is_latency_aware(monkeypatch):
    for name in ("ROUTER_STRATEGY", "ROUTER_EWMA_ALPHA", "ROUTER_PROBE_INTERVAL_S",
                 "ROUTER_ERROR_PENALTY_S", "ROUTER_OUTSTANDING_WEIGHT"):
        monkeypatch.delenv(name, raising=False)
    assert routing_settings() == RoutingSettings()
    assert routing_settings().strategy == "latency"
    pool = UpstreamPool(urls=["http://u1:9000"])
    assert isinstance(pool.balancer, LatencyAwareBalancer)
    asyncio.run(pool.aclose())


def test_routing_settings_from_env(monkeypatch):
    monkeypatch.setenv("ROUTER_STRATEGY", " Round_Robin ")
    monkeypatch.setenv("ROUTER_EWMA_ALPHA", "0.5")
    monkeypatch.setenv("ROUTER_PROBE_INTERVAL_S", "2")
    monkeypatch.setenv("ROUTER_ERROR_PENALTY_S", "9")
    monkeypatch.setenv("ROUTER_OUTSTANDING_WEIGHT", "0.25")
    assert routing_settings() == RoutingSettings(
        strategy="round_robin", ewma_alpha=0.5, probe_interval_s=2.0,
        error_penalty_s=9.0, outstanding_weight=0.25,
    )
    pool = UpstreamPool(urls=["http://u1:9000"])
    assert isinstance(pool.balancer, RoundRobinBalancer)
    asyncio.run(pool.aclose())


def test_routing_settings_rejects_unknown_strategy(monkeypatch):
    monkeypatch.setenv("ROUTER_STRATEGY", "fastest")
    with pytest.raises(ValueError, match="ROUTER_STRATEGY"):
        routing_settings()


def test_round_robin_env_config_off_retains_original_rotation(monkeypatch):
    monkeypatch.setenv("ROUTER_STRATEGY", "round_robin")
    hits: list[str] = []
    urls = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]
    pool = UpstreamPool(urls=urls, transport=_slow_transport(hits, {"u1": 0.05}))

    async def run():
        for _ in range(6):
            await pool.forward({"prompt": "p"})
        await pool.aclose()

    asyncio.run(run())
    assert hits == ["u1", "u2", "u3", "u1", "u2", "u3"]


def _slow_transport(hits: list[str], delays: dict[str, float],
                    statuses: dict[str, int] | None = None) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        hits.append(host)
        await asyncio.sleep(delays.get(host, 0.0))
        status = (statuses or {}).get(host, 200)
        return httpx.Response(status, json={"completion": host})

    return httpx.MockTransport(handler)


def _latency_pool(transport, **overrides) -> UpstreamPool:
    settings = RoutingSettings(**{"probe_interval_s": 60.0, **overrides})
    urls = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]
    return UpstreamPool(urls=urls, transport=transport, settings=settings)


def test_latency_aware_pool_routes_away_from_slow_replica():
    hits: list[str] = []
    pool = _latency_pool(_slow_transport(hits, {"u3": 0.2}))

    async def run():
        results = [await pool.forward({"prompt": "p"}) for _ in range(20)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert hits[:3] == ["u1", "u2", "u3"]
    assert "u3" not in hits[3:]
    assert all(status == 200 for status, _ in results)


def test_latency_aware_pool_balances_concurrent_load():
    hits: list[str] = []
    pool = _latency_pool(_slow_transport(hits, {"u1": 0.02, "u2": 0.02, "u3": 0.02}))

    async def run():
        for _ in range(3):
            await pool.forward({"prompt": "warmup"})
        await asyncio.gather(*[pool.forward({"prompt": "p"}) for _ in range(9)])
        await pool.aclose()

    asyncio.run(run())
    burst = hits[3:]
    assert sorted(burst.count(u) for u in ("u1", "u2", "u3")) == [3, 3, 3]
    assert all(s.outstanding == 0 for s in pool.balancer.stats)


def test_upstream_5xx_is_passed_through_and_penalised():
    hits: list[str] = []
    pool = _latency_pool(_slow_transport(hits, {}, statuses={"u1": 503}), error_penalty_s=5.0)

    async def run():
        results = [await pool.forward({"prompt": "p"}) for _ in range(6)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert results[0] == (503, {"completion": "u1"})
    assert hits.count("u1") == 1
    assert pool.balancer.stats[0].ewma_s >= 5.0


def test_transport_error_propagates_and_releases_replica():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "u1":
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(200, json={"completion": "ok"})

    pool = _latency_pool(httpx.MockTransport(handler))

    async def run():
        with pytest.raises(httpx.ConnectError):
            await pool.forward({"prompt": "p"})
        results = [await pool.forward({"prompt": "p"}) for _ in range(4)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert all(r == (200, {"completion": "ok"}) for r in results)
    stats = pool.balancer.stats
    assert stats[0].outstanding == 0
    assert stats[0].ewma_s >= RoutingSettings().error_penalty_s
