import asyncio

import httpx

from app.config import upstream_urls
from app.upstream import UpstreamPool


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
    pool = UpstreamPool(urls=urls, transport=_recording_transport(hits))

    async def run():
        results = [await pool.forward({"prompt": "p"}) for _ in range(4)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert hits == ["http://u1:9000", "http://u2:9000", "http://u3:9000", "http://u1:9000"]
    assert all(r == (200, {"completion": "ok"}) for r in results)


def _delayed_transport(delays: dict[str, float], hits: list[str],
                       failing: frozenset[str] = frozenset()) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        hits.append(host)
        if host in failing:
            raise httpx.ConnectError("connection refused", request=request)
        await asyncio.sleep(delays.get(host, 0))
        return httpx.Response(200, json={"completion": host})

    return httpx.MockTransport(handler)


def _forward_once(pool: UpstreamPool) -> tuple[int, dict]:
    async def run():
        try:
            return await pool.forward({"prompt": "p"})
        finally:
            await pool.aclose()

    return asyncio.run(run())


def test_fast_primary_does_not_hedge():
    hits: list[str] = []
    pool = UpstreamPool(urls=["http://u1:9000", "http://u2:9000"],
                        transport=_delayed_transport({"u1": 0.01}, hits),
                        hedge_delay_s=0.2)
    assert _forward_once(pool) == (200, {"completion": "u1"})
    assert hits == ["u1"]


def test_slow_primary_is_hedged_to_next_replica():
    hits: list[str] = []
    pool = UpstreamPool(urls=["http://u1:9000", "http://u2:9000", "http://u3:9000"],
                        transport=_delayed_transport({"u1": 2.0, "u2": 0.01}, hits),
                        hedge_delay_s=0.05)
    assert _forward_once(pool) == (200, {"completion": "u2"})
    assert hits == ["u1", "u2"]


def test_hedges_stagger_across_replicas_up_to_max_attempts():
    hits: list[str] = []
    pool = UpstreamPool(urls=["http://u1:9000", "http://u2:9000", "http://u3:9000"],
                        transport=_delayed_transport({"u1": 0.5, "u2": 0.2, "u3": 0.01}, hits),
                        hedge_delay_s=0.05, max_attempts=2)
    assert _forward_once(pool) == (200, {"completion": "u2"})
    assert hits == ["u1", "u2"]


def test_transport_failure_fails_over_immediately():
    hits: list[str] = []
    pool = UpstreamPool(urls=["http://u1:9000", "http://u2:9000"],
                        transport=_delayed_transport({}, hits, failing=frozenset({"u1"})),
                        hedge_delay_s=10.0)
    assert _forward_once(pool) == (200, {"completion": "u2"})
    assert hits == ["u1", "u2"]


def test_all_attempts_failing_raises():
    hits: list[str] = []
    pool = UpstreamPool(urls=["http://u1:9000", "http://u2:9000"],
                        transport=_delayed_transport({}, hits, failing=frozenset({"u1", "u2"})),
                        hedge_delay_s=0.05)
    try:
        _forward_once(pool)
    except httpx.ConnectError:
        pass
    else:
        raise AssertionError("expected ConnectError")
    assert hits == ["u1", "u2"]


def test_max_attempts_one_disables_hedging():
    hits: list[str] = []
    pool = UpstreamPool(urls=["http://u1:9000", "http://u2:9000"],
                        transport=_delayed_transport({"u1": 0.2}, hits),
                        hedge_delay_s=0.01, max_attempts=1)
    assert _forward_once(pool) == (200, {"completion": "u1"})
    assert hits == ["u1"]
