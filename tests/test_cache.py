import asyncio

import httpx
from fastapi.testclient import TestClient

from app import main
from app.cache import ResponseCache, cache_key, forward_with_retry
from app.config import (response_cache_enabled, response_cache_max_entries,
                        response_cache_ttl_s)
from app.upstream import UpstreamPool

URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]


def _host(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


def _ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"completion": "ok", "signature": "ab" * 32})


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_config_defaults_enable_cache(monkeypatch):
    for name in ("ROUTER_RESPONSE_CACHE", "ROUTER_RESPONSE_CACHE_TTL_S",
                 "ROUTER_RESPONSE_CACHE_MAX_ENTRIES"):
        monkeypatch.delenv(name, raising=False)
    assert response_cache_enabled() is True
    assert response_cache_ttl_s() == 300
    assert response_cache_max_entries() == 4096


def test_config_env_overrides(monkeypatch):
    monkeypatch.setenv("ROUTER_RESPONSE_CACHE", "0")
    monkeypatch.setenv("ROUTER_RESPONSE_CACHE_TTL_S", "1.5")
    monkeypatch.setenv("ROUTER_RESPONSE_CACHE_MAX_ENTRIES", "7")
    assert response_cache_enabled() is False
    assert response_cache_ttl_s() == 1.5
    assert response_cache_max_entries() == 7


def test_cache_key_uses_prompt_and_max_tokens():
    assert cache_key({"prompt": "p", "max_tokens": 8}) == ("p", 8)
    assert cache_key({"prompt": "p", "max_tokens": 8}) != cache_key({"prompt": "p", "max_tokens": 9})


def test_hit_avoids_second_upstream_call():
    hits: list[str] = []

    async def handler(request):
        hits.append(_host(request))
        return _ok(request)

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))
    cache = ResponseCache(ttl_s=60, max_entries=10)

    async def run():
        payload = {"prompt": "p", "max_tokens": 8}
        fetch = lambda: forward_with_retry(pool, payload, 3)  # noqa: E731
        results = [await cache.get_or_fetch(cache_key(payload), fetch) for _ in range(3)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert hits == ["http://u1:9000"]
    assert all(r[0] == 200 for r in results)
    assert (cache.hits, cache.misses) == (2, 1)


def test_ttl_expiry_refetches():
    clock = FakeClock()
    cache = ResponseCache(ttl_s=10, max_entries=10, clock=clock)
    calls = 0

    async def fetch():
        nonlocal calls
        calls += 1
        return 200, {"completion": f"c{calls}"}

    async def run():
        a = await cache.get_or_fetch("k", fetch)
        clock.now = 9.9
        b = await cache.get_or_fetch("k", fetch)
        clock.now = 10.0
        c = await cache.get_or_fetch("k", fetch)
        return a, b, c

    a, b, c = asyncio.run(run())
    assert a == b == (200, {"completion": "c1"})
    assert c == (200, {"completion": "c2"})
    assert calls == 2


def test_lru_eviction_bounds_size():
    cache = ResponseCache(ttl_s=60, max_entries=2)
    cache.put("a", (200, {"completion": "a"}))
    cache.put("b", (200, {"completion": "b"}))
    assert cache.get("a") is not None  # a becomes most recently used
    cache.put("c", (200, {"completion": "c"}))
    assert len(cache) == 2
    assert cache.get("b") is None
    assert cache.get("a") is not None
    assert cache.get("c") is not None


def test_non_200_and_empty_completions_not_cached():
    cache = ResponseCache(ttl_s=60, max_entries=10)
    responses = [(503, {"detail": "overloaded"}), (200, {"completion": ""}),
                 (200, {"completion": "ok"})]

    async def fetch():
        return responses.pop(0)

    async def run():
        return [await cache.get_or_fetch("k", fetch) for _ in range(4)]

    results = asyncio.run(run())
    assert results[0][0] == 503
    assert results[1] == (200, {"completion": ""})
    assert results[2] == results[3] == (200, {"completion": "ok"})
    assert responses == []


def test_concurrent_identical_requests_single_flight():
    hits: list[str] = []
    release = asyncio.Event()

    async def handler(request):
        hits.append(_host(request))
        await release.wait()
        return _ok(request)

    async def run():
        pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))
        cache = ResponseCache(ttl_s=60, max_entries=10)
        payload = {"prompt": "burst", "max_tokens": 64}
        fetch = lambda: forward_with_retry(pool, payload, 3)  # noqa: E731
        tasks = [asyncio.create_task(cache.get_or_fetch(cache_key(payload), fetch))
                 for _ in range(20)]
        await asyncio.sleep(0.01)
        release.set()
        results = await asyncio.gather(*tasks)
        await pool.aclose()
        return cache, results

    cache, results = asyncio.run(run())
    assert len(hits) == 1
    assert all(r[0] == 200 for r in results)
    assert cache.coalesced == 19


def test_cancelled_leader_does_not_cancel_followers():
    release = asyncio.Event()

    async def fetch():
        await release.wait()
        return 200, {"completion": "ok"}

    async def run():
        cache = ResponseCache(ttl_s=60, max_entries=10)
        leader = asyncio.create_task(cache.get_or_fetch("k", fetch))
        await asyncio.sleep(0)
        follower = asyncio.create_task(cache.get_or_fetch("k", fetch))
        await asyncio.sleep(0)
        leader.cancel()
        release.set()
        return await follower, cache.get("k")

    result, cached = asyncio.run(run())
    assert result == cached == (200, {"completion": "ok"})


def test_retry_moves_to_next_replica_on_error_and_transport_failure():
    hits: list[str] = []

    async def handler(request):
        host = _host(request)
        hits.append(host)
        if len(hits) == 1:
            return httpx.Response(503, json={"detail": "overloaded"})
        if len(hits) == 2:
            raise httpx.ConnectError("boom", request=request)
        return _ok(request)

    async def run():
        pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))
        result = await forward_with_retry(pool, {"prompt": "p"}, 3)
        await pool.aclose()
        return result

    status, body = asyncio.run(run())
    assert status == 200 and body["completion"] == "ok"
    assert hits == URLS


def test_retry_returns_last_error_when_all_replicas_fail():
    async def handler(request):
        return httpx.Response(503, json={"detail": "overloaded"})

    async def run():
        pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))
        result = await forward_with_retry(pool, {"prompt": "p"}, 3)
        await pool.aclose()
        return result

    assert asyncio.run(run()) == (503, {"detail": "overloaded"})


def test_retry_all_transport_failures_returns_502():
    async def handler(request):
        raise httpx.ConnectError("down", request=request)

    async def run():
        pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))
        result = await forward_with_retry(pool, {"prompt": "p"}, 3)
        await pool.aclose()
        return result

    status, _ = asyncio.run(run())
    assert status == 502


class CountingPool:
    def __init__(self):
        self.calls: list[dict] = []

    async def forward(self, payload):
        self.calls.append(payload)
        return 200, {"completion": "ok", "signature": "ab" * 32}

    async def aclose(self):
        pass


def test_generate_endpoint_serves_repeats_from_cache(monkeypatch):
    monkeypatch.delenv("ROUTER_RESPONSE_CACHE", raising=False)
    pool = CountingPool()
    with TestClient(main.app) as c:
        main.app.state.pool = pool
        r1 = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
        r2 = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
        r3 = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 9})
    assert r1.json() == r2.json() and r1.status_code == r2.status_code == r3.status_code == 200
    assert pool.calls == [{"prompt": "hi", "max_tokens": 8}, {"prompt": "hi", "max_tokens": 9}]


def test_generate_endpoint_cache_disabled_restores_passthrough(monkeypatch):
    monkeypatch.setenv("ROUTER_RESPONSE_CACHE", "0")
    pool = CountingPool()
    with TestClient(main.app) as c:
        main.app.state.pool = pool
        assert main.app.state.response_cache is None
        for _ in range(2):
            c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
    assert len(pool.calls) == 2
