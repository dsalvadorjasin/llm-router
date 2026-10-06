import asyncio

import httpx
import pytest

from app.cache import ResponseCache
from app.config import (
    response_cache_coalesce,
    response_cache_enabled,
    response_cache_max_entries,
    response_cache_ttl_s,
)

VALID = {"completion": "ok", "signature": "ab" * 32}


def make_fetch(calls, status=200, body=None):
    async def fetch(payload):
        calls.append(payload)
        return status, VALID if body is None else body

    return fetch


def test_miss_then_hit():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10)
    fetch = make_fetch(calls)

    async def run():
        r1 = await cache.get_or_fetch({"prompt": "p"}, fetch)
        r1[1]["completion"] = "mutated"
        r2 = await cache.get_or_fetch({"prompt": "p"}, fetch)
        return r1, r2

    r1, r2 = asyncio.run(run())
    assert len(calls) == 1
    assert r1[0] == 200 and r2[0] == 200
    assert r2[1] == VALID
    assert cache.stats.hits == 1
    assert cache.stats.misses == 1


def test_key_includes_all_payload_fields():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10)
    fetch = make_fetch(calls)

    async def run():
        await cache.get_or_fetch({"prompt": "p", "max_tokens": 8}, fetch)
        await cache.get_or_fetch({"prompt": "p", "max_tokens": 9}, fetch)
        await cache.get_or_fetch({"prompt": "p", "max_tokens": 8, "model": "m1"}, fetch)
        await cache.get_or_fetch({"prompt": "p", "max_tokens": 8}, fetch)

    asyncio.run(run())
    assert len(calls) == 3
    assert cache.stats.hits == 1


def test_ttl_expiry():
    now = [0.0]
    calls = []
    cache = ResponseCache(ttl_s=10, max_entries=10, clock=lambda: now[0])
    fetch = make_fetch(calls)

    async def run():
        await cache.get_or_fetch({"prompt": "p"}, fetch)
        now[0] = 11.0
        await cache.get_or_fetch({"prompt": "p"}, fetch)

    asyncio.run(run())
    assert len(calls) == 2
    assert cache.stats.expirations == 1


def test_lru_eviction():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=2)
    fetch = make_fetch(calls)
    a = {"prompt": "a"}
    b = {"prompt": "b"}
    c = {"prompt": "c"}

    async def run():
        await cache.get_or_fetch(a, fetch)
        await cache.get_or_fetch(b, fetch)
        await cache.get_or_fetch(a, fetch)  # hit, refreshes A
        await cache.get_or_fetch(c, fetch)  # evicts B
        assert cache.stats.evictions == 1
        await cache.get_or_fetch(a, fetch)  # A still a hit
        await cache.get_or_fetch(b, fetch)  # miss, B was evicted

    asyncio.run(run())
    assert len(calls) == 4
    assert cache.stats.hits == 2


def test_error_status_not_cached():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10)
    fetch = make_fetch(calls, status=503, body={"detail": "overloaded"})

    async def run():
        r1 = await cache.get_or_fetch({"prompt": "p"}, fetch)
        r2 = await cache.get_or_fetch({"prompt": "p"}, fetch)
        return r1, r2

    r1, r2 = asyncio.run(run())
    assert len(calls) == 2
    assert r1[0] == 503 and r2[0] == 503
    assert len(cache) == 0


def test_empty_completion_not_cached():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10)
    fetch = make_fetch(calls, body={"completion": "", "signature": "ab" * 32})

    async def run():
        await cache.get_or_fetch({"prompt": "p"}, fetch)
        await cache.get_or_fetch({"prompt": "p"}, fetch)

    asyncio.run(run())
    assert len(calls) == 2
    assert len(cache) == 0


def test_malformed_signature_not_cached():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10)
    fetch = make_fetch(calls, body={"completion": "ok", "signature": "xyz"})

    async def run():
        await cache.get_or_fetch({"prompt": "p"}, fetch)
        await cache.get_or_fetch({"prompt": "p"}, fetch)

    asyncio.run(run())
    assert len(calls) == 2
    assert len(cache) == 0


def test_fetch_exception_not_cached():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10)

    async def fetch(payload):
        calls.append(payload)
        raise httpx.ConnectError("boom")

    async def run():
        for _ in range(2):
            with pytest.raises(httpx.ConnectError):
                await cache.get_or_fetch({"prompt": "p"}, fetch)

    asyncio.run(run())
    assert len(calls) == 2
    assert len(cache) == 0


def test_coalescing_single_flight():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10)

    async def run():
        event = asyncio.Event()

        async def fetch(payload):
            calls.append(payload)
            await event.wait()
            return 200, dict(VALID)

        tasks = [
            asyncio.ensure_future(cache.get_or_fetch({"prompt": "p"}, fetch))
            for _ in range(10)
        ]
        for _ in range(3):
            await asyncio.sleep(0)
        event.set()
        results = await asyncio.gather(*tasks)
        hit = await cache.get_or_fetch({"prompt": "p"}, fetch)
        return results, hit

    results, hit = asyncio.run(run())
    assert len(calls) == 1
    assert all(r == (200, VALID) for r in results)
    assert cache.stats.coalesced == 9
    assert cache.stats.misses == 1
    assert hit == (200, VALID)
    assert cache.stats.hits == 1


def test_failing_leader_waiters_fall_through():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10)

    async def run():
        event = asyncio.Event()

        async def fetch(payload):
            calls.append(payload)
            if len(calls) == 1:
                await event.wait()
                return 503, {"detail": "overloaded"}
            return 200, dict(VALID)

        tasks = [
            asyncio.ensure_future(cache.get_or_fetch({"prompt": "p"}, fetch))
            for _ in range(4)
        ]
        for _ in range(3):
            await asyncio.sleep(0)
        event.set()
        return await asyncio.gather(*tasks)

    results = asyncio.run(run())
    assert results[0][0] == 503
    assert all(r == (200, VALID) for r in results[1:])
    assert cache.stats.fallthroughs == 3


def test_exception_leader_waiters_fall_through():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10)

    async def run():
        event = asyncio.Event()

        async def fetch(payload):
            calls.append(payload)
            if len(calls) == 1:
                await event.wait()
                raise httpx.ConnectError("boom")
            return 200, dict(VALID)

        tasks = [
            asyncio.ensure_future(cache.get_or_fetch({"prompt": "p"}, fetch))
            for _ in range(4)
        ]
        for _ in range(3):
            await asyncio.sleep(0)
        event.set()
        return await asyncio.gather(*tasks, return_exceptions=True)

    results = asyncio.run(run())
    assert isinstance(results[0], httpx.ConnectError)
    assert all(r == (200, VALID) for r in results[1:])
    assert cache.stats.fallthroughs == 3


def test_leader_cancellation_waiters_still_served():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10)

    async def run():
        event = asyncio.Event()

        async def fetch(payload):
            calls.append(payload)
            await event.wait()
            return 200, dict(VALID)

        leader = asyncio.ensure_future(cache.get_or_fetch({"prompt": "p"}, fetch))
        waiters = [
            asyncio.ensure_future(cache.get_or_fetch({"prompt": "p"}, fetch))
            for _ in range(3)
        ]
        for _ in range(3):
            await asyncio.sleep(0)
        leader.cancel()
        with pytest.raises(asyncio.CancelledError):
            await leader
        event.set()
        return await asyncio.gather(*waiters)

    results = asyncio.run(run())
    assert all(r == (200, VALID) for r in results)
    assert len(calls) == 1


def test_cancelled_leader_fetch_still_populates_cache():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10)

    async def run():
        started = asyncio.Event()
        release = asyncio.Event()

        async def fetch(payload):
            calls.append(payload)
            started.set()
            await release.wait()
            return 200, dict(VALID)

        leader = asyncio.create_task(cache.get_or_fetch({"prompt": "p"}, fetch))
        await started.wait()
        leader.cancel()
        with pytest.raises(asyncio.CancelledError):
            await leader

        release.set()
        while cache._inflight:
            await asyncio.sleep(0)

        result = await cache.get_or_fetch({"prompt": "p"}, fetch)
        return result

    assert asyncio.run(run()) == (200, VALID)
    assert len(calls) == 1
    assert cache.stats.hits == 1
    assert cache.stats.stores == 1


def test_zero_ttl_never_stores():
    calls = []
    cache = ResponseCache(ttl_s=0, max_entries=10)
    fetch = make_fetch(calls)

    async def run():
        await cache.get_or_fetch({"prompt": "p"}, fetch)
        await cache.get_or_fetch({"prompt": "p"}, fetch)

    asyncio.run(run())
    assert len(calls) == 2
    assert len(cache) == 0


def test_zero_max_entries_never_stores():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=0)
    fetch = make_fetch(calls)

    async def run():
        await cache.get_or_fetch({"prompt": "p"}, fetch)
        await cache.get_or_fetch({"prompt": "p"}, fetch)

    asyncio.run(run())
    assert len(calls) == 2
    assert len(cache) == 0


def test_coalesce_disabled_fetches_per_caller():
    calls = []
    cache = ResponseCache(ttl_s=60, max_entries=10, coalesce=False)

    async def run():
        event = asyncio.Event()

        async def fetch(payload):
            calls.append(payload)
            await event.wait()
            return 200, dict(VALID)

        tasks = [
            asyncio.ensure_future(cache.get_or_fetch({"prompt": "p"}, fetch))
            for _ in range(3)
        ]
        for _ in range(3):
            await asyncio.sleep(0)
        event.set()
        return await asyncio.gather(*tasks)

    asyncio.run(run())
    assert len(calls) == 3


def test_config_defaults(monkeypatch):
    for name in (
        "RESPONSE_CACHE_ENABLED",
        "RESPONSE_CACHE_TTL_S",
        "RESPONSE_CACHE_MAX_ENTRIES",
        "RESPONSE_CACHE_COALESCE",
    ):
        monkeypatch.delenv(name, raising=False)
    assert response_cache_enabled() is True
    assert response_cache_ttl_s() == 300.0
    assert response_cache_max_entries() == 1024
    assert response_cache_coalesce() is True


def test_config_env_parsing(monkeypatch):
    monkeypatch.setenv("RESPONSE_CACHE_ENABLED", "OFF")
    assert response_cache_enabled() is False
    assert response_cache_coalesce() is False
    monkeypatch.setenv("RESPONSE_CACHE_ENABLED", "yes")
    assert response_cache_enabled() is True
    monkeypatch.setenv("RESPONSE_CACHE_TTL_S", "12.5")
    assert response_cache_ttl_s() == 12.5
    monkeypatch.setenv("RESPONSE_CACHE_MAX_ENTRIES", "7")
    assert response_cache_max_entries() == 7
    monkeypatch.setenv("RESPONSE_CACHE_COALESCE", "0")
    assert response_cache_coalesce() is False
    monkeypatch.setenv("RESPONSE_CACHE_COALESCE", "True")
    assert response_cache_coalesce() is True
