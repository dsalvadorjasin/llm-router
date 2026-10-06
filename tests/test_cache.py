import asyncio

import pytest

from app.cache import ResponseCache


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _fetcher(results, calls):
    async def fetch():
        calls.append(1)
        return results.pop(0)
    return fetch


def test_miss_then_hit_calls_upstream_once():
    cache = ResponseCache(ttl_s=60, max_entries=8)
    calls: list = []
    fetch = _fetcher([(200, {"completion": "a", "signature": "s"})], calls)

    async def run():
        first = await cache.get_or_fetch(("p", 64), fetch)
        second = await cache.get_or_fetch(("p", 64), fetch)
        return first, second

    first, second = asyncio.run(run())
    assert first == ((200, {"completion": "a", "signature": "s"}), "miss")
    assert second == ((200, {"completion": "a", "signature": "s"}), "hit")
    assert len(calls) == 1


def test_key_includes_max_tokens():
    cache = ResponseCache(ttl_s=60, max_entries=8)
    cache.put(ResponseCache.key("p", 64), {"completion": "long"})
    assert cache.get(ResponseCache.key("p", 64)) == {"completion": "long"}
    assert cache.get(ResponseCache.key("p", 8)) is None
    assert cache.get(ResponseCache.key("q", 64)) is None


def test_entries_expire_after_ttl():
    clock = FakeClock()
    cache = ResponseCache(ttl_s=10, max_entries=8, clock=clock)
    cache.put(("p", 64), {"completion": "a"})
    clock.now += 9.9
    assert cache.get(("p", 64)) == {"completion": "a"}
    clock.now += 0.1
    assert cache.get(("p", 64)) is None
    assert len(cache) == 0


def test_lru_eviction_is_bounded():
    cache = ResponseCache(ttl_s=60, max_entries=2)
    cache.put(("a", 1), {"v": "a"})
    cache.put(("b", 1), {"v": "b"})
    assert cache.get(("a", 1)) == {"v": "a"}  # a is now most recently used
    cache.put(("c", 1), {"v": "c"})
    assert len(cache) == 2
    assert cache.get(("b", 1)) is None
    assert cache.get(("a", 1)) == {"v": "a"}
    assert cache.get(("c", 1)) == {"v": "c"}


def test_mutating_returned_or_stored_body_does_not_leak():
    cache = ResponseCache(ttl_s=60, max_entries=8)
    original = {"completion": "a", "usage": {"completion_tokens": 3}}
    cache.put(("p", 64), original)
    original["completion"] = "mutated"
    original["usage"]["completion_tokens"] = 99

    got = cache.get(("p", 64))
    assert got == {"completion": "a", "usage": {"completion_tokens": 3}}
    got["usage"]["completion_tokens"] = 42
    assert cache.get(("p", 64)) == {"completion": "a", "usage": {"completion_tokens": 3}}


def test_coalesced_waiters_get_independent_copies():
    cache = ResponseCache(ttl_s=60, max_entries=8)

    async def run():
        release = asyncio.Event()

        async def fetch():
            await release.wait()
            return 200, {"usage": {"n": 1}}

        t1 = asyncio.create_task(cache.get_or_fetch(("p", 64), fetch))
        t2 = asyncio.create_task(cache.get_or_fetch(("p", 64), fetch))
        await asyncio.sleep(0)
        release.set()
        return await t1, await t2

    (r1, o1), (r2, o2) = asyncio.run(run())
    assert {o1, o2} == {"miss", "coalesced"}
    r1[1]["usage"]["n"] = 7
    assert r2[1] == {"usage": {"n": 1}}
    assert cache.get(("p", 64)) == {"usage": {"n": 1}}


@pytest.mark.parametrize("status", [400, 429, 500, 503])
def test_errors_are_not_cached(status):
    cache = ResponseCache(ttl_s=60, max_entries=8)
    calls: list = []
    fetch = _fetcher([(status, {"detail": "boom"}), (200, {"completion": "ok"})], calls)

    async def run():
        first = await cache.get_or_fetch(("p", 64), fetch)
        second = await cache.get_or_fetch(("p", 64), fetch)
        return first, second

    first, second = asyncio.run(run())
    assert first == ((status, {"detail": "boom"}), "miss")
    assert second == ((200, {"completion": "ok"}), "miss")
    assert len(calls) == 2
    assert cache.get(("p", 64)) == {"completion": "ok"}


def test_exceptions_propagate_and_are_not_cached():
    cache = ResponseCache(ttl_s=60, max_entries=8)
    calls: list = []

    async def failing():
        calls.append(1)
        raise ConnectionError("upstream down")

    async def run():
        with pytest.raises(ConnectionError):
            await cache.get_or_fetch(("p", 64), failing)
        with pytest.raises(ConnectionError):
            await cache.get_or_fetch(("p", 64), failing)

    asyncio.run(run())
    assert len(calls) == 2
    assert len(cache) == 0


def test_concurrent_same_key_requests_coalesce_into_one_upstream_call():
    cache = ResponseCache(ttl_s=60, max_entries=8)
    calls: list = []

    async def run():
        release = asyncio.Event()

        async def fetch():
            calls.append(1)
            await release.wait()
            return 200, {"completion": "shared"}

        tasks = [asyncio.create_task(cache.get_or_fetch(("p", 64), fetch)) for _ in range(10)]
        await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(*tasks)

    results = asyncio.run(run())
    assert len(calls) == 1
    assert all(r == (200, {"completion": "shared"}) for r, _ in results)
    outcomes = [o for _, o in results]
    assert outcomes.count("miss") == 1
    assert outcomes.count("coalesced") == 9


def test_coalesced_error_is_shared_but_not_cached():
    cache = ResponseCache(ttl_s=60, max_entries=8)
    calls: list = []

    async def run():
        release = asyncio.Event()

        async def fetch():
            calls.append(1)
            await release.wait()
            return 503, {"detail": "overloaded"}

        tasks = [asyncio.create_task(cache.get_or_fetch(("p", 64), fetch)) for _ in range(3)]
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(*tasks)
        await asyncio.sleep(0)
        return results

    results = asyncio.run(run())
    assert len(calls) == 1
    assert all(r == (503, {"detail": "overloaded"}) for r, _ in results)
    assert len(cache) == 0
    assert cache._inflight == {}


def test_cancelled_waiter_does_not_cancel_shared_fetch():
    cache = ResponseCache(ttl_s=60, max_entries=8)
    calls: list = []

    async def run():
        release = asyncio.Event()

        async def fetch():
            calls.append(1)
            await release.wait()
            return 200, {"completion": "ok"}

        leader = asyncio.create_task(cache.get_or_fetch(("p", 64), fetch))
        follower = asyncio.create_task(cache.get_or_fetch(("p", 64), fetch))
        await asyncio.sleep(0)
        leader.cancel()
        await asyncio.sleep(0)
        release.set()
        return await follower, leader.cancelled()

    (result, outcome), leader_cancelled = asyncio.run(run())
    assert leader_cancelled
    assert result == (200, {"completion": "ok"})
    assert outcome == "coalesced"
    assert len(calls) == 1
    assert cache.get(("p", 64)) == {"completion": "ok"}


def test_coalescing_disabled_issues_independent_calls():
    cache = ResponseCache(ttl_s=60, max_entries=8, coalesce=False)
    calls: list = []

    async def run():
        release = asyncio.Event()

        async def fetch():
            calls.append(1)
            await release.wait()
            return 200, {"completion": "ok"}

        tasks = [asyncio.create_task(cache.get_or_fetch(("p", 64), fetch)) for _ in range(3)]
        await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(*tasks)

    results = asyncio.run(run())
    assert len(calls) == 3
    assert [o for _, o in results] == ["miss"] * 3
    assert cache.get(("p", 64)) == {"completion": "ok"}


@pytest.mark.parametrize("kwargs", [{"ttl_s": 0, "max_entries": 1}, {"ttl_s": 1, "max_entries": 0}])
def test_rejects_invalid_bounds(kwargs):
    with pytest.raises(ValueError):
        ResponseCache(**kwargs)
