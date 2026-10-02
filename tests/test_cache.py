import asyncio

import pytest

from app.cache import ResponseCache


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _fetcher(results: list[tuple[int, dict]], calls: list[int], delay: float = 0.0):
    async def fetch():
        calls.append(1)
        await asyncio.sleep(delay)
        return results[min(len(calls), len(results)) - 1]

    return fetch


def test_hit_returns_cached_result_without_refetch():
    cache = ResponseCache(max_entries=8, ttl_s=60)
    calls: list[int] = []
    fetch = _fetcher([(200, {"completion": "a"})], calls)

    async def run():
        return [await cache.get_or_fetch("k", fetch) for _ in range(3)]

    assert asyncio.run(run()) == [(200, {"completion": "a"})] * 3
    assert len(calls) == 1


def test_concurrent_misses_share_one_fetch():
    cache = ResponseCache(max_entries=8, ttl_s=60)
    calls: list[int] = []
    fetch = _fetcher([(200, {"completion": "a"})], calls, delay=0.05)

    async def run():
        return await asyncio.gather(*(cache.get_or_fetch("k", fetch) for _ in range(5)))

    assert asyncio.run(run()) == [(200, {"completion": "a"})] * 5
    assert len(calls) == 1


def test_non_200_results_are_not_cached():
    cache = ResponseCache(max_entries=8, ttl_s=60)
    calls: list[int] = []
    fetch = _fetcher([(503, {"detail": "overloaded"}), (200, {"completion": "a"})], calls)

    async def run():
        return [await cache.get_or_fetch("k", fetch) for _ in range(3)]

    assert asyncio.run(run()) == [
        (503, {"detail": "overloaded"}),
        (200, {"completion": "a"}),
        (200, {"completion": "a"}),
    ]
    assert len(calls) == 2


def test_fetch_errors_propagate_and_are_not_cached():
    cache = ResponseCache(max_entries=8, ttl_s=60)
    calls: list[int] = []

    async def failing():
        calls.append(1)
        raise RuntimeError("boom")

    async def run():
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await cache.get_or_fetch("k", failing)

    asyncio.run(run())
    assert len(calls) == 2
    assert len(cache) == 0


def test_entries_expire_after_ttl():
    clock = FakeClock()
    cache = ResponseCache(max_entries=8, ttl_s=10, clock=clock)
    calls: list[int] = []
    fetch = _fetcher([(200, {"completion": "a"})], calls)

    async def run():
        await cache.get_or_fetch("k", fetch)
        clock.now = 9.9
        await cache.get_or_fetch("k", fetch)
        clock.now = 10.0
        await cache.get_or_fetch("k", fetch)

    asyncio.run(run())
    assert len(calls) == 2


def test_least_recently_used_entry_is_evicted():
    cache = ResponseCache(max_entries=2, ttl_s=60)
    calls: list[str] = []

    def fetch_for(key: str):
        async def fetch():
            calls.append(key)
            return 200, {"completion": key}

        return fetch

    async def run():
        for key in ["a", "b", "a", "c", "a", "b"]:
            await cache.get_or_fetch(key, fetch_for(key))

    asyncio.run(run())
    assert calls == ["a", "b", "c", "b"]


def test_zero_max_entries_disables_cache():
    cache = ResponseCache(max_entries=0, ttl_s=60)
    calls: list[int] = []
    fetch = _fetcher([(200, {"completion": "a"})], calls)

    async def run():
        for _ in range(3):
            await cache.get_or_fetch("k", fetch)

    asyncio.run(run())
    assert len(calls) == 3
    assert len(cache) == 0
