import asyncio

import pytest
from fastapi.testclient import TestClient

from app import main
from app.cache import ResponseCache, cache_key, cache_max_entries, cache_ttl_s

SIG = "ab" * 32


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class FakePool:
    def __init__(self, status=200, delay=0.0, fail=None):
        self.status = status
        self.delay = delay
        self.fail = fail
        self.calls: list[dict] = []
        self.started = 0
        self.cancelled = 0
        self.closed = False

    async def forward(self, payload):
        self.calls.append(dict(payload))
        self.started += 1
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        if self.fail is not None:
            raise self.fail
        if self.status != 200:
            return self.status, {"detail": "model overloaded"}
        n = len(self.calls)
        return 200, {
            "id": f"cmpl-{n}",
            "model": "mock-large",
            "completion": f"answer {payload['prompt']} {payload['max_tokens']}",
            "signature": SIG,
            "usage": {"prompt_tokens": 1, "completion_tokens": 3},
        }

    async def aclose(self):
        self.closed = True


def run(coro):
    return asyncio.run(coro)


def test_repeated_identical_prompt_hits_upstream_once():
    pool = FakePool()
    cache = ResponseCache(pool)

    async def go():
        return [await cache.forward({"prompt": "p", "max_tokens": 8}) for _ in range(5)]

    results = run(go())
    assert len(pool.calls) == 1
    assert all(r == results[0] for r in results)
    status, body = results[0]
    assert status == 200
    assert body["signature"] == SIG
    assert set(body) == {"id", "model", "completion", "signature", "usage"}


def test_cached_body_is_isolated_from_caller_mutation():
    cache = ResponseCache(FakePool())

    async def go():
        _, first = await cache.forward({"prompt": "p", "max_tokens": 8})
        first["completion"] = "tampered"
        first["usage"]["prompt_tokens"] = 99
        return await cache.forward({"prompt": "p", "max_tokens": 8})

    _, second = run(go())
    assert second["completion"] == "answer p 8"
    assert second["usage"]["prompt_tokens"] == 1


def test_concurrent_identical_misses_are_coalesced():
    pool = FakePool(delay=0.05)
    cache = ResponseCache(pool)

    async def go():
        return await asyncio.gather(
            *(cache.forward({"prompt": "same", "max_tokens": 8}) for _ in range(10))
        )

    results = run(go())
    assert len(pool.calls) == 1
    assert all(r == results[0] for r in results)
    assert results[0][0] == 200


def test_ttl_expiry_refetches():
    pool = FakePool()
    clock = FakeClock()
    cache = ResponseCache(pool, ttl_s=60, clock=clock)
    payload = {"prompt": "p", "max_tokens": 8}

    async def go():
        await cache.forward(payload)
        clock.now += 59.9
        await cache.forward(payload)
        assert len(pool.calls) == 1
        clock.now += 0.1
        await cache.forward(payload)
        assert len(pool.calls) == 2
        await cache.forward(payload)

    run(go())
    assert len(pool.calls) == 2


def test_capacity_evicts_least_recently_used():
    pool = FakePool()
    cache = ResponseCache(pool, max_entries=2)

    def p(prompt):
        return {"prompt": prompt, "max_tokens": 8}

    async def go():
        await cache.forward(p("a"))
        await cache.forward(p("b"))
        await cache.forward(p("a"))  # refresh "a"; "b" is now LRU
        await cache.forward(p("c"))  # evicts "b"
        assert len(cache) == 2
        assert len(pool.calls) == 3
        await cache.forward(p("a"))
        await cache.forward(p("c"))
        assert len(pool.calls) == 3
        await cache.forward(p("b"))
        assert len(pool.calls) == 4

    run(go())
    assert len(cache) == 2


def test_max_tokens_is_part_of_the_key():
    pool = FakePool()
    cache = ResponseCache(pool)

    async def go():
        a = await cache.forward({"prompt": "p", "max_tokens": 8})
        b = await cache.forward({"prompt": "p", "max_tokens": 16})
        a2 = await cache.forward({"prompt": "p", "max_tokens": 8})
        return a, b, a2

    a, b, a2 = run(go())
    assert [c["max_tokens"] for c in pool.calls] == [8, 16]
    assert a[1]["completion"] == "answer p 8"
    assert b[1]["completion"] == "answer p 16"
    assert a2 == a


def test_error_status_is_never_cached():
    pool = FakePool(status=503)
    cache = ResponseCache(pool)
    payload = {"prompt": "p", "max_tokens": 8}

    async def go():
        first = await cache.forward(payload)
        pool.status = 200
        second = await cache.forward(payload)
        third = await cache.forward(payload)
        return first, second, third

    first, second, third = run(go())
    assert first == (503, {"detail": "model overloaded"})
    assert second[0] == 200 and third == second
    assert len(pool.calls) == 2
    assert len(cache) == 1


def test_upstream_exception_propagates_and_is_not_cached():
    pool = FakePool(fail=RuntimeError("connect failed"))
    cache = ResponseCache(pool)
    payload = {"prompt": "p", "max_tokens": 8}

    async def go():
        with pytest.raises(RuntimeError):
            await cache.forward(payload)
        pool.fail = None
        return await cache.forward(payload)

    status, _ = run(go())
    assert status == 200
    assert len(pool.calls) == 2


def test_cancelled_waiter_does_not_affect_other_waiters():
    pool = FakePool(delay=0.05)
    cache = ResponseCache(pool)
    payload = {"prompt": "p", "max_tokens": 8}

    async def go():
        t1 = asyncio.create_task(cache.forward(payload))
        t2 = asyncio.create_task(cache.forward(payload))
        await asyncio.sleep(0.01)
        t1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t1
        return await t2

    status, body = run(go())
    assert status == 200 and body["signature"] == SIG
    assert len(pool.calls) == 1
    assert pool.cancelled == 0
    assert len(cache) == 1


def test_last_waiter_cancelled_cancels_upstream_and_allows_fresh_fetch():
    pool = FakePool(delay=0.05)
    cache = ResponseCache(pool)
    payload = {"prompt": "p", "max_tokens": 8}

    async def go():
        t = asyncio.create_task(cache.forward(payload))
        await asyncio.sleep(0.01)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        await asyncio.sleep(0)
        assert pool.cancelled == 1
        assert len(cache) == 0
        return await cache.forward(payload)

    status, _ = run(go())
    assert status == 200
    assert len(pool.calls) == 2


def test_disabled_cache_passes_through():
    pool = FakePool()
    cache = ResponseCache(pool, ttl_s=0)

    async def go():
        for _ in range(3):
            await cache.forward({"prompt": "p", "max_tokens": 8})

    run(go())
    assert len(pool.calls) == 3


def test_cache_key_is_canonical_and_rejects_unknown_fields():
    assert cache_key({"max_tokens": 8, "prompt": "p"}) == ("p", 8)
    assert cache_key({"prompt": "p"}) == ("p", 64)
    assert cache_key({"prompt": "p", "max_tokens": 8, "model": "x"}) is None


def test_aclose_closes_wrapped_pool():
    pool = FakePool()
    run(ResponseCache(pool).aclose())
    assert pool.closed


def test_config_env(monkeypatch):
    monkeypatch.delenv("RESPONSE_CACHE_TTL_S", raising=False)
    monkeypatch.delenv("RESPONSE_CACHE_MAX_ENTRIES", raising=False)
    assert cache_ttl_s() == 60.0
    assert cache_max_entries() == 1024
    monkeypatch.setenv("RESPONSE_CACHE_TTL_S", "5")
    monkeypatch.setenv("RESPONSE_CACHE_MAX_ENTRIES", "10")
    assert cache_ttl_s() == 5.0
    assert cache_max_entries() == 10


def _client_with_cache(pool):
    c = TestClient(main.app)
    c.__enter__()
    assert isinstance(main.app.state.pool, ResponseCache)
    main.app.state.pool = ResponseCache(pool)
    return c


def test_lifespan_wires_cache_and_generate_serves_cached_body():
    pool = FakePool()
    c = _client_with_cache(pool)
    r1 = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
    r2 = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
    r3 = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 9})
    c.__exit__(None, None, None)
    assert r1.status_code == r2.status_code == r3.status_code == 200
    assert r1.json() == r2.json()
    assert r2.json()["signature"] == SIG
    assert len(pool.calls) == 2


def test_generate_does_not_cache_error_responses():
    pool = FakePool(status=503)
    c = _client_with_cache(pool)
    r1 = c.post("/v1/generate", json={"prompt": "hi"})
    pool.status = 200
    r2 = c.post("/v1/generate", json={"prompt": "hi"})
    c.__exit__(None, None, None)
    assert r1.status_code == 503
    assert r2.status_code == 200
    assert len(pool.calls) == 2


def test_chat_path_uses_cache_for_identical_prompt():
    pool = FakePool()
    c = _client_with_cache(pool)
    cids = [c.post("/v1/conversations", json={}).json()["id"] for _ in range(2)]
    replies = [
        c.post("/v1/chat", json={"conversation_id": cid, "message": "What is a hash map?"})
        for cid in cids
    ]
    c.__exit__(None, None, None)
    assert [r.status_code for r in replies] == [200, 200]
    assert replies[0].json()["message"]["content"] == replies[1].json()["message"]["content"]
    assert len(pool.calls) == 1
