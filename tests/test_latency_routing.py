import asyncio
import random
from collections import Counter

import httpx
import pytest

from app import config
from app.upstream import UpstreamPool

URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]


def _host(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


def _transport(delays: dict[str, float], hits: list[str], fail: set[str] | None = None,
               status: dict[str, int] | None = None) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        host = _host(request)
        hits.append(host)
        if fail and host in fail:
            raise httpx.ConnectError("refused", request=request)
        await asyncio.sleep(delays.get(host, 0.0))
        code = (status or {}).get(host, 200)
        return httpx.Response(code, json={"completion": "ok", "host": host})

    return httpx.MockTransport(handler)


def _pool(transport, **kw) -> UpstreamPool:
    kw.setdefault("strategy", "latency")
    kw.setdefault("explore_rate", 0.0)
    kw.setdefault("probe_interval_s", 60.0)
    kw.setdefault("ewma_half_life_s", 0.0)
    kw.setdefault("rng", random.Random(0))
    return UpstreamPool(urls=URLS, transport=transport, **kw)


async def _burst(pool: UpstreamPool, n: int, concurrency: int) -> list[tuple[int, dict]]:
    sem = asyncio.Semaphore(concurrency)

    async def one():
        async with sem:
            return await pool.forward({"prompt": "p"})

    return await asyncio.gather(*(one() for _ in range(n)))


def test_slower_replica_gets_less_traffic():
    hits: list[str] = []
    delays = {URLS[0]: 0.005, URLS[1]: 0.005, URLS[2]: 0.05}
    pool = _pool(_transport(delays, hits), explore_rate=0.05)

    async def run():
        results = await _burst(pool, 300, concurrency=4)
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert all(status == 200 for status, _ in results)
    counts = Counter(hits)
    assert counts[URLS[2]] >= 1
    assert counts[URLS[2]] < 0.1 * len(hits)
    assert counts[URLS[0]] > 3 * counts[URLS[2]]
    assert counts[URLS[1]] > 3 * counts[URLS[2]]


def test_cold_start_samples_every_replica_once():
    hits: list[str] = []
    pool = _pool(_transport({}, hits))

    async def run():
        for _ in range(3):
            await pool.forward({"prompt": "p"})
        await pool.aclose()

    asyncio.run(run())
    assert sorted(hits) == sorted(URLS)


def test_slow_replica_is_never_starved_and_recovers():
    hits: list[str] = []
    delays = {URLS[0]: 0.002, URLS[1]: 0.002, URLS[2]: 0.05}
    pool = _pool(_transport(delays, hits), probe_interval_s=0.05, ewma_half_life_s=0.02)

    async def run():
        await _burst(pool, 150, concurrency=2)
        slow_phase = Counter(hits)[URLS[2]]
        delays[URLS[2]] = 0.001  # replica recovers and is now the fastest
        hits.clear()
        await asyncio.sleep(0.06)
        await _burst(pool, 300, concurrency=2)
        await pool.aclose()
        return slow_phase

    slow_phase = asyncio.run(run())
    # Probes keep reaching the slow replica while it is slow...
    assert slow_phase >= 2
    # ...so once it speeds up it is re-discovered and wins a real share of traffic.
    assert Counter(hits)[URLS[2]] > 0.25 * len(hits)


def test_failover_on_connection_error_returns_200():
    hits: list[str] = []
    pool = _pool(_transport({}, hits, fail={URLS[1]}))

    async def run():
        results = [await pool.forward({"prompt": "p"}) for _ in range(30)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert all(status == 200 for status, _ in results)
    assert all(body["host"] != URLS[1] for _, body in results)
    # The failing replica is penalised, not hammered.
    assert Counter(hits)[URLS[1]] < 5


@pytest.mark.parametrize("bad", ["5xx", "timeout"])
def test_failover_on_5xx_and_on_timeout(bad):
    hits: list[str] = []
    if bad == "5xx":
        transport = _transport({}, hits, status={URLS[1]: 503})
    else:
        transport = _transport({URLS[1]: 1.0}, hits)
    pool = _pool(transport, attempt_timeout_s=0.1)

    async def run():
        results = [await pool.forward({"prompt": "p"}) for _ in range(10)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert all(status == 200 for status, _ in results)
    assert all(body["host"] != URLS[1] for _, body in results)
    assert URLS[1] in hits


def test_all_attempts_failing_returns_gateway_error():
    hits: list[str] = []
    pool = _pool(_transport({}, hits, fail=set(URLS)), max_attempts=2)

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    status, body = asyncio.run(run())
    assert status == 502
    assert "upstream error" in body["detail"]
    assert len(hits) == 2 and len(set(hits)) == 2


def test_client_errors_are_not_retried():
    hits: list[str] = []
    pool = _pool(_transport({}, hits, status={u: 422 for u in URLS}))

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    status, _ = asyncio.run(run())
    assert status == 422
    assert len(hits) == 1


def test_round_robin_selectable_via_env(monkeypatch):
    monkeypatch.setenv("ROUTER_STRATEGY", "round-robin")
    hits: list[str] = []
    delays = {URLS[2]: 0.02}
    pool = UpstreamPool(urls=URLS, transport=_transport(delays, hits))
    assert pool.strategy == "round_robin"

    async def run():
        for _ in range(6):
            await pool.forward({"prompt": "p"})
        await pool.aclose()

    asyncio.run(run())
    assert hits == URLS * 2


def test_round_robin_retries_on_next_replica():
    hits: list[str] = []
    pool = UpstreamPool(urls=URLS, transport=_transport({}, hits, fail={URLS[0]}),
                        strategy="round_robin")

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    status, body = asyncio.run(run())
    assert status == 200 and body["host"] == URLS[1]
    assert hits == URLS[:2]


def test_config_defaults_and_validation(monkeypatch):
    for name in ("ROUTER_STRATEGY", "ROUTER_EWMA_ALPHA", "ROUTER_EXPLORE_RATE",
                 "ROUTER_ATTEMPT_TIMEOUT_S", "ROUTER_MAX_ATTEMPTS"):
        monkeypatch.delenv(name, raising=False)
    assert config.routing_strategy() == "latency"
    assert 0 < config.ewma_alpha() <= 1
    assert 0 <= config.explore_rate() < 1
    assert config.attempt_timeout_s() > 0
    assert config.max_attempts() == 2
    monkeypatch.setenv("ROUTER_EWMA_ALPHA", "0.5")
    assert config.ewma_alpha() == 0.5
    monkeypatch.setenv("ROUTER_STRATEGY", "random")
    with pytest.raises(ValueError):
        config.routing_strategy()
