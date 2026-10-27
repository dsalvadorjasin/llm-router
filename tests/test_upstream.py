import asyncio

import httpx
import pytest

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


def _scripted_transport(hits: list[str], failing: set[str]) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        url = f"{request.url.scheme}://{request.url.host}:{request.url.port}"
        hits.append(url)
        if url in failing:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={"completion": "ok", "from": url})

    return httpx.MockTransport(handler)


URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]


def test_bounded_timeout_from_env(monkeypatch):
    monkeypatch.setenv("LLM_UPSTREAM_TIMEOUT_S", "2.5")
    pool = UpstreamPool(urls=URLS)
    assert pool._client.timeout == httpx.Timeout(2.5)
    asyncio.run(pool.aclose())


def test_default_timeout_is_bounded(monkeypatch):
    monkeypatch.delenv("LLM_UPSTREAM_TIMEOUT_S", raising=False)
    pool = UpstreamPool(urls=URLS)
    assert pool._client.timeout == httpx.Timeout(10.0)
    asyncio.run(pool.aclose())


def test_transport_error_retries_next_url_and_skips_it_during_cooldown(caplog):
    hits: list[str] = []
    pool = UpstreamPool(urls=URLS, transport=_scripted_transport(hits, {"http://u2:9000"}),
                        cooldown_s=60)

    async def run():
        results = [await pool.forward({"prompt": "p"}) for _ in range(4)]
        await pool.aclose()
        return results

    with caplog.at_level("INFO", logger="llm-router.upstream"):
        results = asyncio.run(run())

    assert [r[0] for r in results] == [200, 200, 200, 200]
    assert [r[1]["from"] for r in results] == [
        "http://u1:9000", "http://u3:9000", "http://u1:9000", "http://u3:9000",
    ]
    # u2 is tried once, then skipped while in cooldown; u1/u3 split the traffic
    assert hits == ["http://u1:9000", "http://u2:9000", "http://u3:9000",
                    "http://u1:9000", "http://u3:9000"]
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1 and "url=http://u2:9000" in warnings[0].getMessage()
    infos = [r.getMessage() for r in caplog.records if r.levelname == "INFO"]
    assert any("upstream url=http://u3:9000 status=200" in m for m in infos)


def test_url_returns_after_cooldown_and_success_resets(monkeypatch):
    hits: list[str] = []
    failing = {"http://u1:9000"}
    pool = UpstreamPool(urls=URLS, transport=_scripted_transport(hits, failing), cooldown_s=30)
    clock = [1000.0]
    monkeypatch.setattr("app.upstream.time.monotonic", lambda: clock[0])

    async def run():
        await pool.forward({"prompt": "p"})  # u1 fails -> u2
        failing.clear()
        clock[0] += 31  # cooldown over
        await pool.forward({"prompt": "p"})  # u2
        await pool.forward({"prompt": "p"})  # u3
        status, body = await pool.forward({"prompt": "p"})  # u1 back in rotation
        await pool.aclose()
        return body

    body = asyncio.run(run())
    assert body["from"] == "http://u1:9000"
    assert pool._failed_at == {}


@pytest.mark.parametrize("hedging,expected_hits", [
    (True, ["http://u1:9000", "http://u2:9000", "http://u3:9000"]),
    (False, ["http://u1:9000", "http://u2:9000"]),  # LLM_HEDGING=0: one failover retry
])
def test_all_attempts_fail_raises_httpx_error(hedging, expected_hits):
    hits: list[str] = []
    pool = UpstreamPool(urls=URLS, transport=_scripted_transport(hits, set(URLS)),
                        hedging=hedging)

    async def run():
        try:
            await pool.forward({"prompt": "p"})
        finally:
            await pool.aclose()

    try:
        asyncio.run(run())
    except httpx.HTTPError:
        pass
    else:
        raise AssertionError("expected httpx.HTTPError")
    assert hits == expected_hits


def test_all_in_cooldown_still_tries_upstreams():
    hits: list[str] = []
    failing = set(URLS)
    pool = UpstreamPool(urls=URLS, transport=_scripted_transport(hits, failing), cooldown_s=60)

    async def run():
        try:
            await pool.forward({"prompt": "p"})
        except httpx.HTTPError:
            pass
        try:
            await pool.forward({"prompt": "p"})
        except httpx.HTTPError:
            pass
        failing.clear()
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    status, _ = asyncio.run(run())
    assert status == 200


def test_timeout_is_retried_on_next_url():
    hits: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        url = f"{request.url.scheme}://{request.url.host}:{request.url.port}"
        hits.append(url)
        if url == "http://u1:9000":
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(200, json={"completion": "ok"})

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert asyncio.run(run()) == (200, {"completion": "ok"})
    assert hits == ["http://u1:9000", "http://u2:9000"]


def test_all_in_cooldown_tries_least_recently_failed_first():
    hits: list[str] = []
    failing = set(URLS)
    pool = UpstreamPool(urls=URLS, transport=_scripted_transport(hits, failing), cooldown_s=60)

    async def run():
        for _ in range(2):  # fails u1,u2 then u3,u1 -> all three cooling, u2 oldest
            try:
                await pool.forward({"prompt": "p"})
            except httpx.HTTPError:
                pass
        failing.discard("http://u3:9000")  # only u3 recovers
        hits.clear()
        results = []
        for _ in range(3):
            try:
                results.append(await pool.forward({"prompt": "p"}))
            except httpx.HTTPError:
                results.append(None)
        await pool.aclose()
        return results

    results = asyncio.run(run())
    # the recovered replica is reached within the next two requests instead of
    # being starved until its cooldown expires
    assert results[1] is not None and results[2] is not None
    assert results[1][1]["from"] == "http://u3:9000"


def test_attempt_deadline_caps_slow_upstream_and_fails_over():
    hits: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        url = f"{request.url.scheme}://{request.url.host}:{request.url.port}"
        hits.append(url)
        if url == "http://u1:9000":
            await asyncio.sleep(5)  # MockTransport ignores httpx.Timeout
        return httpx.Response(200, json={"completion": "ok"})

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler), timeout_s=0.1)

    async def run():
        start = asyncio.get_running_loop().time()
        result = await pool.forward({"prompt": "p"})
        elapsed = asyncio.get_running_loop().time() - start
        await pool.aclose()
        return result, elapsed

    result, elapsed = asyncio.run(run())
    assert result == (200, {"completion": "ok"})
    assert elapsed < 1
    assert hits == ["http://u1:9000", "http://u2:9000"]
    assert "http://u1:9000" in pool._failed_at


def _latency_transport(hits: list[str], delays: dict[str, float]) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        url = f"{request.url.scheme}://{request.url.host}:{request.url.port}"
        hits.append(url)
        await asyncio.sleep(delays.get(url, 0.0))
        return httpx.Response(200, json={"completion": "ok", "from": url})

    return httpx.MockTransport(handler)


def test_slow_primary_is_hedged_and_fast_replica_wins():
    hits: list[str] = []
    pool = UpstreamPool(urls=URLS, transport=_latency_transport(hits, {"http://u1:9000": 2.0}))

    async def run():
        loop = asyncio.get_running_loop()
        start = loop.time()
        result = await pool.forward({"prompt": "p"})
        elapsed = loop.time() - start
        await pool.aclose()
        return result, elapsed

    (status, body), elapsed = asyncio.run(run())
    assert status == 200 and body["from"] == "http://u2:9000"
    assert hits == ["http://u1:9000", "http://u2:9000"]
    assert elapsed < 0.5  # hedge fired after ~150ms instead of waiting 2s


def test_hedging_disabled_waits_for_primary():
    hits: list[str] = []
    pool = UpstreamPool(urls=URLS, transport=_latency_transport(hits, {"http://u1:9000": 0.3}),
                        hedging=False)

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    status, body = asyncio.run(run())
    assert body["from"] == "http://u1:9000"
    assert hits == ["http://u1:9000"]


def test_consistently_slow_replica_leaves_the_rotation():
    hits: list[str] = []
    delays = {"http://u1:9000": 0.01, "http://u2:9000": 0.01, "http://u3:9000": 0.4}
    pool = UpstreamPool(urls=URLS, transport=_latency_transport(hits, delays))

    async def run():
        results = [await pool.forward({"prompt": "p"}) for _ in range(12)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    winners = [body["from"] for _, body in results]
    assert "http://u3:9000" not in winners
    # after its first (hedged) attempt, u3 is no longer chosen first
    firsts_on_u3 = hits.count("http://u3:9000")
    assert firsts_on_u3 == 1


def test_hedging_disabled_keeps_plain_round_robin():
    hits: list[str] = []
    pool = UpstreamPool(urls=URLS, transport=_latency_transport(hits, {}), hedging=False)
    pool._latencies["http://u1:9000"].extend([0.01] * 5)
    pool._latencies["http://u2:9000"].extend([0.01] * 5)
    pool._latencies["http://u3:9000"].extend([0.5] * 5)

    async def run():
        for _ in range(3):
            await pool.forward({"prompt": "p"})
        await pool.aclose()

    asyncio.run(run())
    assert hits == URLS


def test_hedge_error_status_does_not_beat_pending_success():
    async def handler(request: httpx.Request) -> httpx.Response:
        url = f"{request.url.scheme}://{request.url.host}:{request.url.port}"
        if url == "http://u1:9000":
            await asyncio.sleep(0.4)
            return httpx.Response(200, json={"completion": "ok", "from": url})
        return httpx.Response(503, json={"error": "overloaded", "from": url})

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    status, body = asyncio.run(run())
    assert status == 200 and body["from"] == "http://u1:9000"
