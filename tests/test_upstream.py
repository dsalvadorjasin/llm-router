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


def _scripted_transport(hits: list[str], delays: dict[str, float],
                        statuses: dict[str, int] | None = None) -> httpx.MockTransport:
    statuses = statuses or {}

    async def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        hits.append(host)
        await asyncio.sleep(delays.get(host, 0))
        status = statuses.get(host, 200)
        return httpx.Response(status, json={"completion": host} if status == 200
                              else {"detail": f"{host} down"})

    return httpx.MockTransport(handler)


def _pool(transport, n=3):
    return UpstreamPool(urls=[f"http://u{i}:9000" for i in range(1, n + 1)], transport=transport)


def test_hedge_answers_before_slow_primary(monkeypatch):
    monkeypatch.setenv("LLM_HEDGE_DELAY_MS", "50")
    hits: list[str] = []
    pool = _pool(_scripted_transport(hits, {"u1": 2.0}))

    async def run():
        start = asyncio.get_running_loop().time()
        result = await pool.forward({"prompt": "p"})
        elapsed = asyncio.get_running_loop().time() - start
        await pool.aclose()
        return result, elapsed

    (status, body), elapsed = asyncio.run(run())
    assert status == 200 and body == {"completion": "u2"}
    assert hits == ["u1", "u2"]
    assert elapsed < 0.5


def test_server_error_fails_over_immediately():
    hits: list[str] = []
    pool = _pool(_scripted_transport(hits, {}, {"u1": 503}))

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert asyncio.run(run()) == (200, {"completion": "u2"})
    assert hits == ["u1", "u2"]


def test_all_failing_passes_last_error_through(monkeypatch):
    monkeypatch.setenv("LLM_HEDGE_MAX_ATTEMPTS", "2")
    hits: list[str] = []
    pool = _pool(_scripted_transport(hits, {}, {"u1": 503, "u2": 503}), n=2)

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert asyncio.run(run()) == (503, {"detail": "u2 down"})


def test_client_errors_are_not_retried():
    hits: list[str] = []
    pool = _pool(_scripted_transport(hits, {}, {"u1": 400}))

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert asyncio.run(run())[0] == 400
    assert hits == ["u1"]


def test_slow_replica_drops_out_of_rotation(monkeypatch):
    monkeypatch.setenv("LLM_HEDGE_DELAY_MS", "30")
    hits: list[str] = []
    pool = _pool(_scripted_transport(hits, {"u1": 0.01, "u2": 0.01, "u3": 0.5}))

    async def run():
        for _ in range(9):
            await pool.forward({"prompt": "p"})
        hits.clear()
        for _ in range(10):
            assert (await pool.forward({"prompt": "p"}))[0] == 200
        await pool.aclose()

    asyncio.run(run())
    assert "u3" not in hits
    assert {"u1", "u2"} <= set(hits)


def test_kill_switch_restores_plain_round_robin(monkeypatch):
    monkeypatch.setenv("LLM_HEDGING", "0")
    hits: list[str] = []
    pool = _pool(_scripted_transport(hits, {}, {"u1": 503}))

    async def run():
        results = [await pool.forward({"prompt": "p"}) for _ in range(4)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert hits == ["u1", "u2", "u3", "u1"]
    assert results[0] == (503, {"detail": "u1 down"})


def test_short_cancelled_hedge_does_not_make_slow_replica_look_fast(monkeypatch):
    monkeypatch.setenv("LLM_HEDGE_DELAY_MS", "80")
    hits: list[str] = []
    # u1 answers just after the hedge to slow u2 starts, so u2 is cancelled
    # within ~10ms; that must not rank u2 as the fastest replica.
    pool = _pool(_scripted_transport(hits, {"u1": 0.09, "u2": 1.0}), n=2)

    async def run():
        loop = asyncio.get_running_loop()
        timings = []
        for _ in range(6):
            start = loop.time()
            result = await pool.forward({"prompt": "p"})
            timings.append((result, loop.time() - start))
        await pool.aclose()
        return timings

    timings = asyncio.run(run())
    assert all(result == (200, {"completion": "u1"}) for result, _ in timings)
    assert all(elapsed < 0.4 for _, elapsed in timings)


def test_stalled_fast_replica_falls_back_to_slower_healthy_one(monkeypatch):
    monkeypatch.setenv("LLM_HEDGE_DELAY_MS", "50")
    monkeypatch.setenv("LLM_REQUEST_TIMEOUT_S", "2")
    delays = {"u1": 0.01, "u2": 0.3}
    hits: list[str] = []
    pool = _pool(_scripted_transport(hits, delays), n=2)

    async def run():
        for _ in range(6):
            await pool.forward({"prompt": "p"})
        delays["u1"] = 10.0  # u1 stalls; u2 is out of the fast group but healthy
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert asyncio.run(run()) == (200, {"completion": "u2"})


def test_kill_switch_still_applies_request_timeout(monkeypatch):
    monkeypatch.setenv("LLM_HEDGING", "0")
    monkeypatch.setenv("LLM_REQUEST_TIMEOUT_S", "0.1")
    pool = _pool(_scripted_transport([], {"u1": 5.0}))

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert asyncio.run(run()) == (504, {"detail": "upstream timeout"})


def test_probes_rotate_across_stale_replicas(monkeypatch):
    from app import upstream

    monkeypatch.setattr(upstream, "_PROBE_EVERY", 1)
    monkeypatch.setattr(upstream, "_STALE_AFTER_S", 0.0)
    pool = _pool(_scripted_transport([], {}, {"u1": 503}), n=2)
    probed: list[str] = []

    async def fake_attempt(replica, payload, informative_after=0.0):
        probed.append(replica.url)
        return 503, {}

    async def run():
        monkeypatch.setattr(pool, "_attempt", fake_attempt)
        for _ in range(4):
            pool._maybe_probe({"prompt": "p"})
            await asyncio.sleep(0)
        await pool.aclose()

    asyncio.run(run())
    assert set(probed) == {"http://u1:9000", "http://u2:9000"}
