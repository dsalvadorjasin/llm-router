import asyncio
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from app import main
from app.upstream import UpstreamPool, _Health, upstream_log_fields

URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]


def _host(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


def _transport(behaviour: dict, hits: list[str]) -> httpx.MockTransport:
    """behaviour maps upstream -> async callable(request) -> httpx.Response."""

    async def handler(request: httpx.Request) -> httpx.Response:
        host = _host(request)
        hits.append(host)
        return await behaviour.get(host, _ok)(request)

    return httpx.MockTransport(handler)


async def _ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"completion": "ok", "from": _host(request)})


def _sleep_then_ok(seconds: float):
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(seconds)
        return httpx.Response(200, json={"completion": "late", "from": _host(request)})

    return handler


def _status(code: int, body: dict | None = None):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(code, json=body or {"detail": f"err {code}"})

    return handler


async def _connect_error(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("refused", request=request)


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("LLM_ATTEMPT_TIMEOUT_MS", "50")
    monkeypatch.setenv("LLM_REQUEST_BUDGET_MS", "1000")
    monkeypatch.setenv("LLM_MAX_ATTEMPTS", "3")
    monkeypatch.setenv("LLM_ADAPTIVE_TIMEOUT", "0")
    # Isolate strategy A: plain round-robin first attempts, no hedging.
    monkeypatch.setenv("LLM_ROUTING", "roundrobin")
    monkeypatch.setenv("LLM_HEDGING", "0")
    return monkeypatch


def _run(pool: UpstreamPool, payloads: int = 1):
    async def go():
        try:
            return [await pool.forward({"prompt": "p"}) for _ in range(payloads)]
        finally:
            await pool.aclose()

    return asyncio.run(go())


def test_timeout_retries_on_a_different_upstream_and_drops_the_loser(env):
    hits: list[str] = []
    pool = UpstreamPool(URLS, _transport({URLS[0]: _sleep_then_ok(1.0)}, hits))
    start = time.monotonic()
    [(status, body)] = _run(pool)
    assert time.monotonic() - start < 0.5
    assert status == 200
    assert body["from"] == URLS[1]
    assert hits == [URLS[0], URLS[1]]


def test_5xx_and_transport_errors_fail_over(env):
    hits: list[str] = []
    behaviour = {URLS[0]: _status(503), URLS[1]: _connect_error}
    pool = UpstreamPool(URLS, _transport(behaviour, hits))
    [(status, body)] = _run(pool)
    assert (status, body["from"]) == (200, URLS[2])
    assert hits == URLS


def test_all_attempts_fail_returns_last_upstream_error(env):
    hits: list[str] = []
    behaviour = {u: _status(500 + i, {"detail": f"boom {i}"}) for i, u in enumerate(URLS)}
    pool = UpstreamPool(URLS, _transport(behaviour, hits))
    [(status, body)] = _run(pool)
    assert len(hits) == 3 and len(set(hits)) == 3
    assert status == 502
    assert body == {"detail": "boom 2"}


def test_client_4xx_is_passed_through_without_retry(env):
    hits: list[str] = []
    pool = UpstreamPool(URLS, _transport({URLS[0]: _status(422, {"detail": "bad"})}, hits))
    [(status, body)] = _run(pool)
    assert (status, body) == (422, {"detail": "bad"})
    assert hits == [URLS[0]]


def test_budget_caps_total_time_and_returns_504(env):
    env.setenv("LLM_ATTEMPT_TIMEOUT_MS", "500")
    env.setenv("LLM_REQUEST_BUDGET_MS", "120")
    hits: list[str] = []
    behaviour = {u: _sleep_then_ok(1.0) for u in URLS}
    pool = UpstreamPool(URLS, _transport(behaviour, hits))
    start = time.monotonic()
    [(status, body)] = _run(pool)
    elapsed = time.monotonic() - start
    assert status == 504
    assert "detail" in body
    assert elapsed < 0.3
    assert hits == [URLS[0]]


def test_last_attempt_gets_the_remaining_budget(env):
    env.setenv("LLM_MAX_ATTEMPTS", "2")
    hits: list[str] = []
    behaviour = {URLS[0]: _sleep_then_ok(1.0), URLS[1]: _sleep_then_ok(0.15)}
    pool = UpstreamPool(URLS, _transport(behaviour, hits))
    [(status, body)] = _run(pool)
    assert (status, body["from"]) == (200, URLS[1])


def test_kill_switch_single_attempt_behaves_like_plain_round_robin(env):
    env.setenv("LLM_MAX_ATTEMPTS", "1")
    env.setenv("LLM_REQUEST_BUDGET_MS", "60000")
    env.setenv("LLM_ATTEMPT_TIMEOUT_MS", "60000")
    hits: list[str] = []
    behaviour = {URLS[0]: _sleep_then_ok(0.2), URLS[1]: _status(503)}
    pool = UpstreamPool(URLS, _transport(behaviour, hits))
    results = _run(pool, payloads=4)
    assert hits == [URLS[0], URLS[1], URLS[2], URLS[0]]
    assert [s for s, _ in results] == [200, 503, 200, 200]


def test_retries_skip_a_failing_upstream_but_first_attempts_still_rotate(env):
    env.setenv("LLM_SUSPECT_TIMEOUT_FACTOR", "1")
    hits: list[str] = []
    behaviour = {URLS[1]: _sleep_then_ok(1.0)}  # u2 always times out
    pool = UpstreamPool(URLS, _transport(behaviour, hits))

    async def go():
        for _ in range(6):
            await pool.forward({"prompt": "p"})
        first_attempts = hits[:]
        hits.clear()
        behaviour[URLS[0]] = _status(500)
        result = await pool.forward({"prompt": "p"})  # first attempt -> u1
        await pool.aclose()
        return first_attempts, result

    first_attempts, (status, body) = asyncio.run(go())
    assert first_attempts[0] == URLS[0] and URLS[1] in first_attempts
    # u1 failed; next in rotation is u2, but it keeps timing out, so u3 is chosen.
    assert hits == [URLS[0], URLS[2]]
    assert (status, body["from"]) == (200, URLS[2])


def test_suspect_upstream_gets_short_timeout_with_periodic_full_probes(env):
    env.setenv("LLM_ATTEMPT_TIMEOUT_MS", "200")
    env.setenv("LLM_SUSPECT_TIMEOUT_FACTOR", "0.25")
    env.setenv("LLM_SUSPECT_PROBE_EVERY", "3")
    hits: list[str] = []
    slow = {"delay": 1.0}

    async def u1(request):
        await asyncio.sleep(slow["delay"])
        return httpx.Response(200, json={"from": "u1"})

    pool = UpstreamPool(URLS[:2], _transport({URLS[0]: u1}, hits))

    async def timed():
        start = time.monotonic()
        result = await pool.forward({"prompt": "p"})
        return time.monotonic() - start, result

    async def go():
        await timed()  # u1 full timeout -> failure ratio 1.0 (u1 is now suspect)
        await timed()  # u2 serves
        short = [await timed() for _ in range(4)][0::2]  # first attempts on u1
        slow["delay"] = 0.1  # u1 recovers
        recovered = [await timed() for _ in range(6)][0::2]
        await pool.aclose()
        return short, recovered

    short, recovered = asyncio.run(go())
    assert all(elapsed < 0.15 and body["from"] != "u1" for elapsed, (_, body) in short[:2])
    # within a few attempts a full-timeout probe reaches the recovered u1 again
    assert any(body.get("from") == "u1" for _, (_, body) in recovered)


def test_failure_ratio_decays_so_no_upstream_is_penalised_forever():
    health = _Health(halflife_s=1.0)
    for i in range(10):
        health.record(False, now=float(i) * 0.01)
    assert health.failure_ratio(now=0.1) == pytest.approx(1.0)
    health.record(True, now=10.0)
    assert health.failure_ratio(now=10.0) < 0.02


def test_adaptive_timeout_tracks_recent_latency_within_bounds(monkeypatch):
    monkeypatch.setenv("LLM_ATTEMPT_TIMEOUT_MS", "200")
    monkeypatch.setenv("LLM_MIN_ATTEMPT_TIMEOUT_MS", "120")
    monkeypatch.setenv("LLM_ADAPTIVE_TIMEOUT_MULTIPLIER", "1.1")
    pool = UpstreamPool(URLS, httpx.MockTransport(lambda r: httpx.Response(200, json={})))
    assert pool.attempt_timeout() == pytest.approx(0.2)
    pool._latencies.extend([0.14] * 50)
    assert pool.attempt_timeout() == pytest.approx(0.154)
    pool._latencies.extend([0.01] * 200)
    assert pool.attempt_timeout() == pytest.approx(0.12)
    pool._latencies.extend([1.0] * 200)
    assert pool.attempt_timeout() == pytest.approx(0.2)
    asyncio.run(pool.aclose())


def test_forward_fills_log_fields(env):
    hits: list[str] = []
    pool = UpstreamPool(URLS, _transport({URLS[0]: _status(500)}, hits))
    fields: dict = {}

    async def go():
        token = upstream_log_fields.set(fields)
        try:
            return await pool.forward({"prompt": "p"})
        finally:
            upstream_log_fields.reset(token)
            await pool.aclose()

    asyncio.run(go())
    assert fields == {"upstream_url": URLS[1], "hedged": False}


def test_request_log_line_includes_upstream_url_and_hedged(env, caplog):
    hits: list[str] = []
    with TestClient(main.app) as client:
        main.app.state.pool = UpstreamPool(URLS, _transport({}, hits))
        with caplog.at_level("INFO", logger="llm-router.requests"):
            client.post("/v1/generate", json={"prompt": "hi"})
            client.get("/v1/conversations")
    gen, conv = [m for m in caplog.messages if m.startswith("request ")][-2:]
    assert f"upstream_url={URLS[0]}" in gen and "hedged=false" in gen
    assert "upstream_url=- hedged=-" in conv
