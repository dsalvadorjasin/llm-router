import asyncio
import re

import httpx
from fastapi.testclient import TestClient

from app import main
from app.routing import LatencyRouter
from app.upstream import UpstreamPool

URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]


class FakeClock:
    def __init__(self, now: float = 100.0):
        self.now = now

    def __call__(self) -> float:
        return self.now


def _router(clock, **kw) -> LatencyRouter:
    kw.setdefault("probe_ratio", 0.0)
    return LatencyRouter(URLS, mode="latency", alpha=0.5, penalty_ms=1000,
                         penalty_halflife_ms=1000, clock=clock, rng=lambda: 1.0, **kw)


def _sample(router, clock, url, seconds, ok=True):
    token = router.start(url)
    clock.now += seconds
    router.finish(url, token, ok=ok)


def _host(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


def test_lowest_ewma_wins_and_inflight_scales_score():
    clock = FakeClock()
    r = _router(clock)
    _sample(r, clock, URLS[0], 0.10)
    _sample(r, clock, URLS[1], 0.20)
    _sample(r, clock, URLS[2], 1.30)
    assert r.rank() == URLS
    # 0.10 * (1 + 2) = 0.30 > 0.20 * (1 + 0)
    r.start(URLS[0])
    r.start(URLS[0])
    assert r.rank() == [URLS[1], URLS[0], URLS[2]]


def test_ewma_uses_alpha():
    clock = FakeClock()
    r = _router(clock)
    _sample(r, clock, URLS[0], 0.10)
    _sample(r, clock, URLS[0], 0.30)
    assert abs(r.snapshot()[URLS[0]]["ewma_ms"] - 200.0) < 1e-6


def test_unmeasured_preferred_then_scored_by_oldest_pending():
    clock = FakeClock()
    r = _router(clock)
    _sample(r, clock, URLS[0], 0.15)
    # u2/u3 unmeasured and idle: tried before the measured u1, in round-robin order
    assert r.rank()[:2] == [URLS[1], URLS[2]]
    r.start(URLS[2])
    assert r.rank()[0] == URLS[1]
    r.start(URLS[1])
    clock.now += 0.5
    # u2/u3 now have a 0.5s-old pending attempt: score 0.5 * 2 = 1.0 > u1's 0.15
    assert r.rank()[0] == URLS[0]


def test_ties_break_round_robin():
    clock = FakeClock()
    r = _router(clock)
    for u in URLS:
        _sample(r, clock, u, 0.1)
    firsts = [r.plan()[0] for _ in range(6)]
    assert firsts == URLS + URLS


def test_failure_penalty_decays_and_is_never_permanent():
    clock = FakeClock()
    r = _router(clock)
    for u in URLS:
        _sample(r, clock, u, 0.1)
    _sample(r, clock, URLS[0], 0.0, ok=False)
    assert abs(r.penalty(URLS[0]) - 1.0) < 1e-9
    assert r.rank()[-1] == URLS[0]
    clock.now += 1.0  # one half-life
    assert abs(r.penalty(URLS[0]) - 0.5) < 1e-9
    _sample(r, clock, URLS[0], 0.0, ok=False)  # stacks onto the decayed value
    assert abs(r.penalty(URLS[0]) - 1.5) < 1e-9
    clock.now += 20.0
    assert r.penalty(URLS[0]) < 1e-5
    assert abs(r.score(URLS[0])[1] - r.score(URLS[1])[1]) < 1e-4
    r.start(URLS[1])  # load on the peers makes the recovered upstream win again
    r.start(URLS[2])
    assert r.rank()[0] == URLS[0]


def test_probe_sends_traffic_to_least_recently_successful():
    clock = FakeClock()
    rolls = iter([0.5, 0.01])
    r = LatencyRouter(URLS, mode="latency", alpha=0.5, penalty_ms=1000,
                      penalty_halflife_ms=1000, probe_ratio=0.02,
                      clock=clock, rng=lambda: next(rolls))
    _sample(r, clock, URLS[2], 3.0)  # oldest success, slowest
    _sample(r, clock, URLS[0], 0.1)
    _sample(r, clock, URLS[1], 0.1)
    assert r.plan()[0] != URLS[2]       # roll 0.5 >= 0.02: no probe
    plan = r.plan()                     # roll 0.01 < 0.02: probe
    assert plan[0] == URLS[2]
    assert sorted(plan) == sorted(URLS)


def test_roundrobin_mode_ignores_scores():
    clock = FakeClock()
    r = LatencyRouter(URLS, mode="roundrobin", clock=clock)
    _sample(r, clock, URLS[0], 5.0)
    assert [r.plan()[0] for _ in range(4)] == URLS + URLS[:1]


def test_forward_prefers_fast_upstream():
    async def handler(request):
        if request.url.host == "u3":
            await asyncio.sleep(0.05)
        return httpx.Response(200, json={"completion": "ok"})

    hits: list[str] = []

    async def recording(request):
        hits.append(request.url.host)
        return await handler(request)

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(recording), rng=lambda: 1.0)

    async def run():
        for _ in range(30):
            await pool.forward({"prompt": "p"})
        await pool.aclose()

    asyncio.run(run())
    assert set(hits[:3]) == {"u1", "u2", "u3"}
    assert "u3" not in hits[3:]


def test_failover_on_5xx_and_transport_error_and_4xx_passthrough():
    calls: list[str] = []

    async def handler(request):
        calls.append(request.url.host)
        if request.url.host == "u1":
            return httpx.Response(503, json={"detail": "busy"})
        if request.url.host == "u2":
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(422, json={"detail": "bad prompt"})

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler),
                        rng=lambda: 1.0)

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert asyncio.run(run()) == (422, {"detail": "bad prompt"})
    assert calls == ["u1", "u2", "u3"]
    snap = pool.router.snapshot()
    assert snap["http://u1:9000"]["penalty_ms"] > 0
    assert snap["http://u2:9000"]["penalty_ms"] > 0
    assert snap["http://u3:9000"]["penalty_ms"] == 0


def test_all_attempts_fail_returns_last_error():
    async def handler(request):
        return httpx.Response(500, json={"detail": "boom"})

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler), rng=lambda: 1.0)

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert asyncio.run(run()) == (500, {"detail": "boom"})


def test_per_attempt_timeout_fails_over():
    async def handler(request):
        if request.url.host == "u1":
            await asyncio.sleep(1.0)
            return httpx.Response(200, json={"completion": "late"})
        return httpx.Response(200, json={"completion": "ok"})

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler),
                        attempt_timeout_ms=50, rng=lambda: 1.0)

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert asyncio.run(run()) == (200, {"completion": "ok"})
    assert pool.router.snapshot()["http://u1:9000"]["penalty_ms"] > 0
    assert pool.router.snapshot()["http://u1:9000"]["inflight"] == 0


def test_request_log_includes_upstream_url_and_hedged(caplog):
    async def handler(request):
        return httpx.Response(200, json={"completion": "ok"})

    with TestClient(main.app) as client:
        main.app.state.pool = UpstreamPool(urls=["http://u9:9000"],
                                           transport=httpx.MockTransport(handler))
        with caplog.at_level("INFO", logger="llm-router.requests"):
            client.post("/v1/generate", json={"prompt": "hi"})
            client.get("/v1/conversations")
    gen = next(m for m in caplog.messages if "path=/v1/generate" in m)
    other = next(m for m in caplog.messages if "path=/v1/conversations" in m)
    assert "upstream_url=http://u9:9000" in gen
    assert "hedged=false" in gen
    assert re.search(r"upstream_url=- hedged=-$", other)
