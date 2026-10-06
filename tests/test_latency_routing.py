import asyncio

import httpx

from app.routing import LatencyAwareRouter, RoutingConfig
from app.upstream import UpstreamPool

URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]


def _host(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def _run(coro):
    return asyncio.run(coro)


def test_ties_are_broken_in_round_robin_order():
    router = LatencyAwareRouter(URLS, RoutingConfig())
    assert [router.pick() for _ in range(4)] == [0, 1, 2, 0]


def test_ewma_prefers_faster_replica():
    clock = FakeClock()
    router = LatencyAwareRouter(URLS, RoutingConfig(), clock=clock)
    for idx, ms in ((0, 100.0), (1, 100.0), (2, 1300.0)):
        router.record_success(idx, ms)
    picks = [router.pick() for _ in range(6)]
    assert 2 not in picks
    assert picks == [0, 1, 0, 1, 0, 1]


def test_slow_replica_is_never_permanently_excluded():
    clock = FakeClock()
    router = LatencyAwareRouter(URLS, RoutingConfig(decay_half_life_s=1.0), clock=clock)
    for idx, ms in ((0, 100.0), (1, 100.0), (2, 5000.0)):
        router.record_success(idx, ms)
    assert 2 not in {router.pick() for _ in range(6)}
    clock.t = 30.0
    assert 2 in {router.pick() for _ in range(3)}


def test_retries_exclude_the_replica_that_just_failed():
    router = LatencyAwareRouter(URLS, RoutingConfig())
    assert router.pick(exclude={0}) == 1
    assert router.pick(exclude={1}) in (0, 2)


def test_least_outstanding_policy():
    router = LatencyAwareRouter(URLS, RoutingConfig(policy="least_outstanding"))
    router._replicas[0].outstanding = 2
    router._replicas[1].outstanding = 1
    assert router.pick() == 2


def test_adaptive_attempt_timeout_tracks_successful_latency():
    cfg = RoutingConfig(min_samples=5, timeout_quantile=0.99, timeout_multiplier=1.1,
                        attempt_timeout_min_ms=10, attempt_timeout_max_ms=10_000)
    router = LatencyAwareRouter(URLS, cfg)
    assert router.attempt_timeout_ms() == cfg.attempt_timeout_ms
    for _ in range(10):
        router.record_success(0, 100.0)
    assert abs(router.attempt_timeout_ms() - 110.0) < 1e-6


def test_non_200_and_transport_errors_are_retried_on_another_replica(monkeypatch):
    monkeypatch.delenv("ROUTER_LATENCY_AWARE", raising=False)
    hits: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        host = _host(request)
        hits.append(host)
        if host == URLS[0]:
            return httpx.Response(500, json={"detail": "boom"})
        if host == URLS[1]:
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(200, json={"completion": "ok"})

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert _run(run()) == (200, {"completion": "ok"})
    assert hits == URLS
    scores = pool._router._scores(pool._router.clock())
    assert scores[2] < scores[0] and scores[2] < scores[1]


def test_slow_attempt_times_out_and_is_retried(monkeypatch):
    monkeypatch.setenv("ROUTER_ATTEMPT_TIMEOUT_MS", "50")
    hits: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        host = _host(request)
        hits.append(host)
        if host == URLS[0]:
            await asyncio.sleep(5)
        return httpx.Response(200, json={"completion": "ok"})

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))

    async def run():
        loop = asyncio.get_running_loop()
        start = loop.time()
        result = await pool.forward({"prompt": "p"})
        elapsed = loop.time() - start
        await pool.aclose()
        return result, elapsed

    result, elapsed = _run(run())
    assert result == (200, {"completion": "ok"})
    assert hits == [URLS[0], URLS[1]]
    assert elapsed < 1.0


def test_all_attempts_failing_returns_last_upstream_response(monkeypatch):
    monkeypatch.setenv("ROUTER_MAX_ATTEMPTS", "3")

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"detail": "unavailable"})

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert _run(run()) == (503, {"detail": "unavailable"})


def test_kill_switch_restores_plain_round_robin(monkeypatch):
    monkeypatch.setenv("ROUTER_LATENCY_AWARE", "0")
    hits: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        hits.append(_host(request))
        return httpx.Response(500, json={"detail": "boom"})

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler))
    assert pool._router is None

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert _run(run()) == (500, {"detail": "boom"})
    assert hits == [URLS[0]]
