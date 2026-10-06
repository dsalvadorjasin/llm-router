import asyncio
import time

import httpx
import pytest

from app.config import HedgeSettings, hedge_settings
from app.upstream import UpstreamPool

URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]


def _host(request: httpx.Request) -> str:
    return request.url.host


def _ok(sig: str = "a" * 64, completion: str = "hello") -> httpx.Response:
    return httpx.Response(200, json={"completion": completion, "signature": sig})


class FakeFleet:
    """Per-host behaviour: (delay_seconds, response factory or exception)."""

    def __init__(self, behaviour):
        self.behaviour = behaviour
        self.hits: list[str] = []
        self.cancelled: list[str] = []

    def transport(self) -> httpx.MockTransport:
        async def handler(request: httpx.Request) -> httpx.Response:
            host = _host(request)
            self.hits.append(host)
            delay, result = self.behaviour[host]
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                self.cancelled.append(host)
                raise
            if isinstance(result, Exception):
                raise result
            return result(request) if callable(result) else result

        return httpx.MockTransport(handler)


def _settings(**kw) -> HedgeSettings:
    base = dict(enabled=True, delay_ms=50, adaptive=False, max_attempts=3,
                attempt_timeout_s=5, min_delay_ms=1)
    base.update(kw)
    return HedgeSettings(**base)


def _run(coro):
    return asyncio.run(coro)


def test_hedge_fires_after_delay_and_fast_response_wins():
    fleet = FakeFleet({"u1": (2.0, _ok()), "u2": (0.01, _ok(completion="fast")),
                       "u3": (0.01, _ok(completion="fast"))})
    pool = UpstreamPool(URLS, fleet.transport(), _settings(delay_ms=50, max_attempts=2))

    async def go():
        t0 = time.monotonic()
        res = await pool.forward({"prompt": "p", "max_tokens": 8})
        elapsed = time.monotonic() - t0
        await pool.aclose()
        return res, elapsed

    (status, body), elapsed = _run(go())
    assert status == 200 and body["completion"] == "fast"
    assert 0.04 <= elapsed < 0.5
    assert fleet.hits[0] == "u1" and len(fleet.hits) == 2 and fleet.hits[1] != "u1"
    assert fleet.cancelled == ["u1"]
    assert pool.stats["hedges"] == 1 and pool.stats["cancelled"] == 1


def test_no_hedge_when_primary_is_fast():
    fleet = FakeFleet({h: (0.005, _ok()) for h in ("u1", "u2", "u3")})
    pool = UpstreamPool(URLS, fleet.transport(), _settings(delay_ms=300))

    async def go():
        res = [await pool.forward({"prompt": f"p{i}", "max_tokens": 8}) for i in range(5)]
        await pool.aclose()
        return res

    results = _run(go())
    assert all(status == 200 for status, _ in results)
    assert len(fleet.hits) == 5
    assert pool.stats["hedges"] == 0 and pool.stats["cancelled"] == 0


@pytest.mark.parametrize("failure", [
    httpx.Response(500, json={"detail": "boom"}),
    httpx.Response(200, json={"completion": "", "signature": "a" * 64}),
    httpx.Response(200, json={"completion": "x"}),
    httpx.Response(200, text="not json"),
    httpx.ConnectError("refused"),
])
def test_failed_attempt_falls_over_to_another_replica(failure):
    fleet = FakeFleet({"u1": (0.0, failure), "u2": (0.0, _ok(completion="good")),
                       "u3": (0.0, _ok(completion="good"))})
    pool = UpstreamPool(URLS, fleet.transport(), _settings(delay_ms=1000))

    async def go():
        t0 = time.monotonic()
        res = await pool.forward({"prompt": "p", "max_tokens": 8})
        elapsed = time.monotonic() - t0
        await pool.aclose()
        return res, elapsed

    (status, body), elapsed = _run(go())
    assert (status, body["completion"]) == (200, "good")
    assert fleet.hits[0] == "u1" and fleet.hits[1] != "u1"
    assert elapsed < 0.5  # failover doesn't wait for the hedge delay
    assert pool.stats["failovers"] == 1


def test_all_attempts_fail_returns_error_not_exception():
    fleet = FakeFleet({h: (0.0, httpx.Response(503, json={"detail": "down"}))
                       for h in ("u1", "u2", "u3")})
    pool = UpstreamPool(URLS, fleet.transport(), _settings())

    async def go():
        res = await pool.forward({"prompt": "p", "max_tokens": 8})
        await pool.aclose()
        return res

    assert _run(go()) == (503, {"detail": "down"})
    assert sorted(fleet.hits) == ["u1", "u2", "u3"]


def test_attempt_timeout_is_bounded():
    fleet = FakeFleet({h: (0.0, httpx.ReadTimeout("slow")) for h in ("u1", "u2", "u3")})
    pool = UpstreamPool(URLS, fleet.transport(), _settings(max_attempts=2))

    async def go():
        res = await pool.forward({"prompt": "p", "max_tokens": 8})
        await pool.aclose()
        return res

    status, body = _run(go())
    assert status == 504 and len(fleet.hits) == 2


def test_deterministic_signatures_are_not_pinned():
    fleet = FakeFleet({h: (0.0, _ok(sig="s" * 64)) for h in ("u1", "u2", "u3")})
    pool = UpstreamPool(URLS, fleet.transport(), _settings(delay_ms=1000))

    async def go():
        res = [await pool.forward({"prompt": "same", "max_tokens": 8}) for _ in range(3)]
        await pool.aclose()
        return res

    results = _run(go())
    assert {body["signature"] for _, body in results} == {"s" * 64}
    assert len(set(fleet.hits)) > 1  # repeated prompt still load-balanced
    assert pool.stats["signature_mismatches"] == 0


def test_divergent_signatures_stay_consistent_per_prompt():
    def per_host(req):
        return _ok(sig=req.url.host[-1] * 64)

    fleet = FakeFleet({h: (0.0, per_host) for h in ("u1", "u2", "u3")})
    pool = UpstreamPool(URLS, fleet.transport(), _settings(delay_ms=1000))

    async def go():
        res = [await pool.forward({"prompt": "same", "max_tokens": 8}) for _ in range(6)]
        other = await pool.forward({"prompt": "other", "max_tokens": 8})
        await pool.aclose()
        return res, other

    results, other = _run(go())
    first_sig = results[0][1]["signature"]
    assert all(body["signature"] == first_sig for _, body in results)
    assert pool.stats["signature_mismatches"] >= 1
    # once divergence is detected, the repeated prompt is pinned to its first replica
    first_host = fleet.hits[0]
    assert fleet.hits[-2] == first_host
    assert other[0] == 200


def test_divergent_signatures_hedge_only_accepts_matching_signature():
    def per_host(req):
        return _ok(sig=req.url.host[-1] * 64)

    fleet = FakeFleet({"u1": (0.0, per_host), "u2": (0.0, per_host), "u3": (0.0, per_host)})
    pool = UpstreamPool(URLS, fleet.transport(), _settings(delay_ms=30, max_attempts=3))

    async def go():
        first = await pool.forward({"prompt": "k", "max_tokens": 8})   # served by u1
        await pool.forward({"prompt": "k", "max_tokens": 8})           # u2 mismatches -> u1
        fleet.behaviour["u1"] = (0.2, per_host)                         # pinned replica slow
        third = await pool.forward({"prompt": "k", "max_tokens": 8})
        await pool.aclose()
        return first, third

    first, third = _run(go())
    assert third[1]["signature"] == first[1]["signature"] == "1" * 64


def test_unmeasured_replica_is_not_treated_as_fastest():
    fleet = FakeFleet({h: (0.0, _ok()) for h in ("u1", "u2", "u3")})
    pool = UpstreamPool(URLS[:2], fleet.transport(), _settings())
    now = time.monotonic()
    pool._observe(0, 0.2, now, success=True)
    # u2 has no sample: it must get a neutral prior, not a score of 0
    assert pool._estimate(1, now) == pytest.approx(0.2)
    # an unmeasured replica with a long-pending attempt looks slow
    pool._replicas[1].pending[99] = now - 3.0
    assert pool._estimate(1, now) >= 3.0
    assert pool._pick(set()) == 0
    _run(pool.aclose())


def test_slow_replica_estimate_relaxes_so_it_is_not_excluded_forever():
    pool = UpstreamPool(URLS, httpx.MockTransport(lambda r: _ok()),
                        _settings(idle_decay_half_life_s=10))
    now = time.monotonic()
    pool._observe(0, 0.2, now, success=True)
    pool._observe(1, 0.2, now, success=True)
    pool._observe(2, 2.0, now, success=True)
    assert pool._estimate(2, now) == pytest.approx(2.0)
    assert pool._estimate(2, now + 60) < 0.25
    _run(pool.aclose())


def test_adaptive_delay_uses_percentile_with_floor_and_ceiling():
    pool = UpstreamPool(URLS, httpx.MockTransport(lambda r: _ok()),
                        _settings(adaptive=True, delay_ms=400, percentile=0.8,
                                  min_samples=10, min_delay_ms=50, max_delay_ms=1000))
    now = time.monotonic()
    assert pool.hedge_delay() == pytest.approx(0.4)  # cold start: configured delay
    for i in range(100):
        pool._observe(i % 3, 0.1 + i / 1000, now, success=True)
    assert pool.hedge_delay() == pytest.approx(0.18, abs=0.01)
    for _ in range(600):
        pool._observe(0, 0.001, now, success=True)
    assert pool.hedge_delay() == pytest.approx(0.05)
    for _ in range(600):
        pool._observe(0, 5.0, now, success=True)
    assert pool.hedge_delay() == pytest.approx(1.0)
    _run(pool.aclose())


def test_hedge_settings_from_env(monkeypatch):
    monkeypatch.setenv("HEDGE_ENABLED", "0")
    monkeypatch.setenv("HEDGE_DELAY_MS", "250")
    monkeypatch.setenv("HEDGE_MAX_ATTEMPTS", "2")
    s = hedge_settings()
    assert (s.enabled, s.delay_ms, s.max_attempts) == (False, 250.0, 2)
    monkeypatch.delenv("HEDGE_ENABLED")
    assert hedge_settings().enabled is True
