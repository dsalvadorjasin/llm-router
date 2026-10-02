import asyncio
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from app import main
from app.config import upstream_hedge_budget, upstream_hedge_delay
from app.upstream import HedgeBudget, UpstreamPool

URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]
HEDGE_S = 0.05


def _host(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


class DelayTransport(httpx.AsyncBaseTransport):
    """Answers each upstream after a scripted per-call delay (seconds)."""

    def __init__(self, delays: dict[str, list[float]], status: dict[str, int] | None = None):
        self.delays = delays
        self.status = status or {}
        self.hits: list[str] = []
        self.cancelled: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = _host(request)
        self.hits.append(host)
        script = self.delays.get(host, [0.0])
        delay = script.pop(0) if len(script) > 1 else script[0]
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            self.cancelled.append(host)
            raise
        code = self.status.get(host, 200)
        return httpx.Response(code, json={"completion": host, "signature": "ab" * 32})


def _pool(t: DelayTransport, **kw) -> UpstreamPool:
    kw.setdefault("hedge_delay_s", HEDGE_S)
    return UpstreamPool(urls=URLS, transport=t, **kw)


def _run(coro):
    return asyncio.run(coro)


def test_slow_attempt_is_hedged_and_fast_hedge_wins():
    t = DelayTransport({URLS[0]: [5.0], URLS[1]: [0.0]})

    async def go():
        pool = _pool(t)
        start = time.monotonic()
        result = await pool.forward({"prompt": "p"})
        elapsed = time.monotonic() - start
        stats, inflight = pool.stats(), pool.inflight()
        await pool.aclose()
        return result, elapsed, stats, inflight

    result, elapsed, stats, inflight = _run(go())
    assert result[0] == 200 and result[1]["completion"] == URLS[1]
    assert HEDGE_S <= elapsed < 1.0
    assert t.hits == URLS[:2]
    assert t.cancelled == [URLS[0]]
    assert inflight == {u: 0 for u in URLS}
    assert (stats["attempts"], stats["hedges"], stats["hedge_wins"],
            stats["cancelled_attempts"]) == (2, 1, 1, 1)


def test_fast_attempt_is_not_hedged():
    t = DelayTransport({})

    async def go():
        pool = _pool(t)
        results = [await pool.forward({"prompt": "p"}) for _ in range(3)]
        stats = pool.stats()
        await pool.aclose()
        return results, stats

    results, stats = _run(go())
    assert all(status == 200 for status, _ in results)
    assert sorted(t.hits) == sorted(URLS)
    assert stats["hedges"] == 0 and stats["attempts"] == 3


def test_primary_still_wins_if_it_answers_before_hedge():
    t = DelayTransport({URLS[0]: [0.3], URLS[1]: [5.0], URLS[2]: [5.0]})

    async def go():
        pool = _pool(t, hedge_delay_s=0.1)
        result = await pool.forward({"prompt": "p"})
        stats = pool.stats()
        await pool.aclose()
        return result, stats

    (status, body), stats = _run(go())
    assert status == 200 and body["completion"] == URLS[0]
    assert stats["hedges"] == 2 and stats["hedge_wins"] == 0
    assert stats["cancelled_attempts"] == 2


def test_hedges_stop_at_max_attempts():
    t = DelayTransport({u: [0.4] for u in URLS})

    async def go():
        pool = _pool(t, max_attempts=2)
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    status, _ = _run(go())
    assert status == 200
    assert len(t.hits) == 2 and len(set(t.hits)) == 2


def test_hedge_reuses_fast_replica_instead_of_known_slow_one():
    # u1 and u2 hit independent slow tails on this request; u3 is always slower.
    t = DelayTransport({URLS[0]: [0.0, 5.0, 0.0], URLS[1]: [0.0, 5.0, 0.0], URLS[2]: [0.15]})

    async def go():
        pool = _pool(t, hedge_delay_s=0.2, latency_floor_s=0.001)
        for _ in range(3):
            await pool.forward({"prompt": "warm"})
        t.hits.clear()
        result = await pool.forward({"prompt": "p"})
        stats, inflight = pool.stats(), pool.inflight()
        await pool.aclose()
        return result, stats, inflight

    (status, body), stats, inflight = _run(go())
    assert t.hits[:2] == URLS[:2] and len(t.hits) == 3
    assert t.hits[2] in URLS[:2]
    assert status == 200 and body["completion"] == t.hits[2]
    assert (stats["hedges"], stats["hedge_wins"]) == (2, 1)
    assert inflight == {u: 0 for u in URLS}


def test_exhausted_hedge_budget_waits_for_primary_and_counts_denial():
    t = DelayTransport({URLS[0]: [0.2]})

    async def go():
        pool = _pool(t, hedge_budget=HedgeBudget(ratio=0.0, burst=0.0))
        result = await pool.forward({"prompt": "p"})
        stats = pool.stats()
        await pool.aclose()
        return result, stats

    (status, body), stats = _run(go())
    assert status == 200 and body["completion"] == URLS[0]
    assert t.hits == [URLS[0]]
    assert (stats["hedges"], stats["hedges_denied"]) == (0, 1)


def test_hedge_budget_refills_per_request_and_caps_at_burst():
    budget = HedgeBudget(ratio=0.5, burst=1.0)
    assert budget.try_spend()
    assert not budget.try_spend()
    budget.deposit()
    assert not budget.try_spend()
    budget.deposit()
    assert budget.try_spend()
    for _ in range(10):
        budget.deposit()
    assert budget.try_spend()
    assert not budget.try_spend()


def test_failed_primary_fails_over_while_hedge_is_in_flight():
    t = DelayTransport({URLS[0]: [0.1], URLS[1]: [5.0], URLS[2]: [0.0]},
                       status={URLS[0]: 503})

    async def go():
        pool = _pool(t, hedge_delay_s=0.05)
        result = await pool.forward({"prompt": "p"})
        stats = pool.stats()
        await pool.aclose()
        return result, stats

    (status, body), stats = _run(go())
    assert status == 200 and body["completion"] == URLS[2]
    assert stats["failed_attempts"] == 1 and stats["failovers"] == 1
    assert t.cancelled == [URLS[1]]


def test_every_attempt_failing_with_hedging_is_reported_not_swallowed():
    t = DelayTransport({u: [0.08] for u in URLS}, status={u: 500 for u in URLS})

    async def go():
        pool = _pool(t)
        result = await pool.forward({"prompt": "p"})
        stats = pool.stats()
        await pool.aclose()
        return result, stats

    (status, body), stats = _run(go())
    assert status == 502
    assert body["detail"] == "upstream error"
    assert stats["exhausted"] == 1 and stats["failed_attempts"] == 3
    assert sorted(t.hits) == sorted(URLS)


def test_consistently_slow_replica_is_deprioritised():
    t = DelayTransport({URLS[0]: [0.0], URLS[1]: [0.0], URLS[2]: [0.15]})

    async def go():
        pool = UpstreamPool(urls=URLS, transport=t)
        for _ in range(3):
            await pool.forward({"prompt": "warm"})
        t.hits.clear()
        for _ in range(6):
            await pool.forward({"prompt": "p"})
        stats = pool.stats()
        await pool.aclose()
        return list(t.hits), stats

    hits, stats = _run(go())
    assert URLS[2] not in hits
    assert sorted(set(hits)) == URLS[:2]
    slow = next(u for u in stats["upstreams"] if u["url"] == URLS[2])
    assert slow["latency_ewma_ms"] >= 100


def test_cancelled_slow_attempt_raises_latency_estimate():
    t = DelayTransport({URLS[0]: [5.0], URLS[1]: [0.0]})

    async def go():
        pool = _pool(t, hedge_delay_s=0.1)
        await pool.forward({"prompt": "p"})
        stats = pool.stats()
        await pool.aclose()
        return stats

    ewma = {u["url"]: u["latency_ewma_ms"] for u in _run(go())["upstreams"]}
    assert ewma[URLS[0]] >= 100
    assert ewma[URLS[1]] < ewma[URLS[0]]


def test_outer_cancellation_cancels_all_attempts():
    t = DelayTransport({u: [5.0] for u in URLS})

    async def go():
        pool = _pool(t)
        task = asyncio.create_task(pool.forward({"prompt": "p"}))
        await asyncio.sleep(HEDGE_S * 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        inflight = pool.inflight()
        await pool.aclose()
        return inflight

    assert _run(go()) == {u: 0 for u in URLS}
    assert sorted(t.cancelled) == sorted(t.hits)


def test_hedge_config_defaults_and_overrides(monkeypatch):
    for var in ("LLM_UPSTREAM_HEDGE_DELAY_MS", "LLM_UPSTREAM_HEDGE_BUDGET_RATIO",
                "LLM_UPSTREAM_HEDGE_BUDGET_BURST"):
        monkeypatch.delenv(var, raising=False)
    assert upstream_hedge_delay() == pytest.approx(0.165)
    assert upstream_hedge_budget() == (0.2, 10.0)
    monkeypatch.setenv("LLM_UPSTREAM_HEDGE_DELAY_MS", "0")
    assert upstream_hedge_delay() is None
    monkeypatch.setenv("LLM_UPSTREAM_HEDGE_DELAY_MS", "350")
    assert upstream_hedge_delay() == pytest.approx(0.35)
    monkeypatch.setenv("LLM_UPSTREAM_HEDGE_BUDGET_RATIO", "0.5")
    monkeypatch.setenv("LLM_UPSTREAM_HEDGE_BUDGET_BURST", "3")
    assert upstream_hedge_budget() == (0.5, 3.0)


@pytest.mark.parametrize("raw", ["abc", "-1", "inf", "nan"])
def test_hedge_config_rejects_invalid(monkeypatch, raw):
    monkeypatch.setenv("LLM_UPSTREAM_HEDGE_DELAY_MS", raw)
    with pytest.raises(ValueError, match="LLM_UPSTREAM_HEDGE_DELAY_MS"):
        upstream_hedge_delay()


def test_stats_endpoint_exposes_upstream_amplification():
    with TestClient(main.app) as c:
        body = c.get("/v1/upstream/stats").json()
    assert {"requests", "attempts", "hedges", "hedges_denied", "hedge_wins",
            "failovers", "failed_attempts", "exhausted"} <= set(body)
    assert body["hedge_delay_ms"] == pytest.approx(165)
    assert len(body["upstreams"]) == 3
