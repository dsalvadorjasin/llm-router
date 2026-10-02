import asyncio
from collections import Counter

import httpx
import pytest

from app.upstream import UpstreamPool

URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]


def _origin(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


class GatedTransport(httpx.AsyncBaseTransport):
    """Records hits and holds each request until its upstream's gate is opened."""

    def __init__(self, urls: list[str]):
        self.hits: list[str] = []
        self.gates = {u: asyncio.Event() for u in urls}
        self.fail: set[str] = set()
        self.status: dict[str, int] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = _origin(request)
        self.hits.append(url)
        await self.gates[url].wait()
        if url in self.fail:
            raise httpx.ConnectError("boom", request=request)
        return httpx.Response(self.status.get(url, 200), json={"completion": url})


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


async def _settle():
    for _ in range(5):
        await asyncio.sleep(0)


def test_idle_fleet_ties_rotate_evenly():
    async def run():
        t = GatedTransport(URLS)
        for g in t.gates.values():
            g.set()
        pool = UpstreamPool(urls=URLS, transport=t)
        for _ in range(30):
            await pool.forward({"prompt": "p"})
        await pool.aclose()
        return t.hits

    hits = asyncio.run(run())
    assert hits[:4] == [URLS[0], URLS[1], URLS[2], URLS[0]]
    assert Counter(hits) == {u: 10 for u in URLS}


def test_busy_upstream_is_avoided_until_it_frees_up():
    async def run():
        t = GatedTransport(URLS)
        t.gates[URLS[1]].set()
        t.gates[URLS[2]].set()
        pool = UpstreamPool(urls=URLS, transport=t)

        stuck = asyncio.create_task(pool.forward({"prompt": "slow"}))
        await _settle()
        assert pool.inflight() == {URLS[0]: 1, URLS[1]: 0, URLS[2]: 0}

        for _ in range(6):
            await pool.forward({"prompt": "p"})
        busy_phase = list(t.hits[1:])

        t.gates[URLS[0]].set()
        await stuck
        assert pool.inflight() == {u: 0 for u in URLS}
        t.hits.clear()
        for _ in range(3):
            await pool.forward({"prompt": "p"})
        await pool.aclose()
        return busy_phase, list(t.hits)

    busy_phase, after = asyncio.run(run())
    assert URLS[0] not in busy_phase
    assert Counter(busy_phase) == {URLS[1]: 3, URLS[2]: 3}
    assert sorted(after) == sorted(URLS)


def test_concurrent_requests_spread_by_inflight_count():
    async def run():
        t = GatedTransport(URLS)
        pool = UpstreamPool(urls=URLS, transport=t)
        tasks = [asyncio.create_task(pool.forward({"prompt": str(i)})) for i in range(9)]
        await _settle()
        snapshot = pool.inflight()
        for g in t.gates.values():
            g.set()
        await asyncio.gather(*tasks)
        await pool.aclose()
        return snapshot, pool.inflight()

    during, after = asyncio.run(run())
    assert during == {u: 3 for u in URLS}
    assert after == {u: 0 for u in URLS}


def test_transport_error_decrements_and_skips_failed_upstream_during_cooldown():
    async def run():
        t = GatedTransport(URLS)
        for g in t.gates.values():
            g.set()
        t.fail.add(URLS[0])
        clock = FakeClock()
        pool = UpstreamPool(urls=URLS, transport=t, failure_cooldown_s=1.0, clock=clock,
                            max_attempts=1)

        status, _ = await pool.forward({"prompt": "p"})
        assert status == 503
        assert pool.inflight() == {u: 0 for u in URLS}

        t.fail.clear()
        t.hits.clear()
        for _ in range(4):
            await pool.forward({"prompt": "p"})
        during_cooldown = list(t.hits)

        clock.now = 1.5
        t.hits.clear()
        for _ in range(3):
            await pool.forward({"prompt": "p"})
        await pool.aclose()
        return during_cooldown, list(t.hits)

    during_cooldown, after_cooldown = asyncio.run(run())
    assert URLS[0] not in during_cooldown
    assert URLS[0] in after_cooldown


def test_5xx_marks_upstream_failed_and_exhausted_attempt_returns_502():
    async def run():
        t = GatedTransport(URLS)
        for g in t.gates.values():
            g.set()
        t.status[URLS[0]] = 503
        pool = UpstreamPool(urls=URLS, transport=t, clock=FakeClock(), max_attempts=1)
        first = await pool.forward({"prompt": "p"})
        t.hits.clear()
        for _ in range(4):
            await pool.forward({"prompt": "p"})
        await pool.aclose()
        return first, list(t.hits), pool.inflight()

    first, hits, inflight = asyncio.run(run())
    assert first[0] == 502
    assert first[1]["detail"] == "upstream error"
    assert URLS[0] not in hits
    assert inflight == {u: 0 for u in URLS}


def test_all_upstreams_failed_still_routes_instead_of_starving():
    async def run():
        t = GatedTransport(URLS)
        for g in t.gates.values():
            g.set()
        t.fail.update(URLS)
        pool = UpstreamPool(urls=URLS, transport=t, clock=FakeClock(), max_attempts=1)
        for _ in range(3):
            status, _ = await pool.forward({"prompt": "p"})
            assert status == 503
        t.fail.clear()
        t.hits.clear()
        results = [await pool.forward({"prompt": "p"}) for _ in range(3)]
        await pool.aclose()
        return results, list(t.hits)

    results, hits = asyncio.run(run())
    assert all(status == 200 for status, _ in results)
    assert sorted(hits) == sorted(URLS)


def test_cancellation_decrements_inflight_without_marking_failure():
    async def run():
        t = GatedTransport(URLS)
        pool = UpstreamPool(urls=URLS, transport=t, clock=FakeClock())
        task = asyncio.create_task(pool.forward({"prompt": "p"}))
        await _settle()
        assert pool.inflight()[URLS[0]] == 1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        after_cancel = pool.inflight()

        for g in t.gates.values():
            g.set()
        t.hits.clear()
        for _ in range(3):
            await pool.forward({"prompt": "p"})
        await pool.aclose()
        return after_cancel, list(t.hits)

    after_cancel, hits = asyncio.run(run())
    assert after_cancel == {u: 0 for u in URLS}
    assert sorted(hits) == sorted(URLS)


def test_empty_url_list_is_rejected(monkeypatch):
    monkeypatch.setenv("LLM_SERVICE_URLS", " , ")
    with pytest.raises(ValueError):
        UpstreamPool()
