import asyncio

import httpx
import pytest

from app.config import HedgeSettings, upstream_urls
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


def test_round_robin_and_passthrough_when_hedging_disabled():
    hits: list[str] = []
    urls = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]
    pool = UpstreamPool(
        urls=urls,
        transport=_recording_transport(hits),
        settings=HedgeSettings(enabled=False),
        strategy="round_robin",
    )

    async def run():
        results = [await pool.forward({"prompt": "p"}) for _ in range(4)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert hits == ["http://u1:9000", "http://u2:9000", "http://u3:9000", "http://u1:9000"]
    assert all(r == (200, {"completion": "ok"}) for r in results)


@pytest.mark.parametrize(
    "malformed_body",
    [[], {}, {"completion": ""}, {"completion": 1}],
)
def test_malformed_2xx_fails_over_when_hedging_is_disabled(malformed_body):
    hits = []

    async def handler(request):
        hits.append(request.url.host)
        if request.url.host == "u1":
            return httpx.Response(200, json=malformed_body)
        return httpx.Response(200, json={"completion": "good"})

    pool = UpstreamPool(
        urls=["http://u1:9000", "http://u2:9000", "http://u3:9000"],
        transport=httpx.MockTransport(handler),
        settings=HedgeSettings(enabled=False),
        strategy="round_robin",
        max_attempts=2,
    )

    async def run():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert asyncio.run(run()) == (200, {"completion": "good"})
    assert hits == ["u1", "u2"]
    assert pool.stats["failovers"] == 1


def test_cancelled_simple_forward_clears_pending_attempt():
    started = asyncio.Event()

    async def handler(request):
        started.set()
        await asyncio.sleep(5)
        return httpx.Response(200, json={"completion": "ok"})

    pool = UpstreamPool(
        urls=["http://u1:9000"],
        transport=httpx.MockTransport(handler),
        settings=HedgeSettings(enabled=False),
        attempt_timeout_s=10,
    )

    async def run():
        task = asyncio.create_task(pool.forward({"prompt": "p"}))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        inflight = pool._replicas[0].inflight
        await pool.aclose()
        return inflight

    assert asyncio.run(run()) == 0
