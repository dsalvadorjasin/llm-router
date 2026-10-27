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


def test_all_attempts_fail_raises_httpx_error():
    hits: list[str] = []
    pool = UpstreamPool(urls=URLS, transport=_scripted_transport(hits, set(URLS)))

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
    assert hits == ["http://u1:9000", "http://u2:9000"]  # one retry only


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
