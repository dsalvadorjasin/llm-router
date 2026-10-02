import asyncio

import httpx
import pytest

from app.config import upstream_urls
from app.upstream import UpstreamPool, redact_url


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
    assert results == [(200, {"completion": "ok"}, u) for u in [*urls, urls[0]]]


def test_forward_logs_selected_url_and_status(caplog):
    pool = UpstreamPool(urls=["http://u1:9000"], transport=_recording_transport([]))

    async def run():
        with caplog.at_level("INFO", logger="llm-router.upstream"):
            await pool.forward({"prompt": "p"}, request_id="abc123")
        await pool.aclose()

    asyncio.run(run())
    line = "".join(caplog.messages)
    assert "url=http://u1:9000" in line
    assert "status=200" in line
    assert "request_id=abc123" in line


def test_forward_logs_url_and_reraises_on_connection_error(caplog):
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    pool = UpstreamPool(urls=["http://u1:9000"], transport=httpx.MockTransport(handler))

    async def run():
        try:
            with caplog.at_level("INFO", logger="llm-router.upstream"):
                await pool.forward({"prompt": "p"}, request_id="abc123")
        finally:
            await pool.aclose()

    with pytest.raises(httpx.ConnectError):
        asyncio.run(run())
    [record] = caplog.records
    assert record.levelname == "WARNING"
    assert "url=http://u1:9000" in record.getMessage()
    assert "error=ConnectError" in record.getMessage()
    assert "request_id=abc123" in record.getMessage()


def test_forward_redacts_url_credentials_in_log_and_return(caplog):
    hits: list[str] = []
    pool = UpstreamPool(urls=["http://user:s3cret@u1:9000"],
                        transport=_recording_transport(hits))

    async def run():
        with caplog.at_level("INFO", logger="llm-router.upstream"):
            result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    _, _, url = asyncio.run(run())
    assert url == "http://u1:9000"
    assert "s3cret" not in "".join(caplog.messages)
    assert "url=http://u1:9000" in "".join(caplog.messages)


@pytest.mark.parametrize("url, expected", [
    ("http://u1:9000", "http://u1:9000"),
    ("http://user:pw@u1:9000", "http://u1:9000"),
    ("https://token@host/base", "https://host/base"),
    ("http://user:p@ss@[::1]:9000", "http://[::1]:9000"),
])
def test_redact_url(url, expected):
    assert redact_url(url) == expected
