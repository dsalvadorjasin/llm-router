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


def test_cache_config_defaults(monkeypatch):
    from app import config

    for name in ("RESPONSE_CACHE_ENABLED", "RESPONSE_CACHE_TTL_S",
                 "RESPONSE_CACHE_MAX_ENTRIES", "RESPONSE_CACHE_COALESCE"):
        monkeypatch.delenv(name, raising=False)
    assert config.cache_enabled() is True
    assert config.cache_ttl_s() == 300.0
    assert config.cache_max_entries() == 1024
    assert config.cache_coalesce() is True


def test_cache_config_falsy_values_disable(monkeypatch):
    from app import config

    for value in ("0", "false", "No", "OFF"):
        monkeypatch.setenv("RESPONSE_CACHE_ENABLED", value)
        assert config.cache_enabled() is False
    monkeypatch.setenv("RESPONSE_CACHE_ENABLED", "1")
    assert config.cache_enabled() is True
