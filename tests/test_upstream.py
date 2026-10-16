import asyncio

import httpx
import pytest

from app.config import upstream_urls
from app.upstream import UpstreamPool, pick_backend


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


def test_prompt_affinity_and_passthrough():
    hits: list[str] = []
    urls = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]
    pool = UpstreamPool(urls=urls, transport=_recording_transport(hits))

    async def run():
        results = [await pool.forward({"prompt": "p", "max_tokens": 64}) for _ in range(4)]
        await pool.aclose()
        return results

    results = asyncio.run(run())
    assert len(hits) == 4 and len(set(hits)) == 1
    assert hits[0] == pick_backend(urls, {"prompt": "p"})
    assert all(r == (200, {"completion": "ok"}) for r in results)


def test_pick_backend_is_stable_and_spreads():
    urls = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]
    prompts = [f"prompt {i}" for i in range(60)]
    first = [pick_backend(urls, {"prompt": p}) for p in prompts]
    assert first == [pick_backend(list(urls), {"prompt": p, "max_tokens": 8}) for p in prompts]
    assert set(first) == set(urls)


def test_pick_backend_without_prompt_hashes_payload():
    urls = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]
    assert pick_backend(urls, {"b": 1, "a": 2}) == pick_backend(urls, {"a": 2, "b": 1})


def test_pick_backend_single_and_empty():
    assert pick_backend(["http://only:9000"], {"prompt": "x"}) == "http://only:9000"
    with pytest.raises(RuntimeError):
        pick_backend([], {"prompt": "x"})
