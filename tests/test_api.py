import httpx
from fastapi.testclient import TestClient

from app import main


class FakePool:
    def __init__(self, status=200, body=None):
        self.status = status
        self.body = body or {"completion": "ok", "signature": "ab" * 32}
        self.calls: list[dict] = []

    async def forward(self, payload):
        self.calls.append(payload)
        return self.status, self.body

    async def aclose(self):
        pass


def client_with(pool: FakePool) -> TestClient:
    c = TestClient(main.app)
    c.__enter__()
    main.app.state.pool = pool
    return c


def test_generate_passes_through_body_and_payload():
    pool = FakePool()
    c = client_with(pool)
    r = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
    c.__exit__(None, None, None)
    assert r.status_code == 200
    assert r.json() == pool.body
    assert pool.calls == [{"prompt": "hi", "max_tokens": 8}]


def test_generate_passes_through_error_status():
    pool = FakePool(status=503, body={"detail": "model overloaded"})
    c = client_with(pool)
    r = c.post("/v1/generate", json={"prompt": "hi"})
    c.__exit__(None, None, None)
    assert r.status_code == 503


def test_generate_validates_prompt():
    pool = FakePool()
    c = client_with(pool)
    r = c.post("/v1/generate", json={})
    c.__exit__(None, None, None)
    assert r.status_code == 422


def _client_with_cache_env(monkeypatch, pool: FakePool, **env) -> TestClient:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return client_with(pool)


def test_generate_cache_enabled_by_default(monkeypatch):
    for name in ("RESPONSE_CACHE_ENABLED", "RESPONSE_CACHE_TTL_S",
                 "RESPONSE_CACHE_MAX_ENTRIES", "RESPONSE_CACHE_COALESCE"):
        monkeypatch.delenv(name, raising=False)
    pool = FakePool()
    c = client_with(pool)
    first = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
    second = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
    other_tokens = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 16})
    c.__exit__(None, None, None)
    assert first.status_code == second.status_code == other_tokens.status_code == 200
    assert first.json() == second.json() == pool.body
    assert first.headers["x-cache"] == "MISS"
    assert second.headers["x-cache"] == "HIT"
    assert other_tokens.headers["x-cache"] == "MISS"
    assert pool.calls == [
        {"prompt": "hi", "max_tokens": 8},
        {"prompt": "hi", "max_tokens": 16},
    ]


def test_generate_does_not_cache_errors(monkeypatch):
    pool = FakePool(status=503, body={"detail": "model overloaded"})
    c = _client_with_cache_env(monkeypatch, pool, RESPONSE_CACHE_ENABLED="1")
    first = c.post("/v1/generate", json={"prompt": "hi"})
    second = c.post("/v1/generate", json={"prompt": "hi"})
    c.__exit__(None, None, None)
    assert first.status_code == second.status_code == 503
    assert second.json() == {"detail": "model overloaded"}
    assert len(pool.calls) == 2


def test_generate_cache_off_forwards_every_request(monkeypatch):
    pool = FakePool()
    c = _client_with_cache_env(monkeypatch, pool, RESPONSE_CACHE_ENABLED="0")
    assert main.app.state.cache is None
    responses = [c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8}) for _ in range(3)]
    c.__exit__(None, None, None)
    assert all(r.status_code == 200 and r.json() == pool.body for r in responses)
    assert all("x-cache" not in r.headers for r in responses)
    assert pool.calls == [{"prompt": "hi", "max_tokens": 8}] * 3


def test_generate_cache_respects_configured_ttl_and_size(monkeypatch):
    pool = FakePool()
    c = _client_with_cache_env(monkeypatch, pool, RESPONSE_CACHE_TTL_S="12.5",
                               RESPONSE_CACHE_MAX_ENTRIES="7", RESPONSE_CACHE_COALESCE="off")
    cache = main.app.state.cache
    c.__exit__(None, None, None)
    assert cache.ttl_s == 12.5
    assert cache.max_entries == 7
    assert cache.coalesce is False
