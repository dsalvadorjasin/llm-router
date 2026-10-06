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


def test_generate_caches_repeat_requests():
    pool = FakePool()
    c = client_with(pool)
    r1 = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
    r2 = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
    c.__exit__(None, None, None)
    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.json() == r2.json() == pool.body
    assert len(pool.calls) == 1


def test_generate_does_not_cache_errors():
    pool = FakePool(status=503, body={"detail": "model overloaded"})
    c = client_with(pool)
    r1 = c.post("/v1/generate", json={"prompt": "hi"})
    r2 = c.post("/v1/generate", json={"prompt": "hi"})
    c.__exit__(None, None, None)
    assert r1.status_code == 503 and r2.status_code == 503
    assert len(pool.calls) == 2


def test_generate_cache_disabled(monkeypatch):
    monkeypatch.setenv("RESPONSE_CACHE_ENABLED", "0")
    pool = FakePool()
    c = client_with(pool)
    c.post("/v1/generate", json={"prompt": "hi"})
    c.post("/v1/generate", json={"prompt": "hi"})
    c.__exit__(None, None, None)
    assert len(pool.calls) == 2
