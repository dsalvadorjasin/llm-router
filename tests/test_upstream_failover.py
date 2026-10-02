import asyncio
from collections.abc import Callable

import httpx
from fastapi.testclient import TestClient

from app import main
from app.upstream import UpstreamPool

URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]
OK = {"completion": "ok", "signature": "a" * 64}

Behavior = Callable[[httpx.Request], httpx.Response]


def _host(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


def _pool(behaviors: dict[str, Behavior], hits: list[str], **kwargs) -> UpstreamPool:
    async def handler(request: httpx.Request) -> httpx.Response:
        host = _host(request)
        hits.append(host)
        return behaviors.get(host, lambda r: httpx.Response(200, json=OK))(request)

    return UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler), **kwargs)


def _run(pool: UpstreamPool, n: int = 1) -> list[tuple[int, dict]]:
    async def go():
        try:
            return [await pool.forward({"prompt": "p"}) for _ in range(n)]
        finally:
            await pool.aclose()

    return asyncio.run(go())


def _status(code: int, **kw) -> Behavior:
    return lambda r: httpx.Response(code, **kw)


def _raise(exc_type: type[httpx.RequestError]) -> Behavior:
    def behavior(request: httpx.Request) -> httpx.Response:
        raise exc_type("boom", request=request)

    return behavior


def test_5xx_retries_next_replica():
    hits: list[str] = []
    pool = _pool({URLS[0]: _status(503, json={"detail": "model overloaded"})}, hits)
    assert _run(pool) == [(200, OK)]
    assert hits == URLS[:2]


def test_timeout_fails_over_to_next_replica():
    hits: list[str] = []
    pool = _pool({URLS[0]: _raise(httpx.ReadTimeout),
                  URLS[1]: _raise(httpx.ConnectTimeout)}, hits)
    assert _run(pool) == [(200, OK)]
    assert hits == URLS


def test_connection_error_fails_over():
    hits: list[str] = []
    pool = _pool({URLS[0]: _raise(httpx.ConnectError)}, hits)
    assert _run(pool) == [(200, OK)]
    assert hits == URLS[:2]


def test_decoding_error_fails_over():
    hits: list[str] = []
    pool = _pool({URLS[0]: _raise(httpx.DecodingError)}, hits)
    assert _run(pool) == [(200, OK)]
    assert hits == URLS[:2]


def test_all_replicas_timeout_returns_503_json():
    hits: list[str] = []
    pool = _pool({u: _raise(httpx.ReadTimeout) for u in URLS}, hits)
    assert _run(pool) == [(503, {"detail": "upstream read timeout"})]
    assert hits == URLS


def test_all_replicas_down_returns_503_json():
    hits: list[str] = []
    pool = _pool({u: _raise(httpx.ConnectError) for u in URLS}, hits)
    [(status, body)] = _run(pool)
    assert status == 503
    assert body["detail"] == "all upstreams unavailable"
    assert hits == URLS


def test_all_replicas_5xx_returns_502_json():
    hits: list[str] = []
    pool = _pool({u: _status(500, json={"detail": "x"}) for u in URLS}, hits)
    [(status, body)] = _run(pool)
    assert status == 502
    assert body["detail"] == "upstream error"
    assert hits == URLS


def test_non_json_success_body_fails_over():
    hits: list[str] = []
    pool = _pool({URLS[0]: _status(200, text="<html>oops</html>")}, hits)
    assert _run(pool) == [(200, OK)]
    assert hits == URLS[:2]


def test_non_json_everywhere_returns_clean_502():
    hits: list[str] = []
    pool = _pool({u: _status(200, text="not json") for u in URLS}, hits)
    [(status, body)] = _run(pool)
    assert status == 502
    assert body["detail"] == "upstream error"


def test_non_object_json_is_invalid():
    hits: list[str] = []
    pool = _pool({URLS[0]: _status(200, json=["not", "a", "dict"])}, hits)
    assert _run(pool) == [(200, OK)]
    assert hits == URLS[:2]


def test_4xx_passes_through_without_retry():
    hits: list[str] = []
    pool = _pool({URLS[0]: _status(422, json={"detail": "bad prompt"})}, hits)
    assert _run(pool) == [(422, {"detail": "bad prompt"})]
    assert hits == URLS[:1]


def test_non_json_4xx_returns_clean_502_without_retry():
    hits: list[str] = []
    pool = _pool({URLS[0]: _status(404, text="Not Found")}, hits)
    assert _run(pool) == [(502, {"detail": "invalid upstream response"})]
    assert hits == URLS[:1]


def test_max_attempts_caps_distinct_replicas():
    hits: list[str] = []
    pool = _pool({u: _status(503, json={}) for u in URLS}, hits, max_attempts=2)
    [(status, _)] = _run(pool)
    assert status == 502
    assert hits == URLS[:2]


def test_max_attempts_never_exceeds_upstream_count():
    hits: list[str] = []
    pool = _pool({u: _raise(httpx.ConnectError) for u in URLS}, hits, max_attempts=10)
    [(status, _)] = _run(pool)
    assert status == 503
    assert hits == URLS


def test_round_robin_start_advances_independently_of_retries():
    hits: list[str] = []
    pool = _pool({URLS[0]: _status(503, json={})}, hits)
    results = _run(pool, n=3)
    assert all(r == (200, OK) for r in results)
    # request 1: u1 fails -> u2; request 2 starts at u2; request 3 starts at u3
    assert hits == [URLS[0], URLS[1], URLS[1], URLS[2]]


def test_generate_endpoint_returns_json_503_when_all_down():
    hits: list[str] = []
    pool = _pool({u: _raise(httpx.ConnectError) for u in URLS}, hits)
    with TestClient(main.app) as c:
        main.app.state.pool = pool
        r = c.post("/v1/generate", json={"prompt": "hi", "max_tokens": 8})
    assert r.status_code == 503
    assert r.json()["detail"] == "all upstreams unavailable"
