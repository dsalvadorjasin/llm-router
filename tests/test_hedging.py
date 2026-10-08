import asyncio
import re
import time

import httpx
from fastapi.testclient import TestClient

from app import config, main
from app.request_context import request_log_fields
from app.upstream import UpstreamPool

URLS = ["http://u1:9000", "http://u2:9000", "http://u3:9000"]


def _host(request: httpx.Request) -> str:
    return f"{request.url.scheme}://{request.url.host}:{request.url.port}"


def _transport(behaviour: dict, hits: list[str], cancelled: list[str] | None = None):
    """behaviour maps url -> (delay_s, status, body) or an Exception to raise."""

    async def handler(request: httpx.Request) -> httpx.Response:
        url = _host(request)
        hits.append(url)
        spec = behaviour[url]
        if isinstance(spec, Exception):
            raise spec
        delay, status, body = spec
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            if cancelled is not None:
                cancelled.append(url)
            raise
        return httpx.Response(status, json=body)

    return httpx.MockTransport(handler)


def _env(monkeypatch, **values):
    for key, value in values.items():
        monkeypatch.setenv(key, str(value))


def _run(pool: UpstreamPool, payload=None, settle_s: float = 0.0):
    async def go():
        fields: dict = {}
        request_log_fields.set(fields)
        start = time.monotonic()
        result = await pool.forward(payload or {"prompt": "p"})
        elapsed = time.monotonic() - start
        await asyncio.sleep(settle_s)
        await pool.aclose()
        return result, elapsed, fields

    return asyncio.run(go())


def test_config_defaults(monkeypatch):
    for key in ("LLM_HEDGING", "LLM_HEDGE_DELAY_MS", "LLM_ATTEMPT_TIMEOUT_MS", "LLM_MAX_ATTEMPTS"):
        monkeypatch.delenv(key, raising=False)
    assert config.hedging_enabled() is True
    assert config.hedge_delay_ms() == 225
    assert config.attempt_timeout_ms() == 5000
    assert config.max_attempts() == 3
    monkeypatch.setenv("LLM_HEDGING", "0")
    assert config.hedging_enabled() is False


def test_hedge_fires_after_delay_and_fast_duplicate_wins(monkeypatch):
    _env(monkeypatch, LLM_HEDGE_DELAY_MS=50)
    hits: list[str] = []
    behaviour = {
        URLS[0]: (2.0, 200, {"completion": "slow", "signature": "s"}),
        URLS[1]: (0.0, 200, {"completion": "fast", "signature": "s"}),
        URLS[2]: (0.0, 200, {"completion": "third", "signature": "s"}),
    }
    pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
    (status, body), elapsed, fields = _run(pool)
    assert (status, body["completion"]) == (200, "fast")
    assert hits == [URLS[0], URLS[1]]
    assert 0.04 <= elapsed < 0.5
    assert fields == {"upstream_url": URLS[1], "hedged": True}


def test_loser_is_cancelled_and_its_body_never_returned(monkeypatch):
    _env(monkeypatch, LLM_HEDGE_DELAY_MS=30)
    hits: list[str] = []
    cancelled: list[str] = []
    behaviour = {
        URLS[0]: (0.3, 200, {"completion": "LOSER"}),
        URLS[1]: (0.01, 200, {"completion": "winner"}),
        URLS[2]: (0.0, 200, {"completion": "unused"}),
    }
    pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits, cancelled))
    (status, body), elapsed, fields = _run(pool, settle_s=0.5)
    assert (status, body) == (200, {"completion": "winner"})
    assert cancelled == [URLS[0]]
    assert elapsed < 0.3
    assert fields["upstream_url"] == URLS[1]


def test_further_hedges_are_bounded_by_max_attempts(monkeypatch):
    _env(monkeypatch, LLM_HEDGE_DELAY_MS=30, LLM_MAX_ATTEMPTS=3)
    hits: list[str] = []
    behaviour = {
        URLS[0]: (1.0, 200, {"completion": "a"}),
        URLS[1]: (1.0, 200, {"completion": "b"}),
        URLS[2]: (0.0, 200, {"completion": "c"}),
    }
    pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
    (status, body), elapsed, fields = _run(pool)
    assert (status, body) == (200, {"completion": "c"})
    assert hits == URLS
    assert elapsed < 0.5


def test_failover_on_5xx_and_transport_error(monkeypatch):
    _env(monkeypatch, LLM_HEDGE_DELAY_MS=10_000)
    hits: list[str] = []
    behaviour = {
        URLS[0]: (0.0, 503, {"detail": "overloaded"}),
        URLS[1]: httpx.ConnectError("refused"),
        URLS[2]: (0.0, 200, {"completion": "ok"}),
    }
    pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
    (status, body), elapsed, fields = _run(pool)
    assert (status, body) == (200, {"completion": "ok"})
    assert hits == URLS
    assert elapsed < 1.0
    assert fields == {"upstream_url": URLS[2], "hedged": False}


def test_failover_on_attempt_timeout(monkeypatch):
    _env(monkeypatch, LLM_HEDGING=0, LLM_ATTEMPT_TIMEOUT_MS=50)
    hits: list[str] = []
    behaviour = {
        URLS[0]: (5.0, 200, {"completion": "too late"}),
        URLS[1]: (0.0, 200, {"completion": "ok"}),
        URLS[2]: (0.0, 200, {"completion": "unused"}),
    }
    pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
    (status, body), elapsed, _ = _run(pool)
    assert (status, body) == (200, {"completion": "ok"})
    assert hits == [URLS[0], URLS[1]]
    assert elapsed < 1.0


def test_all_attempts_fail_returns_last_upstream_error(monkeypatch):
    _env(monkeypatch, LLM_HEDGE_DELAY_MS=10_000)
    hits: list[str] = []
    behaviour = {url: (0.0, 503, {"detail": f"down {i}"}) for i, url in enumerate(URLS)}
    pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
    (status, body), _, _ = _run(pool)
    assert status == 503 and body["detail"].startswith("down")
    assert hits == URLS


def test_all_attempts_unreachable_returns_502(monkeypatch):
    hits: list[str] = []
    behaviour = {url: httpx.ConnectError("refused") for url in URLS}
    pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
    (status, body), _, fields = _run(pool)
    assert status == 502
    assert fields["upstream_url"] == "-"


def test_all_attempts_time_out_returns_504(monkeypatch):
    _env(monkeypatch, LLM_HEDGING=0, LLM_ATTEMPT_TIMEOUT_MS=20)
    hits: list[str] = []
    behaviour = {url: (1.0, 200, {"completion": "late"}) for url in URLS}
    pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
    (status, _), _, _ = _run(pool)
    assert status == 504
    assert hits == URLS


def test_client_4xx_is_passed_through_without_retry():
    hits: list[str] = []
    behaviour = {url: (0.0, 422, {"detail": "bad prompt"}) for url in URLS}
    pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
    (status, body), _, _ = _run(pool)
    assert (status, body) == (422, {"detail": "bad prompt"})
    assert hits == [URLS[0]]


def test_no_hedge_when_first_attempt_is_fast(monkeypatch):
    _env(monkeypatch, LLM_HEDGE_DELAY_MS=50)
    hits: list[str] = []
    behaviour = {url: (0.005, 200, {"completion": url}) for url in URLS}
    pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
    (status, body), _, fields = _run(pool)
    assert (status, body) == (200, {"completion": URLS[0]})
    assert hits == [URLS[0]]
    assert fields == {"upstream_url": URLS[0], "hedged": False}


def test_kill_switch_disables_hedging(monkeypatch):
    _env(monkeypatch, LLM_HEDGING=0, LLM_HEDGE_DELAY_MS=10)
    hits: list[str] = []
    behaviour = {
        URLS[0]: (0.2, 200, {"completion": "slow but only"}),
        URLS[1]: (0.0, 200, {"completion": "never asked"}),
        URLS[2]: (0.0, 200, {"completion": "never asked"}),
    }
    pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
    (status, body), _, fields = _run(pool)
    assert (status, body) == (200, {"completion": "slow but only"})
    assert hits == [URLS[0]]
    assert fields["hedged"] is False


def test_request_log_includes_upstream_url_and_hedged(monkeypatch, caplog):
    _env(monkeypatch, LLM_HEDGE_DELAY_MS=30)
    hits: list[str] = []
    behaviour = {
        URLS[0]: (1.0, 200, {"completion": "slow"}),
        URLS[1]: (0.0, 200, {"completion": "fast"}),
        URLS[2]: (0.0, 200, {"completion": "unused"}),
    }
    with TestClient(main.app) as client:
        main.app.state.pool = UpstreamPool(urls=URLS, transport=_transport(behaviour, hits))
        with caplog.at_level("INFO", logger="llm-router.requests"):
            resp = client.post("/v1/generate", json={"prompt": "hi"})
            client.get("/v1/conversations")
    assert resp.json()["completion"] == "fast"
    generate_line, other_line = caplog.messages[-2:]
    assert f"upstream_url={URLS[1]}" in generate_line
    assert "hedged=true" in generate_line
    assert re.search(r"upstream_url=- hedged=-$", other_line)
