import asyncio
import json
import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

import httpx
import pytest
from fastapi.testclient import TestClient

from app import main
from app.config import upstream_timeout
from app.upstream import UpstreamPool

_TIMEOUT_VARS = [f"LLM_UPSTREAM_{p}_TIMEOUT" for p in ("CONNECT", "READ", "WRITE", "POOL")]


@pytest.fixture(autouse=True)
def _clear_timeout_env(monkeypatch):
    for var in _TIMEOUT_VARS:
        monkeypatch.delenv(var, raising=False)


@contextmanager
def _replica(mode: str) -> Iterator[str]:
    """Real TCP HTTP replica on localhost.

    mode="ok": answers every request with a JSON completion.
    mode="hang": reads the request and never answers.
    mode="stall_body": sends headers and part of the body, then stalls.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    srv.settimeout(0.05)
    stop = threading.Event()
    conns: list[socket.socket] = []

    def handle(conn: socket.socket) -> None:
        conn.settimeout(0.05)
        buf = b""
        while not stop.is_set() and b"\r\n\r\n" not in buf:
            try:
                chunk = conn.recv(65536)
            except TimeoutError:
                continue
            except OSError:
                return
            if not chunk:
                return
            buf += chunk
        if mode == "ok":
            body = json.dumps({"completion": "ok", "signature": "ab" * 32}).encode()
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
        elif mode == "stall_body":
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         b"Content-Length: 100\r\n\r\n{\"completion\":")
        stop.wait()

    def serve() -> None:
        while not stop.is_set():
            try:
                conn, _ = srv.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            conns.append(conn)
            threading.Thread(target=handle, args=(conn,), daemon=True).start()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{srv.getsockname()[1]}"
    finally:
        stop.set()
        thread.join(timeout=2)
        for conn in conns:
            conn.close()
        srv.close()


def _timed_forward(pool: UpstreamPool, n: int = 1) -> list[tuple[float, tuple[int, dict]]]:
    async def run():
        out = []
        for _ in range(n):
            start = time.monotonic()
            result = await pool.forward({"prompt": "p", "max_tokens": 8})
            out.append((time.monotonic() - start, result))
        await pool.aclose()
        return out

    return asyncio.run(run())


def test_timeout_defaults_are_bounded():
    t = upstream_timeout()
    assert (t.connect, t.read, t.write, t.pool) == (2.0, 10.0, 5.0, 5.0)


def test_timeout_env_overrides(monkeypatch):
    monkeypatch.setenv("LLM_UPSTREAM_CONNECT_TIMEOUT", "0.5")
    monkeypatch.setenv("LLM_UPSTREAM_READ_TIMEOUT", "3")
    monkeypatch.setenv("LLM_UPSTREAM_WRITE_TIMEOUT", "1.5")
    monkeypatch.setenv("LLM_UPSTREAM_POOL_TIMEOUT", "0.25")
    t = upstream_timeout()
    assert (t.connect, t.read, t.write, t.pool) == (0.5, 3.0, 1.5, 0.25)


@pytest.mark.parametrize("raw", ["abc", "0", "-1", "inf", "nan"])
def test_timeout_env_rejects_unbounded_or_invalid(monkeypatch, raw):
    monkeypatch.setenv("LLM_UPSTREAM_READ_TIMEOUT", raw)
    with pytest.raises(ValueError, match="LLM_UPSTREAM_READ_TIMEOUT"):
        upstream_timeout()


def test_pool_client_uses_env_timeouts(monkeypatch):
    monkeypatch.setenv("LLM_UPSTREAM_CONNECT_TIMEOUT", "0.7")
    monkeypatch.setenv("LLM_UPSTREAM_READ_TIMEOUT", "4.5")
    pool = UpstreamPool(urls=["http://u1:9000"])
    t = pool._client.timeout
    asyncio.run(pool.aclose())
    assert (t.connect, t.read) == (0.7, 4.5)


def test_hung_replica_read_timeout_returns_504_and_round_robin_continues(monkeypatch):
    monkeypatch.setenv("LLM_UPSTREAM_READ_TIMEOUT", "0.3")
    with _replica("hang") as hung, _replica("ok") as healthy:
        pool = UpstreamPool(urls=[hung, healthy], max_attempts=1)
        (t_hung, r_hung), (t_ok, r_ok) = _timed_forward(pool, n=2)

    assert r_hung == (504, {"detail": "upstream read timeout"})
    assert 0.25 <= t_hung < 2.0
    assert r_ok == (200, {"completion": "ok", "signature": "ab" * 32})
    assert t_ok < 1.0


def test_hung_replica_read_timeout_fails_over_to_healthy_replica(monkeypatch):
    monkeypatch.setenv("LLM_UPSTREAM_READ_TIMEOUT", "0.3")
    with _replica("hang") as hung, _replica("ok") as healthy:
        pool = UpstreamPool(urls=[hung, healthy])
        [(elapsed, result)] = _timed_forward(pool)

    assert result == (200, {"completion": "ok", "signature": "ab" * 32})
    assert 0.25 <= elapsed < 2.0


def test_stalled_response_body_hits_read_timeout():
    with _replica("stall_body") as stalled:
        pool = UpstreamPool(urls=[stalled], timeout=httpx.Timeout(5.0, read=0.3))
        [(elapsed, result)] = _timed_forward(pool)

    assert result == (504, {"detail": "upstream read timeout"})
    assert elapsed < 2.0


def test_unaccepting_replica_hits_connect_timeout():
    # A listener with a full accept queue that never accepts: Linux drops new SYNs,
    # so the TCP handshake never completes.
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(0)
    port = srv.getsockname()[1]
    fillers = []
    try:
        for _ in range(4):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setblocking(False)
            s.connect_ex(("127.0.0.1", port))
            fillers.append(s)
        time.sleep(0.1)
        pool = UpstreamPool(urls=[f"http://127.0.0.1:{port}"],
                            timeout=httpx.Timeout(5.0, connect=0.3))
        [(elapsed, result)] = _timed_forward(pool)
    finally:
        for s in fillers:
            s.close()
        srv.close()

    assert result == (504, {"detail": "upstream connect timeout"})
    assert elapsed < 2.0


def test_generate_and_chat_surface_upstream_timeout_as_504(monkeypatch):
    monkeypatch.setenv("LLM_UPSTREAM_READ_TIMEOUT", "0.3")
    with _replica("hang") as hung:
        c = TestClient(main.app)
        c.__enter__()
        main.app.state.pool = UpstreamPool(urls=[hung])
        try:
            gen = c.post("/v1/generate", json={"prompt": "hi"})
            cid = c.post("/v1/conversations", json={}).json()["id"]
            chat = c.post("/v1/chat", json={"conversation_id": cid, "message": "hi"})
            msgs = c.get(f"/v1/conversations/{cid}/messages").json()
        finally:
            c.__exit__(None, None, None)

    assert gen.status_code == 504
    assert gen.json() == {"detail": "upstream read timeout"}
    assert chat.status_code == 504
    assert chat.json() == {"detail": "upstream read timeout"}
    assert msgs == []
