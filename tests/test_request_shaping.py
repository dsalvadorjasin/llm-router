import threading

import httpx
import pytest
from fastapi.testclient import TestClient

from app import main
from app.config import history_limit
from app.store import Store
from app.upstream import UpstreamPool


class EchoPool:
    def __init__(self):
        self.calls: list[dict] = []

    async def forward(self, payload):
        self.calls.append(payload)
        return 200, {"completion": f"answer {len(self.calls)}", "model": "mock"}

    async def aclose(self):
        pass


def _client(pool):
    c = TestClient(main.app)
    c.__enter__()
    main.app.state.pool = pool
    return c


def _history_lines(prompt: str) -> list[str]:
    body = prompt.split("\n")[2:-2]  # drop preamble, blank, new user turn, "Assistant:"
    return body


@pytest.mark.parametrize("raw,expected", [
    (None, 50), ("10", 10), ("0", None), ("", None), (" 3 ", 3), ("-1", None),
])
def test_history_limit_config(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv("LLM_HISTORY_LIMIT", raising=False)
    else:
        monkeypatch.setenv("LLM_HISTORY_LIMIT", raw)
    assert history_limit() == expected


def test_history_cap_keeps_most_recent_messages_in_order(monkeypatch):
    monkeypatch.setenv("LLM_HISTORY_LIMIT", "3")
    pool = EchoPool()
    c = _client(pool)
    cid = c.post("/v1/conversations", json={}).json()["id"]
    for i in range(1, 5):
        assert c.post("/v1/chat", json={"conversation_id": cid, "message": f"m{i}"}).status_code == 200
    msgs = c.get(f"/v1/conversations/{cid}/messages").json()
    c.__exit__(None, None, None)

    # 4th turn sees only the 3 most recent of the 6 stored messages, oldest first
    assert _history_lines(pool.calls[3]["prompt"]) == [
        "Assistant: answer 2", "User: m3", "Assistant: answer 3",
    ]
    assert pool.calls[3]["prompt"].endswith("User: m4\nAssistant:")
    # the cap only shapes the prompt; everything is still persisted
    assert len(msgs) == 8


@pytest.mark.parametrize("raw", ["0", ""])
def test_history_unlimited(monkeypatch, raw):
    monkeypatch.setenv("LLM_HISTORY_LIMIT", raw)
    pool = EchoPool()
    c = _client(pool)
    cid = c.post("/v1/conversations", json={}).json()["id"]
    for i in range(1, 4):
        c.post("/v1/chat", json={"conversation_id": cid, "message": f"m{i}"})
    c.__exit__(None, None, None)
    assert len(_history_lines(pool.calls[2]["prompt"])) == 4


def test_auto_title_only_on_first_turn_with_cap(monkeypatch):
    monkeypatch.setenv("LLM_HISTORY_LIMIT", "1")
    pool = EchoPool()
    c = _client(pool)
    cid = c.post("/v1/conversations", json={}).json()["id"]
    c.post("/v1/chat", json={"conversation_id": cid, "message": "first question"})
    c.patch(f"/v1/conversations/{cid}", json={"title": "New conversation"})
    c.post("/v1/chat", json={"conversation_id": cid, "message": "second question"})
    title = [x for x in c.get("/v1/conversations").json() if x["id"] == cid][0]["title"]
    c.__exit__(None, None, None)
    assert title == "New conversation"


def test_chat_store_calls_run_off_event_loop_thread():
    pool = EchoPool()
    c = _client(pool)
    cid = c.post("/v1/conversations", json={}).json()["id"]
    store = main.app.state.store
    threads: dict[str, int] = {}
    loop_thread: list[int] = []

    for name in ("get_conversation", "list_messages", "update_conversation", "add_message"):
        orig = getattr(store, name)

        def wrap(*a, _orig=orig, _name=name, **kw):
            threads[_name] = threading.get_ident()
            return _orig(*a, **kw)

        setattr(store, name, wrap)

    class ThreadPool(EchoPool):
        async def forward(self, payload):
            loop_thread.append(threading.get_ident())
            return await super().forward(payload)

    main.app.state.pool = ThreadPool()
    resp = c.post("/v1/chat", json={"conversation_id": cid, "message": "hi"})
    c.__exit__(None, None, None)

    assert resp.status_code == 200
    assert set(threads) == {"get_conversation", "list_messages", "update_conversation", "add_message"}
    assert all(t != loop_thread[0] for t in threads.values())


def test_store_is_safe_under_concurrent_threads(tmp_path):
    store = Store(str(tmp_path / "c.db"))
    cid = store.create_conversation()["id"]
    errors: list[BaseException] = []

    def worker(n):
        try:
            for i in range(50):
                store.add_message(cid, "user", f"{n}-{i}")
                store.list_messages(cid, limit=5)
                store.get_conversation(cid)
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    ts = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert errors == []
    assert len(store.list_messages(cid)) == 400
    store.close()


def test_log_line_has_upstream_url_and_hedged(caplog):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"completion": "ok", "signature": "s"})

    pool = UpstreamPool(urls=["http://u1:9000"], transport=httpx.MockTransport(handler))
    with TestClient(main.app) as client:
        main.app.state.pool = pool
        cid = client.post("/v1/conversations", json={}).json()["id"]
        with caplog.at_level("INFO", logger="llm-router.requests"):
            r1 = client.post("/v1/generate", json={"prompt": "hi"})
            r2 = client.post("/v1/chat", json={"conversation_id": cid, "message": "hi"})
            r3 = client.get("/v1/conversations")
    assert (r1.status_code, r2.status_code, r3.status_code) == (200, 200, 200)
    gen, chat, listing = [m for m in caplog.messages if m.startswith("request ")][-3:]
    assert "path=/v1/generate" in gen and "upstream_url=http://u1:9000 hedged=false" in gen
    assert "path=/v1/chat" in chat and "upstream_url=http://u1:9000 hedged=false" in chat
    assert "path=/v1/conversations" in listing and "upstream_url=- hedged=-" in listing


def test_concurrent_turns_are_stored_as_adjacent_pairs(tmp_path):
    store = Store(str(tmp_path / "turns.db"))
    conv = store.create_conversation()
    threads = [threading.Thread(target=store.add_turn, args=(conv["id"], f"u{i}", f"a{i}"))
               for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    msgs = store.list_messages(conv["id"])
    assert len(msgs) == 40
    for user, assistant in zip(msgs[::2], msgs[1::2]):
        assert (user["role"], assistant["role"]) == ("user", "assistant")
        assert user["content"][1:] == assistant["content"][1:]
