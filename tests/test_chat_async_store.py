"""Store calls from async routes run off the event loop, and the shared
sqlite connection stays consistent under concurrent threaded access."""
import asyncio
import threading
import time

import httpx
from fastapi import FastAPI

from app.routes.chat import router as chat_router
from app.routes.conversations import router as conversations_router
from app.routes.info import router as info_router
from app.store import Store

SLOW_S = 0.5


class EchoPool:
    """Replies with the last user line after an async delay."""

    def __init__(self, delay_s: float = 0.0):
        self.delay_s = delay_s
        self.calls = 0

    async def forward(self, payload):
        self.calls += 1
        await asyncio.sleep(self.delay_s)
        last = payload["prompt"].rsplit("User: ", 1)[-1].removesuffix("\nAssistant:")
        return 200, {"completion": f"echo {last}", "model": "mock-large", "usage": {}}


class SlowReadStore(Store):
    """`list_messages` blocks its calling thread, like a slow disk/query."""

    def list_messages(self, *args, **kwargs):
        time.sleep(SLOW_S)
        return super().list_messages(*args, **kwargs)


class SlowWriteStore(Store):
    def add_message(self, *args, **kwargs):
        time.sleep(0.02)
        return super().add_message(*args, **kwargs)


def make_app(store: Store, pool: EchoPool) -> FastAPI:
    app = FastAPI()
    app.include_router(conversations_router)
    app.include_router(chat_router)
    app.include_router(info_router)
    app.state.store = store
    app.state.pool = pool
    return app


def client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def _max_loop_stall(stop: asyncio.Event) -> float:
    worst = 0.0
    last = time.monotonic()
    while not stop.is_set():
        await asyncio.sleep(0.01)
        now = time.monotonic()
        worst = max(worst, now - last)
        last = now
    return worst


async def _measure_while(coro):
    stop = asyncio.Event()
    ticker = asyncio.create_task(_max_loop_stall(stop))
    await asyncio.sleep(0.02)
    result = await coro
    stop.set()
    return result, await ticker


def test_slow_store_read_in_chat_does_not_block_event_loop(tmp_path):
    store = SlowReadStore(str(tmp_path / "db.sqlite"))
    cid = store.create_conversation()["id"]
    app = make_app(store, EchoPool())

    async def scenario():
        async with client(app) as c:
            chat = asyncio.create_task(
                c.post("/v1/chat", json={"conversation_id": cid, "message": "hi"})
            )
            await asyncio.sleep(0.05)  # chat is now inside the slow store read
            t0 = time.monotonic()
            info = await c.get("/v1/info")
            info_s = time.monotonic() - t0
            return await chat, info, info_s, chat.done()

    (chat, info, info_s, _), stall = asyncio.run(_measure_while(scenario()))
    store.close()

    assert chat.status_code == 200
    assert chat.json()["message"]["content"] == "echo hi"
    assert info.status_code == 200
    assert info_s < SLOW_S / 2  # served while the chat's store read was still running
    assert stall < SLOW_S / 2


def test_slow_store_read_in_conversation_endpoint_does_not_block_event_loop(tmp_path):
    store = SlowReadStore(str(tmp_path / "db.sqlite"))
    cid = store.create_conversation()["id"]
    app = make_app(store, EchoPool())

    async def scenario():
        async with client(app) as c:
            return await asyncio.gather(
                c.get(f"/v1/conversations/{cid}/messages"),
                c.get(f"/v1/conversations/{cid}/export"),
            )

    (messages, export), stall = asyncio.run(_measure_while(scenario()))
    store.close()

    assert messages.status_code == 200 and messages.json() == []
    assert export.status_code == 200
    assert stall < SLOW_S / 2


def test_concurrent_chats_on_separate_conversations(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    n = 12
    delay = 0.3
    cids = [store.create_conversation()["id"] for _ in range(n)]
    pool = EchoPool(delay_s=delay)
    app = make_app(store, pool)

    async def scenario():
        async with client(app) as c:
            t0 = time.monotonic()
            resps = await asyncio.gather(*(
                c.post("/v1/chat", json={"conversation_id": cid, "message": f"m{i}"})
                for i, cid in enumerate(cids)
            ))
            elapsed = time.monotonic() - t0
            listings = await asyncio.gather(*(
                c.get(f"/v1/conversations/{cid}/messages") for cid in cids
            ))
            titles = {row["id"]: row["title"] for row in (await c.get("/v1/conversations")).json()}
            return resps, elapsed, listings, titles

    resps, elapsed, listings, titles = asyncio.run(scenario())
    store.close()

    assert all(r.status_code == 200 for r in resps)
    assert pool.calls == n
    assert elapsed < delay * n / 3  # upstream waits overlapped
    for i, (cid, listing) in enumerate(zip(cids, listings)):
        msgs = listing.json()
        assert [(m["role"], m["content"]) for m in msgs] == [
            ("user", f"m{i}"), ("assistant", f"echo m{i}")
        ]
        assert titles[cid] == f"m{i}"


def test_concurrent_turns_on_one_conversation_are_not_interleaved(tmp_path):
    store = SlowWriteStore(str(tmp_path / "db.sqlite"))
    cid = store.create_conversation()["id"]
    app = make_app(store, EchoPool(delay_s=0.05))
    n = 6

    async def scenario():
        async with client(app) as c:
            resps = await asyncio.gather(*(
                c.post("/v1/chat", json={"conversation_id": cid, "message": f"q{i}"})
                for i in range(n)
            ))
            return resps, (await c.get(f"/v1/conversations/{cid}/messages")).json()

    resps, msgs = asyncio.run(scenario())
    store.close()

    assert all(r.status_code == 200 for r in resps)
    assert len(msgs) == 2 * n
    for user, assistant in zip(msgs[::2], msgs[1::2]):
        assert user["role"] == "user"
        assert assistant["role"] == "assistant"
        assert assistant["content"] == f"echo {user['content']}"


def test_store_is_consistent_under_concurrent_threads(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    writers, per_writer = 8, 40
    errors: list[BaseException] = []
    start = threading.Barrier(writers + 2)

    def writer(i: int):
        try:
            start.wait()
            cid = store.create_conversation(title=f"w{i}")["id"]
            for j in range(per_writer):
                store.add_message(cid, "user", f"{i}-{j}")
                store.update_conversation(cid, pinned=j % 2 == 0)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def reader():
        try:
            start.wait()
            for _ in range(per_writer * 2):
                for convo in store.list_conversations():
                    store.get_conversation_with_messages(convo["id"], limit=5)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(writers)]
    threads += [threading.Thread(target=reader) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    try:
        assert errors == []
        convos = store.list_conversations()
        assert len(convos) == writers
        for convo in convos:
            i = convo["title"][1:]
            assert [m["content"] for m in store.list_messages(convo["id"])] == [
                f"{i}-{j}" for j in range(per_writer)
            ]
    finally:
        store.close()


def test_get_conversation_with_messages(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    assert store.get_conversation_with_messages("missing") == (None, [])
    cid = store.create_conversation()["id"]
    first = store.add_message(cid, "user", "a")
    store.add_message(cid, "assistant", "b")
    convo, msgs = store.get_conversation_with_messages(cid)
    assert convo["id"] == cid
    assert [m["content"] for m in msgs] == ["a", "b"]
    _, latest = store.get_conversation_with_messages(cid, limit=1)
    assert [m["content"] for m in latest] == ["b"]
    _, older = store.get_conversation_with_messages(cid, before=msgs[1]["id"])
    assert [m["id"] for m in older] == [first["id"]]
    store.close()


def test_record_turn_titles_and_persists_both_messages(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    cid = store.create_conversation()["id"]
    assistant = store.record_turn(cid, "hello", "hi", latency_ms=12, title="hello")
    convo, msgs = store.get_conversation_with_messages(cid)
    assert convo["title"] == "hello"
    assert [(m["role"], m["content"], m["latency_ms"]) for m in msgs] == [
        ("user", "hello", None), ("assistant", "hi", 12)
    ]
    assert assistant["id"] == msgs[1]["id"]
    store.record_turn(cid, "again", "ok")
    assert store.get_conversation(cid)["title"] == "hello"
    store.close()
