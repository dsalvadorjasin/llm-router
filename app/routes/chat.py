import asyncio
import time

from fastapi import APIRouter, HTTPException, Request

from ..prompting import flatten_history
from ..schemas import ChatRequest, ChatTurnResponse

router = APIRouter(tags=["chat"])

_AUTO_TITLE_LIMIT = 48


@router.post("/v1/chat", response_model=ChatTurnResponse)
async def chat(body: ChatRequest, request: Request):
    store = request.app.state.store
    conversation = await asyncio.to_thread(store.get_conversation, body.conversation_id)
    if conversation is None:
        raise HTTPException(status_code=404, detail="conversation not found")

    history = await asyncio.to_thread(store.list_messages, body.conversation_id)
    prompt = flatten_history(history, body.message)

    start = time.monotonic()
    status, upstream = await request.app.state.pool.forward(
        {"prompt": prompt, "max_tokens": body.max_tokens}
    )
    latency_ms = int((time.monotonic() - start) * 1000)

    if status != 200:
        raise HTTPException(status_code=status,
                            detail=upstream.get("detail", "upstream error"))

    def persist_turn() -> dict:
        # One thread call, so cancelling the request can't leave a user
        # message without its assistant reply.
        if not history and conversation["title"] == "New conversation":
            store.update_conversation(body.conversation_id,
                                      title=body.message[:_AUTO_TITLE_LIMIT])
        store.add_message(body.conversation_id, "user", body.message)
        return store.add_message(
            body.conversation_id, "assistant", upstream["completion"], latency_ms=latency_ms
        )

    assistant = await asyncio.to_thread(persist_turn)
    return {
        "message": assistant,
        "latency_ms": latency_ms,
        "model": upstream.get("model"),
        "usage": upstream.get("usage"),
    }
