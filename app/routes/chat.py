import asyncio
import time

from fastapi import APIRouter, HTTPException, Request

from ..config import history_limit
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

    history = await asyncio.to_thread(
        store.list_messages, body.conversation_id, limit=history_limit()
    )
    prompt = flatten_history(history, body.message)

    start = time.monotonic()
    status, upstream = await request.app.state.pool.forward(
        {"prompt": prompt, "max_tokens": body.max_tokens}
    )
    latency_ms = int((time.monotonic() - start) * 1000)

    if status != 200:
        raise HTTPException(status_code=status,
                            detail=upstream.get("detail", "upstream error"))

    title = None
    if not history and conversation["title"] == "New conversation":
        title = body.message[:_AUTO_TITLE_LIMIT]

    assistant = await asyncio.to_thread(
        store.add_turn, body.conversation_id, body.message, upstream["completion"],
        latency_ms=latency_ms, title=title,
    )
    return {
        "message": assistant,
        "latency_ms": latency_ms,
        "model": upstream.get("model"),
        "usage": upstream.get("usage"),
    }
