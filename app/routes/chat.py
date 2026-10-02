import time

from fastapi import APIRouter, HTTPException, Request

from ..prompting import flatten_history
from ..schemas import ChatRequest, ChatTurnResponse

router = APIRouter(tags=["chat"])

_AUTO_TITLE_LIMIT = 48


@router.post("/v1/chat", response_model=ChatTurnResponse)
async def chat(body: ChatRequest, request: Request):
    store = request.app.state.store
    conversation = store.get_conversation(body.conversation_id)
    if conversation is None:
        raise HTTPException(status_code=404, detail="conversation not found")

    history = store.list_messages(body.conversation_id)
    prompt = flatten_history(history, body.message)

    start = time.monotonic()
    status, upstream, upstream_url = await request.app.state.pool.forward(
        {"prompt": prompt, "max_tokens": body.max_tokens},
        request_id=request.state.request_id,
    )
    latency_ms = int((time.monotonic() - start) * 1000)

    if status != 200:
        raise HTTPException(status_code=status,
                            detail=upstream.get("detail", "upstream error"))

    if not history and conversation["title"] == "New conversation":
        store.update_conversation(body.conversation_id, title=body.message[:_AUTO_TITLE_LIMIT])

    store.add_message(body.conversation_id, "user", body.message)
    assistant = store.add_message(
        body.conversation_id, "assistant", upstream["completion"], latency_ms=latency_ms
    )
    return {
        "message": assistant,
        "latency_ms": latency_ms,
        "model": upstream.get("model"),
        "usage": upstream.get("usage"),
        "upstream": upstream_url,
    }
