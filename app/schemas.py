from pydantic import BaseModel, model_validator


class GenerateRequest(BaseModel):
    prompt: str
    max_tokens: int = 64


class ChatRequest(BaseModel):
    conversation_id: str
    message: str
    max_tokens: int = 64
    model: str | None = None


class ConversationCreate(BaseModel):
    title: str | None = None


class ConversationUpdate(BaseModel):
    """PATCH body for a conversation: rename it, pin/unpin it, or both."""

    title: str | None = None
    pinned: bool | None = None

    @model_validator(mode="after")
    def _require_at_least_one_field(self) -> "ConversationUpdate":
        if self.title is None and self.pinned is None:
            raise ValueError("at least one of title or pinned must be provided")
        return self


# -- response models ---------------------------------------------------------
# These describe the shape of what the store already returns; wiring them in
# as `response_model=` gets us request-time validation plus accurate OpenAPI
# docs for the conversation/chat/info surface without changing any payloads.


class ConversationOut(BaseModel):
    id: str
    title: str
    created_at: float
    updated_at: float
    pinned: bool


class MessageOut(BaseModel):
    id: str
    conversation_id: str
    role: str
    content: str
    latency_ms: int | None = None
    created_at: float


class ChatTurnResponse(BaseModel):
    message: MessageOut
    latency_ms: int
    model: str | None = None
    # `usage` is passed through opaquely: it comes straight from the upstream
    # backend, whose exact field set this service doesn't own, so it isn't
    # worth (or safe) pinning to a strict shape here.
    usage: dict | None = None
    upstream: str | None = None


class InfoResponse(BaseModel):
    name: str
    version: str
    models: list[str]
    uptime_s: float
