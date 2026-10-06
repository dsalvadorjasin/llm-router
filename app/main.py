import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .cache import ResponseCache
from .config import cache_coalesce, cache_enabled, cache_max_entries, cache_ttl_s
from .middleware.logging import RequestLogMiddleware
from .routes.chat import router as chat_router
from .routes.conversations import router as conversations_router
from .routes.info import router as info_router
from .schemas import GenerateRequest
from .store import Store
from .upstream import UpstreamPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = UpstreamPool()
    app.state.cache = (
        ResponseCache(ttl_s=cache_ttl_s(), max_entries=cache_max_entries(),
                      coalesce=cache_coalesce())
        if cache_enabled() else None
    )
    app.state.store = Store(os.environ.get("APP_DB_PATH", "data/app.db"))
    yield
    await app.state.pool.aclose()
    app.state.store.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(RequestLogMiddleware)


@app.post("/v1/generate")
async def generate(req: GenerateRequest):
    payload = req.model_dump()
    pool = app.state.pool
    cache: ResponseCache | None = getattr(app.state, "cache", None)
    if cache is None:
        status, body = await pool.forward(payload)
        return JSONResponse(status_code=status, content=body)
    (status, body), outcome = await cache.get_or_fetch(
        ResponseCache.key(req.prompt, req.max_tokens), lambda: pool.forward(payload)
    )
    return JSONResponse(status_code=status, content=body, headers={"X-Cache": outcome.upper()})


app.include_router(conversations_router)
app.include_router(chat_router)
app.include_router(info_router)

_DIST = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")
if os.path.isdir(_DIST):
    app.mount("/", StaticFiles(directory=_DIST, html=True), name="ui")
