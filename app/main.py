import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .cache import ResponseCache, cache_key, forward_with_retry
from .config import (response_cache_enabled, response_cache_max_entries,
                     response_cache_ttl_s, upstream_urls)
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
    app.state.response_cache = (
        ResponseCache(response_cache_ttl_s(), response_cache_max_entries())
        if response_cache_enabled() else None
    )
    app.state.store = Store(os.environ.get("APP_DB_PATH", "data/app.db"))
    yield
    await app.state.pool.aclose()
    app.state.store.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(RequestLogMiddleware)


def _miss_attempts(pool) -> int:
    # a pool that already hedges/fails over across replicas needs no outer retry loop
    return 1 if getattr(pool, "fails_over", False) else len(upstream_urls())


@app.post("/v1/generate")
async def generate(req: GenerateRequest):
    payload = req.model_dump()
    cache = getattr(app.state, "response_cache", None)
    if cache is None:
        status, body = await app.state.pool.forward(payload)
    else:
        status, body = await cache.get_or_fetch(
            cache_key(payload),
            lambda: forward_with_retry(app.state.pool, payload, _miss_attempts(app.state.pool)),
        )
    return JSONResponse(status_code=status, content=body)


app.include_router(conversations_router)
app.include_router(chat_router)
app.include_router(info_router)

_DIST = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")
if os.path.isdir(_DIST):
    app.mount("/", StaticFiles(directory=_DIST, html=True), name="ui")
