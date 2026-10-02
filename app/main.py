import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .cache import ResponseCache, cache_max_entries, cache_ttl_s
from .config import upstream_hedge_budget, upstream_hedge_delay
from .middleware.logging import RequestLogMiddleware
from .routes.chat import router as chat_router
from .routes.conversations import router as conversations_router
from .routes.info import router as info_router
from .schemas import GenerateRequest
from .store import Store
from .upstream import HedgeBudget, UpstreamPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    ratio, burst = upstream_hedge_budget()
    app.state.upstream = UpstreamPool(hedge_delay_s=upstream_hedge_delay(),
                                      hedge_budget=HedgeBudget(ratio=ratio, burst=burst))
    app.state.pool = ResponseCache(
        app.state.upstream, ttl_s=cache_ttl_s(), max_entries=cache_max_entries()
    )
    app.state.store = Store(os.environ.get("APP_DB_PATH", "data/app.db"))
    yield
    await app.state.pool.aclose()
    app.state.store.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(RequestLogMiddleware)


@app.post("/v1/generate")
async def generate(req: GenerateRequest):
    status, body = await app.state.pool.forward(req.model_dump())
    return JSONResponse(status_code=status, content=body)


@app.get("/v1/upstream/stats")
async def upstream_stats():
    return app.state.upstream.stats()


app.include_router(conversations_router)
app.include_router(chat_router)
app.include_router(info_router)

_DIST = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")
if os.path.isdir(_DIST):
    app.mount("/", StaticFiles(directory=_DIST, html=True), name="ui")
