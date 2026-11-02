"""Run the router unchanged, plus one log line per upstream call.

    uv run uvicorn bench.experiments.instrumented:app --port 8000

Wraps UpstreamPool.forward and RequestLogMiddleware.dispatch from the outside
(no change to app/) so each `upstream ... upstream_ms=` line carries the same
request_id as the middleware's `request ... duration_ms=` line.
"""
import contextvars
import logging
import os
import time

from app.middleware import logging as reqlog
from app import upstream

_rid = contextvars.ContextVar("rid", default="-")
_log = logging.getLogger("llm-router.upstream")

_orig_dispatch = reqlog.RequestLogMiddleware.dispatch
_orig_forward = upstream.UpstreamPool.forward
_orig_init = upstream.UpstreamPool.__init__


async def _dispatch(self, request, call_next):
    async def tagged_call_next(req):
        _rid.set(req.state.request_id)
        return await call_next(req)
    return await _orig_dispatch(self, request, tagged_call_next)


def _init(self, *a, **kw):
    _orig_init(self, *a, **kw)
    urls = kw.get("urls") or (a[0] if a else None) or upstream.upstream_urls()
    self._exp_urls = upstream.cycle(urls)  # mirrors the pool's own cycle


async def _forward(self, payload):
    url = next(self._exp_urls)
    start = time.monotonic()
    try:
        return await _orig_forward(self, payload)
    finally:
        _log.info("upstream url=%s upstream_ms=%.1f request_id=%s",
                  url, (time.monotonic() - start) * 1000, _rid.get())


reqlog.RequestLogMiddleware.dispatch = _dispatch
upstream.UpstreamPool.__init__ = _init
upstream.UpstreamPool.forward = _forward

if os.environ.get("EXP_STORE_LOCK") == "1":
    # Needed once store calls run in worker threads (asyncio.to_thread) while
    # sharing one sqlite connection: serialize Store methods with an RLock.
    import functools
    import threading

    from app import store as _store
    _lock = threading.RLock()

    def _locked(fn):
        @functools.wraps(fn)
        def wrapper(*a, **kw):
            with _lock:
                return fn(*a, **kw)
        return wrapper

    for _name in ("create_conversation", "list_conversations", "get_conversation",
                  "update_conversation", "delete_conversation", "add_message", "list_messages"):
        setattr(_store.Store, _name, _locked(getattr(_store.Store, _name)))

from app.main import app  # noqa: E402,F401
