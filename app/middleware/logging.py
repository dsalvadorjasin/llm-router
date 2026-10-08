"""Request-ID tagging and structured request logging."""
import logging
import time
import uuid
from contextvars import ContextVar

from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("llm-router.requests")

# The middleware installs a fresh dict per request before call_next; the endpoint
# runs in a child task whose context copy shares this same dict object, so
# UpstreamPool.forward can record which upstream served the response.
upstream_info: ContextVar[dict | None] = ContextVar("upstream_info", default=None)


class RequestLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        request_id = uuid.uuid4().hex[:12]
        request.state.request_id = request_id
        info: dict = {}
        token = upstream_info.set(info)
        start = time.monotonic()
        try:
            response = await call_next(request)
        finally:
            upstream_info.reset(token)
        duration_ms = (time.monotonic() - start) * 1000
        logger.info(
            "request method=%s path=%s status=%s duration_ms=%.1f request_id=%s"
            " upstream_url=%s hedged=%s",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
            request_id,
            info.get("upstream_url", "-"),
            info.get("hedged", "-"),
        )
        response.headers["x-request-id"] = request_id
        return response
