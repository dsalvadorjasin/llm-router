"""Request-ID tagging and structured request logging."""
import logging
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware

from ..upstream import upstream_log_fields

logger = logging.getLogger("llm-router.requests")


def _fmt(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class RequestLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        request_id = uuid.uuid4().hex[:12]
        request.state.request_id = request_id
        start = time.monotonic()
        fields: dict = {}
        token = upstream_log_fields.set(fields)
        try:
            response = await call_next(request)
        finally:
            upstream_log_fields.reset(token)
        duration_ms = (time.monotonic() - start) * 1000
        logger.info(
            "request method=%s path=%s status=%s duration_ms=%.1f request_id=%s "
            "upstream_url=%s hedged=%s attempts=%s",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
            request_id,
            fields.get("upstream_url", "-"),
            _fmt(fields.get("hedged")),
            fields.get("attempts", "-"),
        )
        response.headers["x-request-id"] = request_id
        return response
