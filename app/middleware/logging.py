"""Request-ID tagging and structured request logging."""
import logging
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware

from ..request_context import request_log_fields

logger = logging.getLogger("llm-router.requests")


def _fmt_bool(value) -> str:
    return "-" if value is None else str(bool(value)).lower()


class RequestLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        request_id = uuid.uuid4().hex[:12]
        request.state.request_id = request_id
        fields: dict = {}
        ctx_token = request_log_fields.set(fields)
        start = time.monotonic()
        try:
            response = await call_next(request)
        finally:
            request_log_fields.reset(ctx_token)
        duration_ms = (time.monotonic() - start) * 1000
        logger.info(
            "request method=%s path=%s status=%s duration_ms=%.1f request_id=%s "
            "upstream_url=%s hedged=%s",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
            request_id,
            fields.get("upstream_url", "-"),
            _fmt_bool(fields.get("hedged")),
        )
        response.headers["x-request-id"] = request_id
        return response
