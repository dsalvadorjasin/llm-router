"""Per-request mutable log fields shared between the logging middleware and UpstreamPool.

BaseHTTPMiddleware runs the endpoint in a child task, so values *set* on a
ContextVar there never reach the middleware. Instead the middleware stores a
fresh dict before `call_next` and the endpoint mutates that same dict object.
"""
from contextvars import ContextVar

request_log_fields: ContextVar[dict | None] = ContextVar("request_log_fields", default=None)


def set_log_field(key: str, value) -> None:
    fields = request_log_fields.get()
    if fields is not None:
        fields[key] = value
