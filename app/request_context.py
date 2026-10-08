"""Per-request mutable log fields shared between the logging middleware and the upstream pool."""
from contextvars import ContextVar

request_log_fields: ContextVar[dict | None] = ContextVar("request_log_fields", default=None)
