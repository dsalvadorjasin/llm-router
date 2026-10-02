import os

import httpx

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"

_TIMEOUT_DEFAULTS = {
    "connect": 2.0,
    "read": 10.0,
    "write": 5.0,
    "pool": 5.0,
}


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def _timeout_seconds(phase: str) -> float:
    name = f"LLM_UPSTREAM_{phase.upper()}_TIMEOUT"
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return _TIMEOUT_DEFAULTS[phase]
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a number of seconds, got {raw!r}") from None
    if not value > 0 or value == float("inf"):
        raise ValueError(f"{name} must be a positive finite number of seconds, got {raw!r}")
    return value


def upstream_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=_timeout_seconds("connect"),
        read=_timeout_seconds("read"),
        write=_timeout_seconds("write"),
        pool=_timeout_seconds("pool"),
    )
