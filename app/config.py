import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def history_limit() -> int | None:
    """Max prior messages fed into a chat prompt; None (0/empty/unset-to-0) = unlimited."""
    raw = os.environ.get("LLM_HISTORY_LIMIT", "50").strip()
    if not raw:
        return None
    limit = int(raw)
    return limit if limit > 0 else None
