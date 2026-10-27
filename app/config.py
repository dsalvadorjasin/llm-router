import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def upstream_timeout_s() -> float:
    return float(os.environ.get("LLM_UPSTREAM_TIMEOUT_S", "10"))


def upstream_cooldown_s() -> float:
    return float(os.environ.get("LLM_UPSTREAM_COOLDOWN_S", "30"))
