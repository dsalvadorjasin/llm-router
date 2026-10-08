import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def hedging_enabled() -> bool:
    return os.environ.get("LLM_HEDGING", "1").strip().lower() not in ("0", "false", "no", "off")


def hedge_delay_ms() -> float:
    return float(os.environ.get("LLM_HEDGE_DELAY_MS", "225"))


def attempt_timeout_ms() -> float:
    return float(os.environ.get("LLM_ATTEMPT_TIMEOUT_MS", "5000"))


def max_attempts() -> int:
    return max(1, int(os.environ.get("LLM_MAX_ATTEMPTS", "3")))
