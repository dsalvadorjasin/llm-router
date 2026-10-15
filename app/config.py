import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def hedging_enabled() -> bool:
    return os.environ.get("LLM_HEDGING", "1").strip().lower() not in ("0", "false", "off", "no")


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


def hedge_delay_cap_s() -> float:
    return _float_env("LLM_HEDGE_DELAY_MS", 150.0) / 1000


def hedge_max_attempts() -> int:
    return max(1, int(_float_env("LLM_HEDGE_MAX_ATTEMPTS", 3)))


def request_timeout_s() -> float:
    return _float_env("LLM_REQUEST_TIMEOUT_S", 30.0)
