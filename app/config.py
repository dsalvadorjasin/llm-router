import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def routing_mode() -> str:
    return os.environ.get("LLM_ROUTING", "latency").strip().lower()


def ewma_alpha() -> float:
    return float(os.environ.get("LLM_EWMA_ALPHA", "0.3"))


def failure_penalty_ms() -> float:
    return float(os.environ.get("LLM_FAILURE_PENALTY_MS", "1000"))


def failure_penalty_halflife_ms() -> float:
    return float(os.environ.get("LLM_FAILURE_PENALTY_HALFLIFE_MS", "5000"))


def probe_ratio() -> float:
    return float(os.environ.get("LLM_PROBE_RATIO", "0.02"))


def max_attempts() -> int:
    return int(os.environ.get("LLM_MAX_ATTEMPTS", "3"))


def attempt_timeout_ms() -> float:
    return float(os.environ.get("LLM_ATTEMPT_TIMEOUT_MS", "10000"))
