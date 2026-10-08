import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def attempt_timeout_ms() -> int:
    """Upper bound (and cold-start value) for a single upstream attempt."""
    return _env_int("LLM_ATTEMPT_TIMEOUT_MS", 200)


def request_budget_ms() -> int:
    """Deadline for the whole request, across all attempts."""
    return _env_int("LLM_REQUEST_BUDGET_MS", 600)


def max_attempts() -> int:
    """Total attempts per request (1 disables retries)."""
    return max(1, _env_int("LLM_MAX_ATTEMPTS", 3))


def adaptive_timeout() -> bool:
    """Shrink the attempt timeout to a multiple of recent successful latencies."""
    return os.environ.get("LLM_ADAPTIVE_TIMEOUT", "1") not in ("0", "false", "no", "")


def adaptive_timeout_quantile() -> float:
    return _env_float("LLM_ADAPTIVE_TIMEOUT_QUANTILE", 0.99)


def adaptive_timeout_multiplier() -> float:
    return _env_float("LLM_ADAPTIVE_TIMEOUT_MULTIPLIER", 1.05)


def min_attempt_timeout_ms() -> int:
    """Floor for the adaptive attempt timeout."""
    return _env_int("LLM_MIN_ATTEMPT_TIMEOUT_MS", 120)


def failure_halflife_ms() -> int:
    """Half-life of the per-upstream failure ratio used to order retries."""
    return _env_int("LLM_FAILURE_HALFLIFE_MS", 10000)


def suspect_failure_ratio() -> float:
    """Failure ratio at which an upstream's attempts get the short suspect timeout."""
    return _env_float("LLM_SUSPECT_FAILURE_RATIO", 0.5)


def suspect_timeout_factor() -> float:
    """Suspect attempt timeout as a fraction of the normal one (1 disables)."""
    return _env_float("LLM_SUSPECT_TIMEOUT_FACTOR", 0.5)


def suspect_probe_every() -> int:
    """Every Nth attempt to a suspect upstream gets the full timeout as a probe."""
    return max(1, _env_int("LLM_SUSPECT_PROBE_EVERY", 10))
