import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _env_flag(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() not in ("0", "false", "no", "off", "")


# --- Strategy D: chat request shaping -------------------------------------

def history_limit() -> int | None:
    """Max prior messages fed into a chat prompt; None (0/empty/unset-to-0) = unlimited."""
    raw = os.environ.get("LLM_HISTORY_LIMIT", "50").strip()
    if not raw:
        return None
    limit = int(raw)
    return limit if limit > 0 else None


# --- Strategy A: per-attempt timeout, request budget, retries -------------

def attempt_timeout_ms() -> float:
    """Upper bound (and cold-start value) for a single upstream attempt."""
    return _env_float("LLM_ATTEMPT_TIMEOUT_MS", 200)


def request_budget_ms() -> float:
    """Deadline for the whole request, across all attempts (hedges included)."""
    return _env_float("LLM_REQUEST_BUDGET_MS", 600)


def max_attempts() -> int:
    """Total attempts per request: first + hedges + failovers (1 disables both)."""
    return max(1, _env_int("LLM_MAX_ATTEMPTS", 3))


def adaptive_timeout() -> bool:
    """Shrink the attempt timeout to a multiple of recent successful latencies."""
    return _env_flag("LLM_ADAPTIVE_TIMEOUT", "1")


def adaptive_timeout_quantile() -> float:
    return _env_float("LLM_ADAPTIVE_TIMEOUT_QUANTILE", 0.99)


def adaptive_timeout_multiplier() -> float:
    return _env_float("LLM_ADAPTIVE_TIMEOUT_MULTIPLIER", 1.05)


def min_attempt_timeout_ms() -> float:
    """Floor for the adaptive attempt timeout."""
    return _env_float("LLM_MIN_ATTEMPT_TIMEOUT_MS", 120)


def failure_halflife_ms() -> float:
    """Half-life of the per-upstream failure ratio (suspect timeouts, round-robin retry order)."""
    return _env_float("LLM_FAILURE_HALFLIFE_MS", 10000)


def suspect_failure_ratio() -> float:
    """Failure ratio at which an upstream's attempts get the short suspect timeout."""
    return _env_float("LLM_SUSPECT_FAILURE_RATIO", 0.5)


def suspect_timeout_factor() -> float:
    """Suspect attempt timeout as a fraction of the normal one (1 disables)."""
    return _env_float("LLM_SUSPECT_TIMEOUT_FACTOR", 0.5)


def suspect_probe_every() -> int:
    """Every Nth attempt to a suspect upstream gets the full timeout as a probe."""
    return max(1, _env_int("LLM_SUSPECT_PROBE_EVERY", 10))


# --- Strategy C: hedging ---------------------------------------------------

def hedging_enabled() -> bool:
    return _env_flag("LLM_HEDGING", "1")


def hedge_delay_ms() -> float:
    """Interval after which another duplicate is fired while nothing has completed."""
    return _env_float("LLM_HEDGE_DELAY_MS", 150)


# --- Strategy B: latency-aware routing -------------------------------------

def routing_mode() -> str:
    """'latency' (default) or 'roundrobin' (kill switch)."""
    return os.environ.get("LLM_ROUTING", "latency").strip().lower()


def ewma_alpha() -> float:
    return _env_float("LLM_EWMA_ALPHA", 0.3)


def failure_penalty_ms() -> float:
    """Score penalty per failed attempt (5xx, transport error, invalid body)."""
    return _env_float("LLM_FAILURE_PENALTY_MS", 1000)


def timeout_penalty_ms() -> float:
    """Score penalty per per-attempt timeout (0: a timeout is only a latency lower bound)."""
    return _env_float("LLM_TIMEOUT_PENALTY_MS", 0)


def failure_penalty_halflife_ms() -> float:
    return _env_float("LLM_FAILURE_PENALTY_HALFLIFE_MS", 5000)


def probe_ratio() -> float:
    """Share of requests that also send a background probe to the least-recently-successful upstream."""
    return _env_float("LLM_PROBE_RATIO", 0.02)


def probe_timeout_ms() -> float:
    """Timeout for background probe attempts (a probe timeout counts as a failure)."""
    return _env_float("LLM_PROBE_TIMEOUT_MS", 5000)
