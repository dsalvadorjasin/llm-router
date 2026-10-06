import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


STRATEGIES = ("latency", "round_robin")


def routing_strategy() -> str:
    """`latency` (EWMA x in-flight, power-of-two-choices) or `round_robin`."""
    value = os.environ.get("ROUTER_STRATEGY", "latency").strip().lower().replace("-", "_")
    if value not in STRATEGIES:
        raise ValueError(f"ROUTER_STRATEGY must be one of {STRATEGIES}, got {value!r}")
    return value


def ewma_alpha() -> float:
    """Weight of the newest latency sample in each replica's moving average."""
    return _float("ROUTER_EWMA_ALPHA", 0.2)


def ewma_half_life_s() -> float:
    """Old EWMA history loses half its weight after this long without a sample."""
    return _float("ROUTER_EWMA_HALF_LIFE_S", 2.0)


def explore_rate() -> float:
    """Fraction of requests sent to a uniformly random replica."""
    return _float("ROUTER_EXPLORE_RATE", 0.02)


def probe_interval_s() -> float:
    """A replica that has not been picked for this long gets the next request."""
    return _float("ROUTER_PROBE_INTERVAL_S", 5.0)


def connect_timeout_s() -> float:
    return _float("ROUTER_CONNECT_TIMEOUT_S", 1.0)


def attempt_timeout_s() -> float:
    """Hard cap on a single upstream attempt (connect + response)."""
    return _float("ROUTER_ATTEMPT_TIMEOUT_S", 8.0)


def max_attempts() -> int:
    """Total attempts per request; each retry goes to a replica not yet tried."""
    return _int("ROUTER_MAX_ATTEMPTS", 2)
