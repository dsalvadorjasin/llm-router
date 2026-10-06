import os
from dataclasses import dataclass

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return default if raw is None or raw.strip() == "" else float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return default if raw is None or raw.strip() == "" else int(raw)


STRATEGIES = ("latency", "round_robin")


def routing_strategy() -> str:
    """`latency` (EWMA x in-flight, power-of-two-choices) or `round_robin`."""
    value = os.environ.get("ROUTER_STRATEGY", "latency").strip().lower().replace("-", "_")
    if value not in STRATEGIES:
        raise ValueError(f"ROUTER_STRATEGY must be one of {STRATEGIES}, got {value!r}")
    return value


def ewma_alpha() -> float:
    """Weight of the newest latency sample in each replica's moving average."""
    return _env_float("ROUTER_EWMA_ALPHA", 0.2)


def ewma_half_life_s() -> float:
    """Old EWMA history loses half its weight after this long without a sample."""
    return _env_float("ROUTER_EWMA_HALF_LIFE_S", 2.0)


def explore_rate() -> float:
    """Fraction of requests sent to a uniformly random replica."""
    return _env_float("ROUTER_EXPLORE_RATE", 0.02)


def probe_interval_s() -> float:
    """A replica that has not been picked for this long gets the next request."""
    return _env_float("ROUTER_PROBE_INTERVAL_S", 5.0)


def connect_timeout_s() -> float:
    """Upstream connect timeout."""
    return _env_float("ROUTER_CONNECT_TIMEOUT_S", 1.0)


def attempt_timeout_s() -> float:
    """Hard cap on a single upstream attempt (connect + response)."""
    return _env_float("ROUTER_ATTEMPT_TIMEOUT_S", 8.0)


def max_attempts() -> int:
    """Total attempts per request in non-hedged mode."""
    return max(1, _env_int("ROUTER_MAX_ATTEMPTS", 2))


@dataclass(frozen=True)
class HedgeSettings:
    enabled: bool = True
    delay_ms: float = 400.0
    adaptive: bool = True
    percentile: float = 0.5
    min_delay_ms: float = 50.0
    max_delay_ms: float = 1000.0
    min_samples: int = 20
    window: int = 512
    max_attempts: int = 3
    signature_guard: bool = True
    signature_memo_size: int = 10_000


def hedge_settings() -> HedgeSettings:
    d = HedgeSettings()
    return HedgeSettings(
        enabled=_env_bool("HEDGE_ENABLED", d.enabled),
        delay_ms=_env_float("HEDGE_DELAY_MS", d.delay_ms),
        adaptive=_env_bool("HEDGE_ADAPTIVE", d.adaptive),
        percentile=_env_float("HEDGE_PERCENTILE", d.percentile),
        min_delay_ms=_env_float("HEDGE_MIN_DELAY_MS", d.min_delay_ms),
        max_delay_ms=_env_float("HEDGE_MAX_DELAY_MS", d.max_delay_ms),
        min_samples=_env_int("HEDGE_MIN_SAMPLES", d.min_samples),
        window=_env_int("HEDGE_WINDOW", d.window),
        max_attempts=max(1, _env_int("HEDGE_MAX_ATTEMPTS", d.max_attempts)),
        signature_guard=_env_bool("HEDGE_SIGNATURE_GUARD", d.signature_guard),
        signature_memo_size=_env_int("HEDGE_SIGNATURE_MEMO_SIZE", d.signature_memo_size),
    )


def response_cache_enabled() -> bool:
    return _env_bool("RESPONSE_CACHE_ENABLED", True)


def response_cache_ttl_s() -> float:
    return _env_float("RESPONSE_CACHE_TTL_S", 300.0)


def response_cache_max_entries() -> int:
    return _env_int("RESPONSE_CACHE_MAX_ENTRIES", 1024)


def response_cache_coalesce() -> bool:
    return response_cache_enabled() and _env_bool("RESPONSE_CACHE_COALESCE", True)
