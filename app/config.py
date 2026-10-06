import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


def latency_aware_enabled() -> bool:
    return os.environ.get("ROUTER_LATENCY_AWARE", "1").strip().lower() not in ("0", "false", "no", "off")


def routing_config():
    from .routing import RoutingConfig

    d = RoutingConfig()
    return RoutingConfig(
        policy=os.environ.get("ROUTER_POLICY", d.policy).strip().lower() or d.policy,
        max_attempts=int(_env_float("ROUTER_MAX_ATTEMPTS", d.max_attempts)),
        attempt_timeout_ms=_env_float("ROUTER_ATTEMPT_TIMEOUT_MS", d.attempt_timeout_ms),
        attempt_timeout_min_ms=_env_float("ROUTER_ATTEMPT_TIMEOUT_MIN_MS", d.attempt_timeout_min_ms),
        attempt_timeout_max_ms=_env_float("ROUTER_ATTEMPT_TIMEOUT_MAX_MS", d.attempt_timeout_max_ms),
        timeout_quantile=_env_float("ROUTER_TIMEOUT_QUANTILE", d.timeout_quantile),
        timeout_multiplier=_env_float("ROUTER_TIMEOUT_MULTIPLIER", d.timeout_multiplier),
        final_timeout_ms=_env_float("ROUTER_FINAL_TIMEOUT_MS", d.final_timeout_ms),
        ewma_alpha=_env_float("ROUTER_EWMA_ALPHA", d.ewma_alpha),
        failure_penalty=_env_float("ROUTER_FAILURE_PENALTY", d.failure_penalty),
        decay_half_life_s=_env_float("ROUTER_DECAY_HALF_LIFE_S", d.decay_half_life_s),
        tie_ratio=_env_float("ROUTER_TIE_RATIO", d.tie_ratio),
        tie_abs_ms=_env_float("ROUTER_TIE_ABS_MS", d.tie_abs_ms),
    )
