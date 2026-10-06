import os
from dataclasses import dataclass

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"

STRATEGIES = ("latency", "round_robin")


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


@dataclass(frozen=True)
class RoutingSettings:
    strategy: str = "latency"
    ewma_alpha: float = 0.3
    probe_interval_s: float = 5.0
    error_penalty_s: float = 5.0
    outstanding_weight: float = 1.0


def routing_settings() -> RoutingSettings:
    strategy = os.environ.get("ROUTER_STRATEGY", RoutingSettings.strategy).strip().lower()
    if strategy not in STRATEGIES:
        raise ValueError(f"ROUTER_STRATEGY must be one of {STRATEGIES}, got {strategy!r}")
    return RoutingSettings(
        strategy=strategy,
        ewma_alpha=float(os.environ.get("ROUTER_EWMA_ALPHA", RoutingSettings.ewma_alpha)),
        probe_interval_s=float(
            os.environ.get("ROUTER_PROBE_INTERVAL_S", RoutingSettings.probe_interval_s)),
        error_penalty_s=float(
            os.environ.get("ROUTER_ERROR_PENALTY_S", RoutingSettings.error_penalty_s)),
        outstanding_weight=float(
            os.environ.get("ROUTER_OUTSTANDING_WEIGHT", RoutingSettings.outstanding_weight)),
    )
