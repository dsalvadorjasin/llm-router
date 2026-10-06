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
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return default if raw is None or raw.strip() == "" else float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return default if raw is None or raw.strip() == "" else int(raw)


@dataclass(frozen=True)
class HedgeSettings:
    enabled: bool = True
    # Initial / fixed hedge delay; used until enough latency samples exist
    # (or always, when adaptive is off).
    delay_ms: float = 400.0
    adaptive: bool = True
    percentile: float = 0.8
    min_delay_ms: float = 50.0
    max_delay_ms: float = 1000.0
    min_samples: int = 20
    window: int = 512
    max_attempts: int = 3
    connect_timeout_s: float = 2.0
    attempt_timeout_s: float = 10.0
    ewma_alpha: float = 0.2
    # An idle replica's latency estimate relaxes toward the pool median with
    # this half-life, so a replica that was slow is retried later.
    idle_decay_half_life_s: float = 10.0
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
        connect_timeout_s=_env_float("UPSTREAM_CONNECT_TIMEOUT_S", d.connect_timeout_s),
        attempt_timeout_s=_env_float("UPSTREAM_ATTEMPT_TIMEOUT_S", d.attempt_timeout_s),
        ewma_alpha=_env_float("HEDGE_EWMA_ALPHA", d.ewma_alpha),
        idle_decay_half_life_s=_env_float("HEDGE_IDLE_DECAY_HALF_LIFE_S", d.idle_decay_half_life_s),
        signature_guard=_env_bool("HEDGE_SIGNATURE_GUARD", d.signature_guard),
        signature_memo_size=_env_int("HEDGE_SIGNATURE_MEMO_SIZE", d.signature_memo_size),
    )
