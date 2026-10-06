import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


_FALSY = {"0", "false", "no", "off"}


def hedging_enabled() -> bool:
    return os.environ.get("LLM_HEDGING", "1").strip().lower() not in _FALSY


def hedge_settings() -> dict:
    e = os.environ
    delay = e.get("LLM_HEDGE_DELAY_MS", "auto").strip().lower()
    return {
        "fixed_delay_ms": None if delay in ("", "auto") else float(delay),
        "initial_delay_ms": float(e.get("LLM_HEDGE_INITIAL_DELAY_MS", 200)),
        "min_delay_ms": float(e.get("LLM_HEDGE_MIN_DELAY_MS", 50)),
        "max_delay_ms": float(e.get("LLM_HEDGE_MAX_DELAY_MS", 400)),
        "delay_quantile": float(e.get("LLM_HEDGE_QUANTILE", 0.5)),
        "max_hedged_attempts": int(e.get("LLM_HEDGE_MAX_ATTEMPTS", 3)),
        "attempt_timeout_s": float(e.get("LLM_ATTEMPT_TIMEOUT_S", 5)),
        "slow_factor": float(e.get("LLM_HEDGE_SLOW_FACTOR", 2.0)),
        "probe_interval_s": float(e.get("LLM_HEDGE_PROBE_INTERVAL_S", 2.0)),
    }
