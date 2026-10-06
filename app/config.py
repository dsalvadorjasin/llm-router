import os
from dataclasses import dataclass

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"

AFFINITY_MODES = ("off", "replica")
MAX_HEDGE_ATTEMPTS = 8


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


@dataclass(frozen=True)
class HedgeConfig:
    """Hedged-request settings for `UpstreamPool`.

    `delays_ms` are offsets from the start of a request at which another
    duplicate attempt is launched if no valid response has arrived yet, so
    the attempt budget is `len(delays_ms) + 1`.
    """

    enabled: bool = True
    delays_ms: tuple[float, ...] = (180.0, 250.0, 350.0)
    attempt_timeout_s: float = 10.0
    deadline_s: float = 15.0
    explore: float = 0.05
    ewma_alpha: float = 0.2
    affinity: str = "off"
    affinity_max_keys: int = 10_000

    def __post_init__(self):
        if len(self.delays_ms) + 1 > MAX_HEDGE_ATTEMPTS:
            raise ValueError(f"at most {MAX_HEDGE_ATTEMPTS} attempts per request")
        if any(d < 0 for d in self.delays_ms) or list(self.delays_ms) != sorted(self.delays_ms):
            raise ValueError("hedge delays must be non-negative and non-decreasing")
        if not (0 < self.attempt_timeout_s and 0 < self.deadline_s):
            raise ValueError("timeouts must be positive and finite")
        if self.attempt_timeout_s == float("inf") or self.deadline_s == float("inf"):
            raise ValueError("timeouts must be finite")
        if not 0 <= self.explore <= 1:
            raise ValueError("explore must be within [0, 1]")
        if not 0 < self.ewma_alpha <= 1:
            raise ValueError("ewma_alpha must be within (0, 1]")
        if self.affinity not in AFFINITY_MODES:
            raise ValueError(f"affinity must be one of {AFFINITY_MODES}")
        if self.affinity_max_keys < 1:
            raise ValueError("affinity_max_keys must be >= 1")

    @property
    def max_attempts(self) -> int:
        return len(self.delays_ms) + 1


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


def hedge_config() -> HedgeConfig:
    d = HedgeConfig()
    raw_delays = os.environ.get("LLM_HEDGE_DELAYS_MS")
    delays = (
        tuple(float(x) for x in raw_delays.split(",") if x.strip())
        if raw_delays is not None else d.delays_ms
    )
    return HedgeConfig(
        enabled=_env_bool("LLM_HEDGE_ENABLED", d.enabled),
        delays_ms=delays,
        attempt_timeout_s=float(os.environ.get("LLM_HEDGE_ATTEMPT_TIMEOUT_S", d.attempt_timeout_s)),
        deadline_s=float(os.environ.get("LLM_HEDGE_DEADLINE_S", d.deadline_s)),
        explore=float(os.environ.get("LLM_HEDGE_EXPLORE", d.explore)),
        ewma_alpha=float(os.environ.get("LLM_HEDGE_EWMA_ALPHA", d.ewma_alpha)),
        affinity=os.environ.get("LLM_HEDGE_AFFINITY", d.affinity).strip().lower(),
        affinity_max_keys=int(os.environ.get("LLM_HEDGE_AFFINITY_MAX_KEYS", d.affinity_max_keys)),
    )
