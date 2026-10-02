import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def hedge_delay_s() -> float:
    return float(os.environ.get("HEDGE_DELAY_MS", "250")) / 1000


def upstream_max_attempts() -> int:
    return int(os.environ.get("UPSTREAM_MAX_ATTEMPTS", "3"))


def generate_cache_max_entries() -> int:
    return int(os.environ.get("GENERATE_CACHE_MAX_ENTRIES", "1024"))


def generate_cache_ttl_s() -> float:
    return float(os.environ.get("GENERATE_CACHE_TTL_S", "300"))
