import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"

_FALSY = {"0", "false", "no", "off"}


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() not in _FALSY


def cache_enabled() -> bool:
    return _env_bool("RESPONSE_CACHE_ENABLED", True)


def cache_ttl_s() -> float:
    return float(os.environ.get("RESPONSE_CACHE_TTL_S", "300"))


def cache_max_entries() -> int:
    return int(os.environ.get("RESPONSE_CACHE_MAX_ENTRIES", "1024"))


def cache_coalesce() -> bool:
    return _env_bool("RESPONSE_CACHE_COALESCE", True)
