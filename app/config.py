import os

_DEFAULT = "http://localhost:9001,http://localhost:9002,http://localhost:9003"


def upstream_urls() -> list[str]:
    raw = os.environ.get("LLM_SERVICE_URLS", _DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "")


def response_cache_enabled() -> bool:
    return _env_bool("ROUTER_RESPONSE_CACHE", True)


def response_cache_ttl_s() -> float:
    return float(os.environ.get("ROUTER_RESPONSE_CACHE_TTL_S", "300"))


def response_cache_max_entries() -> int:
    return int(os.environ.get("ROUTER_RESPONSE_CACHE_MAX_ENTRIES", "4096"))
