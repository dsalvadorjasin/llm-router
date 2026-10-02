"""In-process TTL response cache wrapped around an upstream pool.

Successful (HTTP 200) upstream responses are cached by the canonical
``(prompt, max_tokens)`` pair for ``ttl_s`` seconds, bounded to ``max_entries``
with least-recently-used eviction. Concurrent identical misses share a single
upstream call. Error responses and exceptions are never cached.
"""
import asyncio
import copy
import os
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Protocol

DEFAULT_TTL_S = 60.0
DEFAULT_MAX_ENTRIES = 1024

_KEY_FIELDS = frozenset({"prompt", "max_tokens"})

CacheKey = tuple[str, int]
Result = tuple[int, dict]


class Forwarder(Protocol):
    def forward(self, payload: dict) -> Awaitable[Result]: ...

    def aclose(self) -> Awaitable[None]: ...


def cache_ttl_s() -> float:
    return float(os.environ.get("RESPONSE_CACHE_TTL_S", DEFAULT_TTL_S))


def cache_max_entries() -> int:
    return int(os.environ.get("RESPONSE_CACHE_MAX_ENTRIES", DEFAULT_MAX_ENTRIES))


def cache_key(payload: dict) -> CacheKey | None:
    if set(payload) - _KEY_FIELDS:
        return None
    prompt = payload.get("prompt")
    max_tokens = payload.get("max_tokens", 64)
    if not isinstance(prompt, str) or not isinstance(max_tokens, int):
        return None
    return prompt, max_tokens


class _Inflight:
    __slots__ = ("task", "waiters")

    def __init__(self, task: "asyncio.Task[Result]"):
        self.task = task
        self.waiters = 0


class ResponseCache:
    def __init__(self, pool: Forwarder, ttl_s: float = DEFAULT_TTL_S,
                 max_entries: int = DEFAULT_MAX_ENTRIES,
                 clock: Callable[[], float] = time.monotonic):
        self._pool = pool
        self._ttl_s = ttl_s
        self._max_entries = max_entries
        self._clock = clock
        self._entries: OrderedDict[CacheKey, tuple[float, dict]] = OrderedDict()
        self._inflight: dict[CacheKey, _Inflight] = {}

    @property
    def enabled(self) -> bool:
        return self._ttl_s > 0 and self._max_entries > 0

    def __len__(self) -> int:
        return len(self._entries)

    async def forward(self, payload: dict) -> Result:
        key = cache_key(payload) if self.enabled else None
        if key is None:
            return await self._pool.forward(payload)

        cached = self._lookup(key)
        if cached is not None:
            return 200, copy.deepcopy(cached)

        inflight = self._inflight.get(key)
        if inflight is None:
            task = asyncio.ensure_future(self._fetch(key, dict(payload)))
            inflight = self._inflight[key] = _Inflight(task)
            task.add_done_callback(lambda _t, k=key, i=inflight: self._clear_inflight(k, i))

        inflight.waiters += 1
        try:
            status, body = await asyncio.shield(inflight.task)
        finally:
            inflight.waiters -= 1
            if inflight.waiters == 0 and not inflight.task.done():
                self._clear_inflight(key, inflight)
                inflight.task.cancel()
        return status, copy.deepcopy(body)

    async def aclose(self) -> None:
        for inflight in list(self._inflight.values()):
            inflight.task.cancel()
        self._inflight.clear()
        self._entries.clear()
        await self._pool.aclose()

    async def _fetch(self, key: CacheKey, payload: dict) -> Result:
        status, body = await self._pool.forward(payload)
        if status == 200:
            self._store(key, body)
        return status, body

    def _lookup(self, key: CacheKey) -> dict | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        expires_at, body = entry
        if self._clock() >= expires_at:
            del self._entries[key]
            return None
        self._entries.move_to_end(key)
        return body

    def _store(self, key: CacheKey, body: dict) -> None:
        self._entries[key] = (self._clock() + self._ttl_s, copy.deepcopy(body))
        self._entries.move_to_end(key)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    def _clear_inflight(self, key: CacheKey, inflight: _Inflight) -> None:
        if self._inflight.get(key) is inflight:
            del self._inflight[key]
