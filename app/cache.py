"""Response cache with single-flight coalescing for the /v1/generate path.

Successful (200, non-empty completion) upstream responses are cached keyed on
(prompt, max_tokens) with a TTL and LRU eviction. Concurrent identical requests
share one in-flight upstream call. Misses retry on the next replica (the pool's
round-robin advances on every call) when an attempt fails, so a single bad
replica response doesn't surface as a router error.
"""
import asyncio
import logging
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Hashable

import httpx

log = logging.getLogger("llm-router.cache")

Result = tuple[int, dict]


def is_cacheable(status: int, body: object) -> bool:
    return (
        status == 200
        and isinstance(body, dict)
        and isinstance(body.get("completion"), str)
        and bool(body["completion"])
    )


class ResponseCache:
    def __init__(self, ttl_s: float, max_entries: int,
                 clock: Callable[[], float] = time.monotonic):
        self._ttl_s = ttl_s
        self._max_entries = max_entries
        self._clock = clock
        self._entries: OrderedDict[Hashable, tuple[float, Result]] = OrderedDict()
        self._inflight: dict[Hashable, asyncio.Future[Result]] = {}
        self.hits = 0
        self.misses = 0
        self.coalesced = 0

    def __len__(self) -> int:
        return len(self._entries)

    def get(self, key: Hashable) -> Result | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        expires_at, result = entry
        if self._clock() >= expires_at:
            del self._entries[key]
            return None
        self._entries.move_to_end(key)
        return result

    def put(self, key: Hashable, result: Result) -> None:
        if self._max_entries <= 0 or self._ttl_s <= 0:
            return
        self._entries[key] = (self._clock() + self._ttl_s, result)
        self._entries.move_to_end(key)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    async def get_or_fetch(self, key: Hashable,
                           fetch: Callable[[], Awaitable[Result]]) -> Result:
        cached = self.get(key)
        if cached is not None:
            self.hits += 1
            return cached

        inflight = self._inflight.get(key)
        if inflight is not None:
            self.coalesced += 1
            return await asyncio.shield(inflight)

        self.misses += 1
        task = asyncio.ensure_future(self._fetch_and_store(key, fetch))
        self._inflight[key] = task
        task.add_done_callback(lambda _t: self._inflight.pop(key, None))
        # shield: a disconnecting leader must not cancel the call followers wait on
        return await asyncio.shield(task)

    async def _fetch_and_store(self, key: Hashable,
                               fetch: Callable[[], Awaitable[Result]]) -> Result:
        status, body = await fetch()
        if is_cacheable(status, body):
            self.put(key, (status, body))
        return status, body


async def forward_with_retry(pool, payload: dict, attempts: int) -> Result:
    """Call pool.forward up to `attempts` times until a cacheable 200 comes back."""
    last: Result = (502, {"detail": "upstream unavailable"})
    for _ in range(max(1, attempts)):
        try:
            status, body = await pool.forward(payload)
        except (httpx.HTTPError, ValueError) as exc:
            log.warning("upstream attempt failed: %r", exc)
            continue
        if is_cacheable(status, body):
            return status, body
        last = (status, body)
    return last


def cache_key(payload: dict) -> Hashable:
    return (payload.get("prompt"), payload.get("max_tokens"))
