"""In-memory LRU+TTL response cache with single-flight de-duplication."""
import asyncio
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Hashable

Result = tuple[int, dict]


class ResponseCache:
    """Caches successful (HTTP 200) upstream results by key.

    Concurrent misses for the same key share a single upstream call. Only
    successful results are stored; errors are returned to the waiters of that
    call and the next request retries. `max_entries <= 0` disables caching.
    """

    def __init__(self, max_entries: int, ttl_s: float,
                 clock: Callable[[], float] = time.monotonic):
        self._max_entries = max_entries
        self._ttl_s = ttl_s
        self._clock = clock
        self._entries: OrderedDict[Hashable, tuple[float, Result]] = OrderedDict()
        self._inflight: dict[Hashable, asyncio.Future[Result]] = {}

    def __len__(self) -> int:
        return len(self._entries)

    async def get_or_fetch(self, key: Hashable,
                           fetch: Callable[[], Awaitable[Result]]) -> Result:
        if self._max_entries <= 0:
            return await fetch()

        entry = self._entries.get(key)
        if entry is not None:
            expires_at, result = entry
            if expires_at > self._clock():
                self._entries.move_to_end(key)
                return result
            del self._entries[key]

        inflight = self._inflight.get(key)
        if inflight is None:
            inflight = asyncio.ensure_future(fetch())
            self._inflight[key] = inflight
            inflight.add_done_callback(lambda fut: self._settle(key, fut))
        return await asyncio.shield(inflight)

    def _settle(self, key: Hashable, fut: asyncio.Future[Result]) -> None:
        self._inflight.pop(key, None)
        if fut.cancelled() or fut.exception() is not None:
            return
        result = fut.result()
        if result[0] != 200:
            return
        self._entries[key] = (self._clock() + self._ttl_s, result)
        self._entries.move_to_end(key)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)
