"""In-process cache for successful upstream completions.

Entries are keyed on ``(prompt, max_tokens)``, expire after a TTL and are
evicted least-recently-used once ``max_entries`` is reached. Concurrent misses
for the same key are coalesced onto a single upstream call. Only 200 responses
are stored; errors are returned to the waiting callers but never cached.
"""
import asyncio
import copy
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Literal

Result = tuple[int, dict]
Outcome = Literal["hit", "miss", "coalesced", "bypass"]


class ResponseCache:
    def __init__(self, ttl_s: float, max_entries: int, coalesce: bool = True,
                 clock: Callable[[], float] = time.monotonic):
        if ttl_s <= 0:
            raise ValueError("ttl_s must be positive")
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self.ttl_s = ttl_s
        self.max_entries = max_entries
        self.coalesce = coalesce
        self._clock = clock
        self._entries: OrderedDict[tuple, tuple[float, dict]] = OrderedDict()
        self._inflight: dict[tuple, asyncio.Task] = {}

    @staticmethod
    def key(prompt: str, max_tokens: int) -> tuple[str, int]:
        return (prompt, max_tokens)

    def __len__(self) -> int:
        return len(self._entries)

    def get(self, key: tuple) -> dict | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        expires_at, body = entry
        if self._clock() >= expires_at:
            del self._entries[key]
            return None
        self._entries.move_to_end(key)
        return copy.deepcopy(body)

    def put(self, key: tuple, body: dict) -> None:
        self._entries[key] = (self._clock() + self.ttl_s, copy.deepcopy(body))
        self._entries.move_to_end(key)
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)

    async def get_or_fetch(self, key: tuple,
                           fetch: Callable[[], Awaitable[Result]]) -> tuple[Result, Outcome]:
        cached = self.get(key)
        if cached is not None:
            return (200, cached), "hit"

        if not self.coalesce:
            status, body = await fetch()
            if status == 200:
                self.put(key, body)
            return (status, body), "miss"

        task = self._inflight.get(key)
        outcome: Outcome = "coalesced"
        if task is None:
            task = asyncio.ensure_future(self._fetch_and_store(key, fetch))
            self._inflight[key] = task
            task.add_done_callback(lambda t: self._forget(key, t))
            outcome = "miss"
        # shield: one caller disconnecting must not cancel the shared upstream call
        status, body = await asyncio.shield(task)
        return (status, copy.deepcopy(body)), outcome

    def _forget(self, key: tuple, task: asyncio.Task) -> None:
        if self._inflight.get(key) is task:
            del self._inflight[key]

    async def _fetch_and_store(self, key: tuple,
                               fetch: Callable[[], Awaitable[Result]]) -> Result:
        status, body = await fetch()
        if status == 200:
            self.put(key, body)
        return status, body
