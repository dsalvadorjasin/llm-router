import asyncio
import copy
import json
import logging
import re
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

logger = logging.getLogger("app.cache")

Fetch = Callable[[dict], Awaitable[tuple[int, dict]]]

_SIGNATURE_RE = re.compile(r"^[0-9a-f]{64}$")


def cache_key(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def is_cacheable(status: int, body) -> bool:
    return (
        200 <= status < 300
        and isinstance(body, dict)
        and isinstance(body.get("completion"), str)
        and len(body["completion"]) > 0
        and isinstance(body.get("signature"), str)
        and _SIGNATURE_RE.match(body["signature"]) is not None
    )


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    coalesced: int = 0
    stores: int = 0
    evictions: int = 0
    expirations: int = 0
    fallthroughs: int = 0

    @property
    def hit_ratio(self) -> float:
        total = self.hits + self.coalesced + self.misses
        if total == 0:
            return 0.0
        return (self.hits + self.coalesced) / total

    def as_dict(self) -> dict:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "coalesced": self.coalesced,
            "stores": self.stores,
            "evictions": self.evictions,
            "expirations": self.expirations,
            "fallthroughs": self.fallthroughs,
        }


class ResponseCache:
    def __init__(
        self,
        ttl_s: float,
        max_entries: int,
        coalesce: bool = True,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.ttl_s = ttl_s
        self.max_entries = max_entries
        self.coalesce = coalesce
        self.clock = clock
        self._entries: OrderedDict[str, tuple[float, int, dict]] = OrderedDict()
        self._inflight: dict[str, asyncio.Task] = {}
        self._stats = CacheStats()
        self._lookups = 0

    @property
    def stats(self) -> CacheStats:
        return self._stats

    def __len__(self) -> int:
        return len(self._entries)

    def log_stats(self) -> None:
        logger.info(
            "response cache stats: %s hit_ratio=%.3f",
            self._stats.as_dict(),
            self._stats.hit_ratio,
        )

    def _record_lookup(self) -> None:
        self._lookups += 1
        if self._lookups % 500 == 0:
            self.log_stats()

    def _lookup(self, key: str):
        entry = self._entries.get(key)
        if entry is None:
            return None
        expires_at, status, body = entry
        if self.clock() >= expires_at:
            del self._entries[key]
            self._stats.expirations += 1
            return None
        self._entries.move_to_end(key)
        self._stats.hits += 1
        return status, copy.deepcopy(body)

    def _store(self, key: str, status: int, body) -> None:
        if self.ttl_s <= 0 or self.max_entries <= 0 or not is_cacheable(status, body):
            return
        self._entries[key] = (self.clock() + self.ttl_s, status, body)
        self._entries.move_to_end(key)
        self._stats.stores += 1
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)
            self._stats.evictions += 1

    async def get_or_fetch(self, payload: dict, fetch: Fetch) -> tuple[int, dict]:
        self._record_lookup()
        key = cache_key(payload)
        hit = self._lookup(key)
        if hit is not None:
            return hit

        if not self.coalesce:
            self._stats.misses += 1
            status, body = await fetch(payload)
            self._store(key, status, body)
            return status, body

        task = self._inflight.get(key)
        if task is None:
            self._stats.misses += 1
            task = asyncio.ensure_future(fetch(payload))
            self._inflight[key] = task

            def _cleanup(t: asyncio.Task, key: str = key, task: asyncio.Task = task) -> None:
                if self._inflight.get(key) is task:
                    del self._inflight[key]
                if not t.cancelled():
                    t.exception()

            task.add_done_callback(_cleanup)
            status, body = await asyncio.shield(task)
            self._store(key, status, body)
            return status, body

        self._stats.coalesced += 1
        try:
            status, body = await asyncio.shield(task)
        except asyncio.CancelledError:
            if not task.cancelled():
                raise
        except Exception:
            pass
        else:
            if is_cacheable(status, body):
                return status, copy.deepcopy(body)

        self._stats.fallthroughs += 1
        status, body = await fetch(payload)
        self._store(key, status, body)
        return status, body
