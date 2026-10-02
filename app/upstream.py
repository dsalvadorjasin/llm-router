import asyncio
from itertools import count

import httpx

from . import config


class UpstreamPool:
    """Round-robin pool with hedged requests.

    Each call starts on the next replica in rotation. If no response has arrived
    within `hedge_delay_s`, or the attempt fails at the transport level, the same
    request is also sent to the following replica, up to `max_attempts` replicas
    in flight. The first HTTP response wins and the rest are cancelled.
    """

    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 hedge_delay_s: float | None = None,
                 max_attempts: int | None = None):
        self._urls = urls or config.upstream_urls()
        self._next = count()
        self._hedge_delay_s = config.hedge_delay_s() if hedge_delay_s is None else hedge_delay_s
        attempts = config.upstream_max_attempts() if max_attempts is None else max_attempts
        self._max_attempts = max(1, min(attempts, len(self._urls)))
        self._client = httpx.AsyncClient(timeout=None, transport=transport)

    async def _attempt(self, url: str, payload: dict) -> tuple[int, dict]:
        resp = await self._client.post(f"{url}/v1/completions", json=payload)
        return resp.status_code, resp.json()

    async def forward(self, payload: dict) -> tuple[int, dict]:
        start = next(self._next)
        targets = [self._urls[(start + i) % len(self._urls)] for i in range(self._max_attempts)]
        pending: set[asyncio.Task] = set()
        error: BaseException | None = None
        try:
            for i, url in enumerate(targets):
                pending.add(asyncio.create_task(self._attempt(url, payload)))
                is_last = i == len(targets) - 1
                while pending:
                    done, pending = await asyncio.wait(
                        pending,
                        timeout=None if is_last else self._hedge_delay_s,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in done:
                        if task.exception() is None:
                            return task.result()
                        error = task.exception()
                    if not is_last:
                        break
            assert error is not None
            raise error
        finally:
            for task in pending:
                task.cancel()

    async def aclose(self) -> None:
        await self._client.aclose()
