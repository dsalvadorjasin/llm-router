import asyncio
import time
from collections.abc import Callable

import httpx

from . import config
from .config import upstream_urls
from .request_context import set_log_field
from .routing import LatencyRouter


class _RetryableStatus(Exception):
    def __init__(self, status: int, body: dict):
        self.status = status
        self.body = body


def _json_body(resp: httpx.Response) -> dict:
    try:
        body = resp.json()
    except ValueError:
        return {"detail": resp.text or "upstream error"}
    return body if isinstance(body, dict) else {"detail": body}


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 *,
                 router: LatencyRouter | None = None,
                 max_attempts: int | None = None,
                 attempt_timeout_ms: float | None = None,
                 clock: Callable[[], float] = time.monotonic,
                 rng: Callable[[], float] | None = None):
        urls = urls or upstream_urls()
        if router is None:
            kwargs = {"clock": clock}
            if rng is not None:
                kwargs["rng"] = rng
            router = LatencyRouter(urls, **kwargs)
        self.router = router
        self._max_attempts = max(1, max_attempts if max_attempts is not None
                                 else config.max_attempts())
        timeout_ms = (attempt_timeout_ms if attempt_timeout_ms is not None
                      else config.attempt_timeout_ms())
        self._attempt_timeout_s = timeout_ms / 1000 if timeout_ms > 0 else None
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._attempt_timeout_s), transport=transport)

    async def _attempt(self, url: str, payload: dict) -> tuple[int, dict]:
        resp = await self._client.post(f"{url}/v1/completions", json=payload)
        body = _json_body(resp)
        if resp.status_code >= 500:
            raise _RetryableStatus(resp.status_code, body)
        return resp.status_code, body

    async def forward(self, payload: dict) -> tuple[int, dict]:
        plan = self.router.plan()
        attempts = [plan[i % len(plan)] for i in range(self._max_attempts)]
        last: tuple[int, dict] = (502, {"detail": "upstream unavailable"})
        for url in attempts:
            token = self.router.start(url)
            try:
                status, body = await asyncio.wait_for(
                    self._attempt(url, payload), self._attempt_timeout_s)
            except _RetryableStatus as exc:
                self.router.finish(url, token, ok=False)
                last = (exc.status, exc.body)
            except (asyncio.TimeoutError, httpx.TimeoutException):
                self.router.finish(url, token, ok=False)
                last = (504, {"detail": "upstream timeout"})
            except httpx.HTTPError:
                self.router.finish(url, token, ok=False)
                last = (502, {"detail": "upstream unavailable"})
            except BaseException:
                self.router.abandon(url, token)
                raise
            else:
                self.router.finish(url, token, ok=True)
                set_log_field("upstream_url", url)
                set_log_field("hedged", False)
                return status, body
        set_log_field("upstream_url", url)
        set_log_field("hedged", False)
        return last

    async def aclose(self) -> None:
        await self._client.aclose()
