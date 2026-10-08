import asyncio
from dataclasses import dataclass

import httpx

from .config import attempt_timeout_ms, hedge_delay_ms, hedging_enabled, max_attempts, upstream_urls
from .request_context import request_log_fields


@dataclass
class _Outcome:
    url: str
    status: int | None
    body: dict
    final: bool  # a response to hand to the caller (2xx or client 4xx), not a failure


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._url_list = list(urls or upstream_urls())
        self._rr = 0
        self._hedging = hedging_enabled()
        self._hedge_delay_s = hedge_delay_ms() / 1000
        self._attempt_timeout_s = attempt_timeout_ms() / 1000
        self._max_attempts = max_attempts()
        self._reaping: set[asyncio.Task] = set()
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._attempt_timeout_s), transport=transport
        )

    async def forward(self, payload: dict) -> tuple[int, dict]:
        n = len(self._url_list)
        start = self._rr % n
        self._rr += 1
        loop = asyncio.get_running_loop()
        pending: dict[asyncio.Task, str] = {}
        attempts = 0
        hedged = False
        last_failure: _Outcome | None = None

        def launch() -> None:
            nonlocal attempts
            url = self._url_list[(start + attempts) % n]
            attempts += 1
            pending[asyncio.create_task(self._attempt(url, payload))] = url

        launch()
        next_hedge_at = loop.time() + self._hedge_delay_s
        try:
            while pending:
                wait_s = None
                if self._hedging and attempts < self._max_attempts:
                    wait_s = max(0.0, next_hedge_at - loop.time())
                done, _ = await asyncio.wait(
                    pending.keys(), timeout=wait_s, return_when=asyncio.FIRST_COMPLETED
                )
                if not done:
                    launch()
                    hedged = True
                    next_hedge_at = loop.time() + self._hedge_delay_s
                    continue
                for task in done:
                    pending.pop(task)
                    outcome = task.result()
                    if outcome.final:
                        self._record(outcome.url, hedged)
                        return outcome.status, outcome.body
                    last_failure = outcome
                if attempts < self._max_attempts:
                    launch()
                    next_hedge_at = loop.time() + self._hedge_delay_s
        finally:
            for task in pending:
                task.cancel()
                self._reaping.add(task)
                task.add_done_callback(self._reaping.discard)

        assert last_failure is not None
        if last_failure.status is not None:
            self._record(last_failure.url, hedged)
            return last_failure.status, last_failure.body
        self._record("-", hedged)
        return (504 if last_failure.body.get("detail") == "upstream timeout" else 502), last_failure.body

    async def _attempt(self, url: str, payload: dict) -> _Outcome:
        try:
            resp = await asyncio.wait_for(
                self._client.post(f"{url}/v1/completions", json=payload), self._attempt_timeout_s
            )
        except (TimeoutError, httpx.TimeoutException):
            return _Outcome(url, None, {"detail": "upstream timeout"}, final=False)
        except httpx.HTTPError:
            return _Outcome(url, None, {"detail": "upstream unavailable"}, final=False)
        try:
            body = resp.json()
        except ValueError:
            if resp.status_code < 300:
                return _Outcome(url, None, {"detail": "upstream sent invalid JSON"}, final=False)
            body = {"detail": resp.text[:200] or "upstream error"}
        return _Outcome(url, resp.status_code, body, final=resp.status_code < 500)

    @staticmethod
    def _record(url: str, hedged: bool) -> None:
        fields = request_log_fields.get()
        if fields is not None:
            fields["upstream_url"] = url
            fields["hedged"] = hedged

    async def aclose(self) -> None:
        for task in list(self._reaping):
            task.cancel()
        await self._client.aclose()
