import time

import httpx

from .balancer import Balancer, LatencyAwareBalancer, RoundRobinBalancer
from .config import RoutingSettings, routing_settings, upstream_urls


def make_balancer(size: int, settings: RoutingSettings) -> Balancer:
    if settings.strategy == "round_robin":
        return RoundRobinBalancer(size)
    return LatencyAwareBalancer(
        size,
        alpha=settings.ewma_alpha,
        probe_interval_s=settings.probe_interval_s,
        error_penalty_s=settings.error_penalty_s,
        outstanding_weight=settings.outstanding_weight,
    )


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None,
                 settings: RoutingSettings | None = None,
                 balancer: Balancer | None = None):
        self._urls = urls or upstream_urls()
        self.balancer = balancer or make_balancer(len(self._urls), settings or routing_settings())
        self._client = httpx.AsyncClient(timeout=None, transport=transport)

    async def forward(self, payload: dict) -> tuple[int, dict]:
        index = self.balancer.acquire()
        start = time.monotonic()
        ok = False
        try:
            resp = await self._client.post(f"{self._urls[index]}/v1/completions", json=payload)
            body = resp.json()
            ok = resp.status_code < 500
            return resp.status_code, body
        finally:
            self.balancer.release(index, time.monotonic() - start, ok)

    async def aclose(self) -> None:
        await self._client.aclose()
