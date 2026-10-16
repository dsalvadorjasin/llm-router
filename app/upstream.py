import hashlib
import json

import httpx

from .config import upstream_urls


def affinity_key(payload: dict) -> str:
    prompt = payload.get("prompt")
    if isinstance(prompt, str):
        return prompt
    return json.dumps(payload, sort_keys=True, default=str)


def pick_backend(urls: list[str], payload: dict) -> str:
    if not urls:
        raise RuntimeError("no upstream URLs configured")
    digest = hashlib.sha256(affinity_key(payload).encode()).hexdigest()
    return urls[int(digest, 16) % len(urls)]


class UpstreamPool:
    def __init__(self, urls: list[str] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._urls = list(urls or upstream_urls())
        self._client = httpx.AsyncClient(timeout=None, transport=transport)

    async def forward(self, payload: dict) -> tuple[int, dict]:
        url = pick_backend(self._urls, payload)
        resp = await self._client.post(f"{url}/v1/completions", json=payload)
        return resp.status_code, resp.json()

    async def aclose(self) -> None:
        await self._client.aclose()
