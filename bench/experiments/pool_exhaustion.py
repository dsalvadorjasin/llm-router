"""Show that requests to a *healthy* replica hang once a frozen replica holds
httpx's default 100 pooled connections (timeout=None => no pool timeout either).

Router must run with LLM_SERVICE_URLS=<blackhole>,<healthy>, e.g.
    LLM_SERVICE_URLS=http://localhost:9099,http://localhost:9001 \
      uv run uvicorn app.main:app --port 8001
    uv run python bench/experiments/pool_exhaustion.py http://localhost:8001
"""
import asyncio
import sys
import time

import httpx


async def main(base):
    async with httpx.AsyncClient(base_url=base, timeout=None,
                                 limits=httpx.Limits(max_connections=1000)) as c:
        async def call(i):
            t = time.monotonic()
            try:
                r = await asyncio.wait_for(
                    c.post("/v1/generate", json={"prompt": f"p{i}", "max_tokens": 8}), 15)
                return i, r.status_code, time.monotonic() - t
            except asyncio.TimeoutError:
                return i, "hung>15s", time.monotonic() - t

        # 240 requests, 20ms apart: round-robin sends even i to the black hole, odd i to the healthy replica.
        tasks = []
        for i in range(240):
            tasks.append(asyncio.create_task(call(i)))
            await asyncio.sleep(0.02)
        res = await asyncio.gather(*tasks)
    healthy = [r for r in res if r[0] % 2 == 1]
    ok = [r for r in healthy if r[1] == 200]
    hung = [r for r in healthy if r[1] != 200]
    print(f"black-hole-bound requests: {sum(1 for r in res if r[0] % 2 == 0)} (all hang by design)")
    print(f"healthy-replica-bound requests: {len(healthy)}; completed={len(ok)} hung={len(hung)}")
    if ok:
        print(f"  last healthy request that completed: #{max(r[0] for r in ok)}")
    if hung:
        print(f"  first healthy request that hung: #{min(r[0] for r in hung)} "
              f"(i.e. after {min(r[0] for r in hung)//2} black-hole requests were in flight)")


asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8001"))
