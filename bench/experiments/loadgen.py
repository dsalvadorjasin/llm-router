"""Open-loop load: /v1/generate at a fixed rate, optionally with /v1/chat traffic alongside.

    uv run python bench/experiments/loadgen.py --gen-rps 30 --chat-rps 10 \
        --conversation <id> --seconds 30 --out results.json

Reports client-side /v1/generate latency percentiles (and chat latency if any).
Used against a fixed-latency upstream so any inflation of generate latency is router-side.
"""
import argparse
import asyncio
import json
import time

import httpx


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = (len(xs) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


async def one(client, method, url, body, out):
    start = time.monotonic()
    try:
        r = await client.request(method, url, json=body)
        status = r.status_code
    except Exception as e:  # noqa: BLE001
        status = repr(e)
    out.append(((time.monotonic() - start) * 1000, status))


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--gen-rps", type=float, default=30)
    ap.add_argument("--chat-rps", type=float, default=0)
    ap.add_argument("--conversation", action="append", default=[])
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--out")
    ap.add_argument("--brief", action="store_true")
    a = ap.parse_args()

    gen, chat = [], []
    limits = httpx.Limits(max_connections=1000, max_keepalive_connections=1000)
    async with httpx.AsyncClient(base_url=a.base, timeout=120, limits=limits) as client:
        tasks = []
        t0 = time.monotonic()

        async def schedule(rate, make):
            if rate <= 0:
                return
            n = int(a.seconds * rate)
            for i in range(n):
                delay = t0 + i / rate - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                tasks.append(asyncio.create_task(make(i)))

        def mk_gen(i):
            return one(client, "POST", "/v1/generate",
                       {"prompt": f"loop-block probe {i}", "max_tokens": 16}, gen)

        def mk_chat(i):
            conv = a.conversation[i % len(a.conversation)]
            return one(client, "POST", "/v1/chat",
                       {"conversation_id": conv, "message": f"follow-up {i}", "max_tokens": 16}, chat)

        await asyncio.gather(schedule(a.gen_rps, mk_gen), schedule(a.chat_rps, mk_chat))
        await asyncio.gather(*tasks)

    def summary(xs):
        lat = [x for x, s in xs]
        return {"n": len(xs), "errors": sum(1 for _, s in xs if s != 200),
                "p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99),
                "max": max(lat) if lat else None}

    res = {"generate": summary(gen), "chat": summary(chat) if chat else None, "args": vars(a)}
    if a.brief:
        for k in ("generate", "chat"):
            if res[k]:
                print(f"{k:<8} " + " ".join(f"{f}={res[k][f]:.1f}" if isinstance(res[k][f], float)
                                            else f"{f}={res[k][f]}" for f in res[k]))
    else:
        print(json.dumps(res, indent=1))
    if a.out:
        json.dump(res, open(a.out, "w"), indent=1)


asyncio.run(main())
