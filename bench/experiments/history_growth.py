"""Per-turn /v1/chat latency for conversations of N turns.

    uv run python bench/experiments/history_growth.py --turns 1 10 50 100 --out growth.json

For each N: create a conversation, send N sequential turns, record client-side
turn latency, the upstream latency the router reports (`latency_ms`), and the
router-side remainder (client - upstream). Also times flatten_history in-process
on the same history sizes.
"""
import argparse
import json
import time

import httpx

from app.prompting import flatten_history


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--turns", type=int, nargs="+", default=[1, 10, 50, 100])
    ap.add_argument("--msg-chars", type=int, default=200)
    ap.add_argument("--out")
    a = ap.parse_args()
    msg = ("tell me more about latency budgets " * 20)[: a.msg_chars]
    results = {}
    with httpx.Client(base_url=a.base, timeout=120) as c:
        for n in a.turns:
            conv = c.post("/v1/conversations", json={"title": f"growth-{n}"}).json()["id"]
            turns = []
            for i in range(n):
                t = time.monotonic()
                r = c.post("/v1/chat", json={"conversation_id": conv, "message": f"{i}: {msg}",
                                             "max_tokens": 64})
                total = (time.monotonic() - t) * 1000
                r.raise_for_status()
                up = r.json()["latency_ms"]
                hist = c.get(f"/v1/conversations/{conv}/messages").json()
                prompt_chars = len(flatten_history(hist[:-2], f"{i}: {msg}"))
                turns.append({"turn": i + 1, "total_ms": total, "upstream_ms": up,
                              "router_ms": total - up, "prompt_chars": prompt_chars})
            results[n] = turns
            last = turns[-1]
            print(f"N={n:3d} last turn: total={last['total_ms']:.1f} upstream={last['upstream_ms']} "
                  f"router={last['router_ms']:.1f} prompt_chars={last['prompt_chars']}")
    # in-process flatten_history cost at the same sizes
    flat = {}
    for n in sorted({t for ts in results.values() for t in range(1, len(ts) + 1)} | {1000}):
        hist = [{"role": "user" if j % 2 == 0 else "assistant", "content": msg} for j in range(2 * (n - 1))]
        reps = 200
        t = time.perf_counter()
        for _ in range(reps):
            flatten_history(hist, msg)
        flat[n] = (time.perf_counter() - t) / reps * 1000
    print("flatten_history ms at turn 1/10/50/100/1000:",
          {k: round(flat[k], 4) for k in (1, 10, 50, 100, 1000) if k in flat})
    if a.out:
        json.dump({"turns": results, "flatten_ms": flat}, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    main()
