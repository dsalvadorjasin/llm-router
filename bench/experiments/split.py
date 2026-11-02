"""Split router-side vs upstream-side latency from an instrumented router log.

    uv run python bench/experiments/split.py ROUTER_LOG [--since 'YYYY-MM-DD HH:MM:SS']
"""
import re
import statistics
import sys
from collections import defaultdict

REQ = re.compile(r"^(\S+ \S+) llm-router\.requests request method=\S+ path=(\S+) status=(\d+) "
                 r"duration_ms=([\d.]+) request_id=(\w+)")
UP = re.compile(r"^(\S+ \S+) llm-router\.upstream upstream url=(\S+) upstream_ms=([\d.]+) request_id=(\w+)")


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = (len(xs) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def row(name, xs):
    return (f"{name:<28} n={len(xs):5d} p50={pct(xs,50):8.1f} p95={pct(xs,95):8.1f} "
            f"p99={pct(xs,99):8.1f} max={max(xs) if xs else float('nan'):8.1f} mean={statistics.fmean(xs) if xs else float('nan'):8.1f}")


def main():
    path = sys.argv[1]
    since = sys.argv[sys.argv.index("--since") + 1] if "--since" in sys.argv else ""
    path_filter = sys.argv[sys.argv.index("--path") + 1] if "--path" in sys.argv else "/v1/generate"
    reqs, ups = {}, {}
    for line in open(path):
        m = REQ.match(line)
        if m and m.group(1) >= since and m.group(2) == path_filter:
            reqs[m.group(5)] = (int(m.group(3)), float(m.group(4)))
            continue
        m = UP.match(line)
        if m and m.group(1) >= since:
            ups[m.group(4)] = (m.group(2), float(m.group(3)))
    joined = [(rid, *reqs[rid], *ups[rid]) for rid in reqs if rid in ups]
    total = [j[2] for j in joined]
    upstream = [j[4] for j in joined]
    overhead = [j[2] - j[4] for j in joined]
    print(row("router duration_ms", total))
    print(row("upstream_ms", upstream))
    print(row("router overhead (dur-up)", overhead))
    by_url = defaultdict(list)
    for j in joined:
        by_url[j[3]].append(j[4])
    for url in sorted(by_url):
        print(row(f"upstream {url[-4:]}", by_url[url]) + f" share={len(by_url[url])/len(joined):.1%}")
    slowest = sorted(joined, key=lambda j: -j[2])[: max(1, len(joined) // 100)]
    s_by = defaultdict(int)
    for j in slowest:
        s_by[j[3][-4:]] += 1
    print(f"slowest 1% ({len(slowest)} reqs) by replica: {dict(s_by)}; "
          f"their overhead max={max(j[2]-j[4] for j in slowest):.1f}ms")
    unmatched = len(reqs) - len(joined)
    print(f"requests={len(reqs)} joined={len(joined)} unmatched={unmatched} "
          f"non200={sum(1 for r in reqs.values() if r[0] != 200)}")


if __name__ == "__main__":
    main()
