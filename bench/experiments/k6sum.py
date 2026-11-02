"""Print p50/p95/p99/max and error rate from a K6_SUMMARY_EXPORT json."""
import json
import sys

for path in sys.argv[1:]:
    m = json.load(open(path))["metrics"]
    d = m["http_req_duration"]["values"] if "values" in m["http_req_duration"] else m["http_req_duration"]
    f = m["http_req_failed"]
    f = f.get("values", f)
    reqs = m["http_reqs"].get("values", m["http_reqs"])
    print(f"{path}: reqs={reqs['count']} p50={d['p(50)']:.1f} p95={d['p(95)']:.1f} "
          f"p99={d['p(99)']:.1f} max={d['max']:.1f} err={f.get('rate', f.get('value')):.4f}")
