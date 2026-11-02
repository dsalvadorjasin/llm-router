#!/usr/bin/env bash
# Frozen replica under bench load: pause llm-service-2 (TCP connect still succeeds,
# reads hang), run the stock k6 bench, count router->9002 sockets still open.
#   bench/experiments/run_stall_bench.sh OUTDIR
set -euo pipefail
out=$1; mkdir -p "$out"
cd "$(dirname "$0")/../.."
docker compose up -d --force-recreate >/dev/null 2>&1
for p in 9001 9002 9003; do until curl -sf localhost:$p/healthz >/dev/null; do sleep 0.5; done; done
pid=$(lsof -t -iTCP:8000 -sTCP:LISTEN || true); [ -n "$pid" ] && kill $pid && sleep 2
APP_DB_PATH="$out/app.db" nohup uv run uvicorn bench.experiments.instrumented:app --port 8000 > "$out/router_paused.log" 2>&1 &
until curl -sf localhost:8000/v1/info >/dev/null 2>&1; do sleep 0.5; done
docker compose pause llm-service-2 >/dev/null
trap 'docker compose unpause llm-service-2 >/dev/null 2>&1 || true' EXIT
K6_SUMMARY_EXPORT="$out/paused.json" k6 run bench/k6.js > "$out/paused.k6.txt" 2>&1 || true
rpid=$(lsof -t -iTCP:8000 -sTCP:LISTEN)
echo "router->:9002 sockets still ESTABLISHED after k6 finished: $(ss -tnp | grep ':9002 ' | grep -c "pid=$rpid,")"
uv run python bench/experiments/k6sum.py "$out/paused.json" 2>/dev/null
grep -E "request_failed|http_req_failed|status 200" "$out/paused.k6.txt" | head -5
uv run python bench/experiments/split.py "$out/router_paused.log" 2>/dev/null
sleep 30
echo "30s later, still ESTABLISHED: $(ss -tnp | grep ':9002 ' | grep -c "pid=$rpid,")"
