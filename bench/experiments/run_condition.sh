#!/usr/bin/env bash
# Recreate the backend fleet with the given EXP_* env, restart the instrumented
# router on :8000, run the stock k6 bench, and print the k6 + router/upstream split.
#   EXP_DEGRADED_MS=0 bench/experiments/run_condition.sh NAME OUTDIR
set -euo pipefail
name=$1; out=${2:-/tmp/llm-router-exp}; mkdir -p "$out"
cd "$(dirname "$0")/../.."
docker compose -f docker-compose.yml -f bench/experiments/docker-compose.exp.yml up -d --force-recreate >/dev/null 2>&1
for p in 9001 9002 9003; do until curl -sf localhost:$p/healthz >/dev/null; do sleep 0.5; done; done
for s in 1 2 3; do echo "llm-service-$s env: $(docker compose exec -T llm-service-$s env | grep -E '^(SLOW_PCT|DEGRADED_MS|DEGRADED_REPLICA)=' | tr '\n' ' ')"; done
pid=$(lsof -t -iTCP:8000 -sTCP:LISTEN || true); [ -n "$pid" ] && kill $pid && sleep 2
APP_DB_PATH="$out/app.db" nohup uv run uvicorn bench.experiments.instrumented:app --port 8000 > "$out/router_$name.log" 2>&1 &
until curl -sf localhost:8000/v1/info >/dev/null 2>&1; do sleep 0.5; done
K6_SUMMARY_EXPORT="$out/$name.json" k6 run bench/k6.js > "$out/$name.k6.txt" 2>&1 || true
uv run python bench/experiments/k6sum.py "$out/$name.json" 2>/dev/null
uv run python bench/experiments/split.py "$out/router_$name.log" 2>/dev/null
