#!/usr/bin/env bash
# Event-loop blocking experiment. Router upstream = one fixed-latency backend
# (100ms, no tail) on :9010, so /v1/generate latency above ~100ms is router-side.
#   bench/experiments/run_loop_block.sh NAME DB_SEED GEN_RPS CHAT_RPS OUTDIR [to_thread]
set -euo pipefail
name=$1; seed=$2; gen=$3; chat=$4; out=$5; mode=${6:-sync}
cd "$(dirname "$0")/../.."
docker inspect llm-fixed >/dev/null 2>&1 || docker run -d --name llm-fixed -p 9010:9000 \
  -e BASE_MS_MIN=100 -e BASE_MS_MAX=100 -e SLOW_PCT=0 -e DEGRADED_MS=0 -e REPLICA_ID=9 \
  ghcr.io/abboudp/llm-service:latest >/dev/null
until curl -sf localhost:9010/healthz >/dev/null; do sleep 0.5; done
pid=$(lsof -t -iTCP:8000 -sTCP:LISTEN || true); [ -n "$pid" ] && kill $pid && sleep 2
cp "$seed.db" "$out/$name.db"; rm -f "$out/$name.db-wal" "$out/$name.db-shm"
lock=0
if [ "$mode" = to_thread ]; then git apply bench/experiments/chat_to_thread.patch; lock=1; fi
trap '[ "$mode" = to_thread ] && git checkout app/routes/chat.py' EXIT
EXP_STORE_LOCK=$lock LLM_SERVICE_URLS=http://localhost:9010 APP_DB_PATH="$out/$name.db" \
  nohup uv run uvicorn bench.experiments.instrumented:app --port 8000 > "$out/router_$name.log" 2>&1 &
until curl -sf localhost:8000/v1/info >/dev/null 2>&1; do sleep 0.5; done
args=(); for c in $(cat "$seed.ids"); do args+=(--conversation "$c"); done
PYTHONPATH=. uv run python bench/experiments/loadgen.py --gen-rps "$gen" --chat-rps "$chat" \
  --seconds 30 "${args[@]}" --out "$out/$name.json" --brief 2>/dev/null
