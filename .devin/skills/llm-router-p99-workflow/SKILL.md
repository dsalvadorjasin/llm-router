---
name: llm-router-p99-workflow
description: Dynamic workflow that cuts /v1/generate p99 below 500ms with 0% errors by benchmarking main, building hedging, latency-aware routing and response caching on parallel branches, combining the winners, gating, and opening one PR. Use when asked to (re)run the p99 latency experiment on llm-router.
---

# llm-router p99 latency workflow

Run it with `run_workflow`, `workflow_name="llm-router-p99-v1"` and `script_path` set to the absolute path of `workflow.py` in this folder. Run your plan past the user first. It starts 7 child sessions (baseline, 3 strategies, combine, final gate, PR) on separate machines, each billed in ACUs (about 12 ACUs and 25 minutes the first time).

## What it does
1. **baseline** and the 3 **strategies** start at the same time:
   - `baseline`: `make bench` 3 times on main, median p99.
   - `<BRANCH_PREFIX>-hedge-requests` (`app/hedging.py`): probes replicas for signature determinism first.
   - `<BRANCH_PREFIX>-latency-aware-routing` (`app/routing.py`).
   - `<BRANCH_PREFIX>-response-cache` (`app/cache.py`).
   - The script itself checks each strategy's gate: `uv run pytest tests/` passes, error rate is 0 and all k6 checks pass.
2. **combine**: merges every strategy that passed its gate and beat the baseline median p99 into `<BRANCH_PREFIX>-combined`, best p99 first.
3. **final-gate**: on the combined branch, bench 3 times, pytest and `make e2e`. Changes no code. Fails the run if a test or check fails, any error rate is above 0, or median p99 is 500ms or more.
4. **pr**: opens one PR to main with a bench table the script builds from the recorded results.

## Before re-running
- Change `BRANCH_PREFIX` at the top of `workflow.py` (for example `devin/p99-r2`), or delete the old branches. Otherwise the agents run into branches from the previous run.
- The "beat baseline" merge rule has no margin. In the first run the cache scored 4148ms against a 4153ms baseline, which is within noise, and still got merged. Add a margin to the `r["p99"] >= baseline["p99_median"]` check if that matters.
- Every prompt says not to touch the backends, `docker-compose.yml`, `bench/k6.js` or the workload. Each strategy has an env-var kill switch in `app/config.py`, and the existing tests must pass unchanged.
