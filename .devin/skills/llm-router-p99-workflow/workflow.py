import asyncio
import json
import statistics

REPO = "dsalvadorjasin/llm-router"
REPOS = [REPO]
# Change BRANCH_PREFIX for a re-run so branches from earlier runs are not overwritten.
BRANCH_PREFIX = "devin/p99"
INTEGRATION_BRANCH = f"{BRANCH_PREFIX}-combined"
P99_TARGET_MS = 500

SETUP = """Environment setup (the VM may not have these tools; install only what is missing):
- uv: `command -v uv || (curl -LsSf https://astral.sh/uv/install.sh | sh && export PATH="$HOME/.local/bin:$PATH")`
- k6 v1.3.0: `command -v k6 || (mkdir -p ~/.local/bin && curl -fsSL https://github.com/grafana/k6/releases/download/v1.3.0/k6-v1.3.0-linux-amd64.tar.gz | tar xz -C /tmp && mv /tmp/k6-v1.3.0-linux-amd64/k6 ~/.local/bin/ && export PATH="$HOME/.local/bin:$PATH")`
- Git: push through the plain `https://github.com/...` origin remote (Devin git proxy); do not rewrite the remote with a token in the URL (that 403s).
- In the repo root: `uv sync`, then `make services` (docker compose: 3 backend replicas on ports 9001-9003, image ghcr.io/abboudp/llm-service:latest). Wait until `curl -sf localhost:900{1,2,3}/healthz` succeeds for all three.
- Router: `uv run uvicorn app.main:app --port 8000` (run it in a separate background shell; log to a file).
"""

BENCH_RULES = """Bench protocol (follow exactly):
- `make bench` runs k6 (bench/k6.js, 30 rps for 60s against localhost:8000/v1/generate). Its summary prints p50/p95/p99, `error rate` and `checks` percentages.
- Before EACH bench run, (re)start the router fresh: stop it BY PORT with `lsof -t -iTCP:8000 -sTCP:LISTEN | xargs -r kill` (NEVER use `pkill -f uvicorn` / `pkill -f "uvicorn app.main:app"`: that pattern also matches the backend containers' processes and your own shell), start it again, and wait until `curl -sf localhost:8000/v1/info` succeeds. Never run two benches at once.
- A run's checks pass only if the summary shows `checks 100.00 %` and k6 exits 0 (thresholds met). Record p99 in ms (number), error rate as a fraction (0.0 = 0%), and checks rate as a fraction.
"""

RULES = """Hard constraints:
- Only router code may change (app/, tests/, README/docs if useful). Do NOT modify the LLM backends, docker-compose.yml, bench/k6.js, bench/workload.json, k6 thresholds, the Makefile bench target, or the frontend.
- Do NOT hard-code anything about individual replicas (no special-casing a URL, port, replica id or 'slow replica'); behaviour must adapt to whatever the replicas do at runtime. Never permanently exclude a replica.
- Config-gate the strategy via environment variables read in app/config.py (follow the existing `upstream_urls()` style). Defaults must be the tuned, ENABLED values so a plain `uv run uvicorn app.main:app --port 8000` uses the strategy; an env var must allow turning it off (restoring today's behaviour).
- Do not weaken or delete existing tests or assertions. The existing tests must pass UNMODIFIED (notably tests/test_upstream.py::test_round_robin_and_passthrough, which calls `UpstreamPool(urls=..., transport=MockTransport)` and expects round-robin order u1,u2,u3,u1 for 4 sequential calls with payload {"prompt": "p"}). Design so that remains true (e.g. tie-breaks in round-robin order, hedges that never fire on instant responses, caching outside UpstreamPool.forward). Add new unit tests for the new behaviour using httpx.MockTransport.
- Upstream non-200 responses and transport errors must not surface as router errors when another replica can serve the request; the bench requires 0% errors.
- Ignore pre-existing remote branches from earlier experiments (e.g. devin/response-cache, devin/load-aware-routing, devin/generate-hedging-cache); start from origin/main.
- Do NOT open a pull request.
"""

ARCH = """Code layout (to keep the later merge of three parallel strategies clean, which all touch app/upstream.py):
- Current app/upstream.py: `UpstreamPool.__init__(urls=None, transport=None)` builds `itertools.cycle(urls)` and one `httpx.AsyncClient(timeout=None)`; `forward(payload) -> (status_code, json)` posts to `{url}/v1/completions`. app/main.py `/v1/generate` and app/routes/chat.py both call `app.state.pool.forward(...)`.
- Put your strategy's logic in its OWN new module (named below) and keep edits to app/upstream.py, app/config.py and app/main.py as small, additive hooks as possible. Do not reformat unrelated code.
"""

STRATEGIES = [
    {
        "strategy": "hedge-requests",
        "branch": f"{BRANCH_PREFIX}-hedge-requests",
        "module": "app/hedging.py",
        "task": """Strategy: hedged requests.
1. FIRST, with the backends running, probe all 3 replicas directly (POST localhost:9001/9002/9003 /v1/completions with the SAME body, e.g. {"prompt": "<a few different prompts>", "max_tokens": 64}, several times each) and determine whether `signature` (and `completion`) is deterministic across replicas for the same (prompt, max_tokens). Report the finding.
2. Implement hedging in UpstreamPool: send the request to a primary replica; if no valid response arrives within a delay threshold (configurable, tune it from observed latencies, e.g. a fixed ms value or a percentile of recent latencies), send a duplicate to a different replica; the first VALID response (HTTP 200 with a non-empty completion and well-formed signature) wins and the loser is cancelled. If a replica errors, fail over to another replica immediately. Use bounded per-attempt timeouts rather than timeout=None.
3. If signatures are NOT deterministic across replicas, hedge in a way that preserves per-prompt signature consistency (e.g. pin repeated prompts to the replica that first served them and only accept responses from it, or hedge only to a replica known to give the same signature).""",
    },
    {
        "strategy": "latency-aware-routing",
        "branch": f"{BRANCH_PREFIX}-latency-aware-routing",
        "module": "app/routing.py",
        "task": """Strategy: latency-aware replica selection.
Replace round-robin with latency-aware selection: EWMA of per-replica latency and/or least-outstanding-requests (your choice, justify it). Requirements: never permanently exclude a replica (keep probing/decaying so a recovered replica gets traffic back); ties broken in round-robin order; failed attempts penalise the replica and the request is retried on another replica; bounded per-attempt timeouts instead of timeout=None. Make the selection policy configurable (e.g. ROUTER_POLICY=round_robin|ewma|least_outstanding) with the tuned one as default.""",
    },
    {
        "strategy": "response-cache",
        "branch": f"{BRANCH_PREFIX}-response-cache",
        "module": "app/cache.py",
        "task": """Strategy: response cache.
Cache successful upstream responses keyed on (prompt, max_tokens) with a TTL and a bounded size with eviction (e.g. LRU). Only cache valid 200 responses. Coalesce concurrent identical in-flight requests (single-flight) so a burst of the same prompt causes one upstream call. Configurable via env (enable flag, TTL seconds, max entries). Place the cache so the existing round-robin unit test still passes unmodified (it repeats the same prompt 4 times through UpstreamPool.forward and expects 4 upstream hits) — e.g. apply caching in the /v1/generate path or a wrapper the app constructs, not inside UpstreamPool.forward's default behaviour.""",
    },
]

BASELINE_SCHEMA = {
    "type": "object",
    "properties": {
        "strategy": {"type": "string"},
        "p99_median": {"type": "number"},
        "error_rate": {"type": "number"},
        "runs": {"type": "array", "items": {"type": "object", "properties": {
            "p99": {"type": "number"}, "error_rate": {"type": "number"},
            "checks_rate": {"type": "number"}}}},
        "main_sha": {"type": "string"},
        "notes": {"type": "string"},
    },
    "required": ["strategy", "p99_median", "error_rate", "runs", "main_sha"],
}

STRATEGY_SCHEMA = {
    "type": "object",
    "properties": {
        "strategy": {"type": "string"},
        "branch": {"type": "string"},
        "head_sha": {"type": "string"},
        "p99": {"type": "number"},
        "error_rate": {"type": "number"},
        "checks_pass": {"type": "boolean"},
        "tests_pass": {"type": "boolean"},
        "tests_summary": {"type": "string"},
        "config_flags": {"type": "string"},
        "signature_deterministic_across_replicas": {"type": "string"},
        "summary": {"type": "string"},
    },
    "required": ["strategy", "branch", "head_sha", "p99", "error_rate",
                 "checks_pass", "tests_pass", "summary"],
}

COMBINE_SCHEMA = {
    "type": "object",
    "properties": {
        "branch": {"type": "string"},
        "head_sha": {"type": "string"},
        "merged": {"type": "array", "items": {"type": "string"}},
        "conflict_notes": {"type": "string"},
        "tests_pass": {"type": "boolean"},
        "sanity_p99": {"type": "number"},
        "sanity_error_rate": {"type": "number"},
        "summary": {"type": "string"},
    },
    "required": ["branch", "head_sha", "merged", "tests_pass", "summary"],
}

FINAL_SCHEMA = {
    "type": "object",
    "properties": {
        "head_sha": {"type": "string"},
        "runs": {"type": "array", "items": {"type": "object", "properties": {
            "p99": {"type": "number"}, "error_rate": {"type": "number"},
            "checks_rate": {"type": "number"}, "k6_exit_code": {"type": "integer"}}}},
        "pytest_pass": {"type": "boolean"},
        "pytest_summary": {"type": "string"},
        "e2e_pass": {"type": "boolean"},
        "e2e_summary": {"type": "string"},
        "notes": {"type": "string"},
    },
    "required": ["head_sha", "runs", "pytest_pass", "e2e_pass"],
}

PR_SCHEMA = {
    "type": "object",
    "properties": {"pr_url": {"type": "string"}, "pr_number": {"type": "integer"}},
    "required": ["pr_url"],
}

META = {
    "name": "llm-router-p99-v1",
    "description": "Cut llm-router p99 below 500ms with 0% errors: baseline, 3 parallel strategies, combine, final gate, PR",
    "product": "dsalvadorjasin/llm-router (FastAPI router)",
    "phases": [
        {"title": "baseline", "detail": "make bench x3 on main, median p99", "count": 1,
         "labels": ["baseline"], "soft_time_limit_minutes": 20},
        {"title": "strategies", "detail": "one branch per strategy, gated by pytest + bench",
         "count": 3, "labels": [s["strategy"] for s in STRATEGIES], "soft_time_limit_minutes": 50},
        {"title": "combine", "detail": "merge qualifying branches into one integration branch",
         "count": 1, "labels": ["combine"], "soft_time_limit_minutes": 40},
        {"title": "final-gate", "detail": "bench x3 + pytest + e2e on integration branch",
         "count": 1, "labels": ["final-gate"], "soft_time_limit_minutes": 30},
        {"title": "pr", "detail": "open single PR to main with bench table",
         "count": 1, "labels": ["pr"], "soft_time_limit_minutes": 15},
    ],
}


def baseline_prompt():
    return f"""Repository: https://github.com/{REPO} (FastAPI LLM router in front of 3 blackbox LLM backend replicas). Work on `main` exactly as it is; change NO code.

Task: measure the baseline latency of main.
{SETUP}
{BENCH_RULES}
Run `make bench` 3 times on main (restart the router before each run). Record p99 (ms), error rate and checks rate for each run, and report p99_median = median of the 3 p99 values, error_rate = median of the 3 error rates. Set strategy to "baseline". Report main_sha (`git rev-parse HEAD`). In notes, paste the three k6 summary blocks verbatim.
"""


def strategy_prompt(s):
    return f"""Repository: https://github.com/{REPO} (FastAPI LLM router in front of 3 blackbox LLM backend replicas on ports 9001-9003). Goal of the overall effort: reduce p99 latency of `make bench` below 500ms with 0% error rate. Known context: app/upstream.py `UpstreamPool.forward` does round-robin via itertools.cycle with a single httpx.AsyncClient(timeout=None); this is the likely source of 4s+ p99 spikes. Backends occasionally have multi-second latency tails per request, so the router must adapt at runtime.

Create branch `{s['branch']}` off origin/main and implement ONE strategy: {s['strategy']}.

{s['task']}

{ARCH}
Your strategy's new module: `{s['module']}`.

{RULES}
{SETUP}
{BENCH_RULES}
Gate (you must meet it before finishing; iterate/tune if not):
1. `uv run pytest tests/` passes (all tests, existing ones unmodified).
2. `make bench` on your branch shows error rate 0.00 %, checks 100.00 % and k6 exit code 0. Run the bench at least twice and report the run with the HIGHER p99 (be conservative).
Commit with clear messages and push the branch to origin. Report: strategy="{s['strategy']}", branch, head_sha (pushed), p99 (ms), error_rate (fraction), checks_pass, tests_pass, tests_summary (the pytest summary line), config_flags (env vars + defaults), signature_deterministic_across_replicas (your probe finding if you probed, else "not probed"), and a short summary of the design. If you cannot meet the gate, still push your best attempt and report the true numbers with checks_pass/tests_pass false.
"""


def combine_prompt(baseline, qualifying):
    branches = json.dumps(
        [{"strategy": r["strategy"], "branch": r["branch"], "head_sha": r["head_sha"],
          "p99": r["p99"], "config_flags": r.get("config_flags", ""),
          "summary": r["summary"]} for r in qualifying],
        sort_keys=True, indent=2)
    return f"""Repository: https://github.com/{REPO} (FastAPI LLM router in front of 3 blackbox LLM backend replicas). Goal: p99 of `make bench` below {P99_TARGET_MS}ms with 0% errors. Baseline (main) median p99 = {baseline['p99_median']:.0f} ms.

Task: combine these independently developed latency strategies (each already passed pytest + bench on its own branch, listed best p99 first) into ONE integration branch:
{branches}

Steps:
1. Create `{INTEGRATION_BRANCH}` from origin/main (if it already exists on origin from a previous attempt, reset your local branch to origin/main and force-push it).
2. Merge the branches in the listed order (`git merge --no-ff origin/<branch>`). All touch app/upstream.py (and likely app/config.py, app/main.py). Resolve conflicts so that EVERY strategy remains functional and individually config-gated by its env vars, with tuned defaults enabled. Make the strategies compose sensibly (e.g. cache in front; latency-aware selection chooses the primary and the hedge target; hedging/failover on top). Keep all unit tests from every branch.
3. Run `uv run pytest tests/` (must pass, existing tests unmodified), then one sanity `make bench`. If p99 >= {P99_TARGET_MS}ms or there are errors/check failures, tune the defaults/composition (without violating the constraints) and re-bench, at most 3 bench rounds.
{RULES}
{SETUP}
{BENCH_RULES}
Push `{INTEGRATION_BRANCH}`. Report branch, head_sha (pushed), merged (strategy names actually merged), conflict_notes (files and how resolved), tests_pass, sanity_p99, sanity_error_rate, summary.
"""


def final_prompt(combined):
    return f"""Repository: https://github.com/{REPO}. Check out branch `{INTEGRATION_BRANCH}` at commit {combined['head_sha']} (verify `git rev-parse HEAD`). Change NO code: this is a verification-only gate.
{SETUP}
{BENCH_RULES}
Run, in this order:
1. `make bench` 3 times (restart the router before each). Record p99 (ms), error rate (fraction), checks rate (fraction) and k6 exit code for each run.
2. `uv run pytest tests/` — record pass/fail and the summary line.
3. `make e2e` (Playwright UI tests; scripts/run_e2e.sh builds the frontend, starts the stack if needed and runs `npx playwright test` in frontend/). Stop your own router on :8000 first so the script can start its own with a temp DB. Record pass/fail and the summary line.
Report head_sha, runs, pytest_pass, pytest_summary, e2e_pass, e2e_summary, and notes (paste the k6 summary blocks verbatim).
"""


def pr_prompt(combined, table, final_note):
    return f"""Repository: https://github.com/{REPO}. Branch `{INTEGRATION_BRANCH}` (head {combined['head_sha']}) is already pushed and has passed the final gate. Change NO code.

Open ONE pull request (not a draft) from `{INTEGRATION_BRANCH}` into `main`. Use the repo's PR template if it has one. Title: "Cut /v1/generate p99 below 500ms: {' + '.join(combined['merged'])}".
Body must contain:
- A short summary of the combined design and each strategy, and how they compose (read the diff: `git diff origin/main...origin/{INTEGRATION_BRANCH}`).
- The config env vars and their defaults (how to disable each strategy).
- This bench table, verbatim:

{table}

- {final_note}
- A note that the backends, docker-compose.yml and bench/k6.js were not modified, and that no replica-specific assumptions are hard-coded.
Report pr_url and pr_number.
"""


def gate_passed(r):
    return bool(r["tests_pass"]) and bool(r["checks_pass"]) and r["error_rate"] == 0


async def run_strategy(s):
    try:
        r = await agent(strategy_prompt(s), phase="strategies", schema=STRATEGY_SCHEMA,
                        label=s["strategy"], repos=REPOS)
    except WorkflowAgentError as e:
        log(f"{s['strategy']}: agent failed: {e}")
        return {"strategy": s["strategy"], "branch": s["branch"], "failed": True}
    log(f"{s['strategy']}: p99={r['p99']:.0f}ms err={r['error_rate']} checks={r['checks_pass']} tests={r['tests_pass']}")
    return r


def fmt_rate(x):
    return f"{x * 100:.2f}%"


async def main():
    await register_workflow(META)

    log("baseline + 3 strategies starting concurrently (separate VMs, independent)")
    baseline_task = agent(baseline_prompt(), phase="baseline", schema=BASELINE_SCHEMA,
                          label="baseline", repos=REPOS)
    results = await asyncio.gather(baseline_task, *[run_strategy(s) for s in STRATEGIES])
    baseline, strat_results = results[0], list(results[1:])

    runs_p99 = [run["p99"] for run in baseline["runs"]]
    if len(runs_p99) == 3:
        baseline["p99_median"] = statistics.median(runs_p99)
    log(f"baseline: p99_median={baseline['p99_median']:.0f}ms runs={runs_p99} err={baseline['error_rate']}")

    qualifying, rejected = [], []
    for r in strat_results:
        if r.get("failed"):
            rejected.append((r["strategy"], "agent failed"))
        elif not gate_passed(r):
            rejected.append((r["strategy"], "gate failed"))
        elif r["p99"] >= baseline["p99_median"]:
            rejected.append((r["strategy"], "no p99 improvement vs baseline"))
        else:
            qualifying.append(r)
    for name, why in rejected:
        log(f"excluded {name}: {why}")
    if not qualifying:
        raise RuntimeError("No strategy passed its gate and improved p99; nothing to combine")
    qualifying.sort(key=lambda r: r["p99"])
    log(f"combining: {[r['strategy'] for r in qualifying]}")

    combined = await agent(combine_prompt(baseline, qualifying), phase="combine",
                           schema=COMBINE_SCHEMA, label="combine", repos=REPOS)
    if not combined["tests_pass"]:
        raise RuntimeError(f"combine: unit tests failing on {combined['branch']}")
    log(f"combined {combined['merged']} at {combined['head_sha']}")

    final = await agent(final_prompt(combined), phase="final-gate", schema=FINAL_SCHEMA,
                        label="final-gate", repos=REPOS)
    final_p99s = [run["p99"] for run in final["runs"]]
    final_errs = [run["error_rate"] for run in final["runs"]]
    final_checks = [run.get("checks_rate", 0) for run in final["runs"]]
    final_exit = [run.get("k6_exit_code", 1) for run in final["runs"]]
    final_median = statistics.median(final_p99s) if final_p99s else float("inf")
    failures = []
    if len(final["runs"]) != 3:
        failures.append(f"expected 3 bench runs, got {len(final['runs'])}")
    if any(e > 0 for e in final_errs):
        failures.append(f"error rate > 0: {final_errs}")
    if any(c < 1 for c in final_checks) or any(x != 0 for x in final_exit):
        failures.append(f"k6 checks/thresholds failed: checks={final_checks} exit={final_exit}")
    if not final["pytest_pass"]:
        failures.append("pytest failed")
    if not final["e2e_pass"]:
        failures.append("e2e failed")
    if final_median >= P99_TARGET_MS:
        failures.append(f"p99 median {final_median:.0f}ms >= {P99_TARGET_MS}ms target")
    log(f"final gate: p99 runs={final_p99s} median={final_median:.0f} errs={final_errs} pytest={final['pytest_pass']} e2e={final['e2e_pass']}")
    if failures:
        raise RuntimeError("Final gate failed: " + "; ".join(failures))

    rows = ["| Variant | p99 (ms) | Error rate | Gate / notes |", "|---|---|---|---|"]
    rows.append(f"| baseline (main, median of 3: {', '.join(f'{p:.0f}' for p in runs_p99)}) | "
                f"{baseline['p99_median']:.0f} | {fmt_rate(baseline['error_rate'])} | - |")
    merged = set(combined["merged"])
    for r in strat_results:
        if r.get("failed"):
            rows.append(f"| {r['strategy']} (alone) | n/a | n/a | agent failed |")
            continue
        note = "passed, merged" if r["strategy"] in merged else dict(rejected).get(r["strategy"], "passed, not merged")
        rows.append(f"| {r['strategy']} (alone) | {r['p99']:.0f} | {fmt_rate(r['error_rate'])} | {note} |")
    rows.append(f"| **combined (median of 3: {', '.join(f'{p:.0f}' for p in final_p99s)})** | "
                f"**{final_median:.0f}** | **{fmt_rate(max(final_errs))}** | all k6 checks 100% |")
    table = "\n".join(rows)
    final_note = (f"Final gate on {combined['head_sha'][:10]}: pytest: {final.get('pytest_summary', 'pass')}; "
                  f"e2e: {final.get('e2e_summary', 'pass')}. Each strategy was benched on its own VM; numbers are from 60s @ 30rps k6 runs.")
    log("bench table:\n" + table)

    pr = await agent(pr_prompt(combined, table, final_note), phase="pr", schema=PR_SCHEMA,
                     label="pr", repos=REPOS)
    log(f"PR: {pr['pr_url']}")


asyncio.run(main())
