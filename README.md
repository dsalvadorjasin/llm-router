# llm-router

LLM gateway with a built-in playground UI — routes generation requests across a fleet of model backends and serves a chat interface for internal use.

## Prerequisites

- Docker
- [uv](https://docs.astral.sh/uv/)
- Python 3.12
- Node >= 22
- [k6](https://k6.io/)

## Quickstart

In separate terminals:

```bash
make services   # start the backend fleet
make ui         # build the playground frontend
make dev        # start the router (serves the API and the UI)
```

Open http://localhost:8000.

Other targets:

```bash
make bench   # run the k6 load harness
make e2e     # run the Playwright end-to-end UI tests
```

## API

| Method | Path                                       | Description                              |
|--------|---------------------------------------------|-------------------------------------------|
| POST   | `/v1/generate`                               | Forward a single prompt to the backend fleet |
| POST   | `/v1/chat`                                   | Send a message to a conversation and get a reply |
| GET    | `/v1/info`                                   | App name, version, and available models |
| GET    | `/v1/conversations?q=`                       | List conversations, optionally filtered by title |
| POST   | `/v1/conversations`                          | Create a conversation |
| PATCH  | `/v1/conversations/{id}`                     | Rename and/or pin/unpin a conversation |
| DELETE | `/v1/conversations/{id}`                     | Delete a conversation |
| GET    | `/v1/conversations/{id}/messages?limit=&before=` | List messages in a conversation, with optional pagination |
| GET    | `/v1/conversations/{id}/export`              | Download the conversation as a markdown transcript |

## Upstream routing

`app/routing.py` picks a backend replica per attempt by an EWMA of its observed latency (failed or timed-out attempts count as penalised samples). Replicas within the tie band of the best score are used in round-robin order; a deprioritised replica's penalty decays over time so it is probed again and gets traffic back once it recovers. Each attempt has an adaptive timeout (a multiple of a high quantile of recent successful latencies); timeouts, transport errors and non-200s are retried on another replica.

| Env var | Default | Meaning |
|---------|---------|---------|
| `ROUTER_LATENCY_AWARE` | `1` | `0` restores plain round-robin with no timeouts or retries |
| `ROUTER_POLICY` | `ewma` | `ewma`, `least_outstanding` or `round_robin` |
| `ROUTER_MAX_ATTEMPTS` | `4` | Attempts per request (each on a different replica than the previous one) |
| `ROUTER_ATTEMPT_TIMEOUT_MS` | `250` | Per-attempt timeout until enough samples are collected |
| `ROUTER_ATTEMPT_TIMEOUT_MIN_MS` / `_MAX_MS` | `50` / `1000` | Clamp for the adaptive per-attempt timeout |
| `ROUTER_TIMEOUT_QUANTILE` / `ROUTER_TIMEOUT_MULTIPLIER` | `0.99` / `1.03` | Adaptive timeout = quantile of recent successful latencies x multiplier |
| `ROUTER_FINAL_TIMEOUT_MS` | `10000` | Timeout of the last attempt |
| `ROUTER_EWMA_ALPHA` | `0.3` | EWMA smoothing factor |
| `ROUTER_FAILURE_PENALTY` | `2.0` | Failed attempt is recorded as `max(elapsed, timeout) x penalty` |
| `ROUTER_DECAY_HALF_LIFE_S` | `10` | Half-life of a replica's excess score while it gets no traffic |
| `ROUTER_TIE_RATIO` / `ROUTER_TIE_ABS_MS` | `1.5` / `10` | Scores within `best x ratio` or `best + abs` count as tied |

## Layout

- `app/` — FastAPI gateway: generate/chat/info routes, conversation store, markdown export, request logging middleware
- `bench/` — k6 load test script and weighted workload
- `docker-compose.yml` — backend fleet service definitions
- `frontend/` — React + TypeScript playground UI (Vite, Vitest, Testing Library, Playwright)
- `Makefile` — `make services` / `make ui` / `make dev` / `make bench` / `make e2e`
- `pyproject.toml` — Python project and dependencies (uv)
- `scripts/` — helper scripts (e2e stack bootstrap)
- `tests/` — backend unit tests (pytest)
- `uv.lock` — locked Python dependencies
