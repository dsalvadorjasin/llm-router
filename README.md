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

## Upstream hedging

`UpstreamPool.forward` hedges requests across the backend replicas (`app/hedging.py`):

- The primary attempt goes to the next replica in round-robin order. Replicas whose recent latency (EWMA) is well above the fastest one are moved to the back of the order; their stats expire after `LLM_HEDGE_PROBE_INTERVAL_S`, so they are re-tried as primary and picked up again once they recover.
- If no valid response (HTTP 200, non-empty `completion`, well-formed `signature` when present) arrives within the hedge delay, a duplicate goes to the next replica. Errors, non-200s, invalid bodies and timeouts fail over immediately. The first valid response wins; other in-flight attempts are cancelled.
- The hedge delay is a quantile of recent attempt latencies on healthy replicas, clamped to a min/max (or a fixed value).
- Backend responses are deterministic per prompt across replicas (same `completion` and `signature`), so any replica's response can be used.

| Env var | Default | Meaning |
|---------|---------|---------|
| `LLM_HEDGING` | `1` | `0`/`false`/`off` restores plain round-robin with no timeouts |
| `LLM_HEDGE_DELAY_MS` | `auto` | fixed hedge delay in ms, or `auto` for the adaptive quantile |
| `LLM_HEDGE_QUANTILE` | `0.5` | quantile of recent latencies used as the adaptive delay |
| `LLM_HEDGE_INITIAL_DELAY_MS` | `200` | delay used until enough samples are collected |
| `LLM_HEDGE_MIN_DELAY_MS` / `LLM_HEDGE_MAX_DELAY_MS` | `50` / `400` | clamp for the adaptive delay |
| `LLM_HEDGE_MAX_ATTEMPTS` | `3` | attempts launched on the hedge timer (errors can fail over beyond this) |
| `LLM_ATTEMPT_TIMEOUT_S` | `5` | per-attempt upstream timeout |
| `LLM_HEDGE_SLOW_FACTOR` | `2.0` | replica is deprioritised when its EWMA > best × factor + 50 ms |
| `LLM_HEDGE_PROBE_INTERVAL_S` | `2.0` | after this long without samples a replica's stats are ignored |

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
