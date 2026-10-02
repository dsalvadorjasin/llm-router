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

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_SERVICE_URLS` | `http://localhost:9001,http://localhost:9002,http://localhost:9003` | Comma-separated backend replica URLs |
| `LLM_UPSTREAM_CONNECT_TIMEOUT` | `2` | Seconds to establish a connection to a backend |
| `LLM_UPSTREAM_READ_TIMEOUT` | `10` | Seconds to wait for each chunk of a backend response |
| `LLM_UPSTREAM_WRITE_TIMEOUT` | `5` | Seconds to send each chunk of the request to a backend |
| `LLM_UPSTREAM_POOL_TIMEOUT` | `5` | Seconds to wait for a free connection from the client pool |
| `LLM_UPSTREAM_HEDGE_DELAY_MS` | `200` | Milliseconds before a still-pending backend attempt is hedged on another replica; `0` disables hedging |
| `LLM_UPSTREAM_HEDGE_BUDGET_RATIO` | `0.2` | Hedge tokens earned per request (long-run cap on hedges as a fraction of requests) |
| `LLM_UPSTREAM_HEDGE_BUDGET_BURST` | `10` | Maximum stored hedge tokens |

Timeouts must be positive, finite numbers. A backend timeout is returned as `504` with
`{"detail": "upstream <connect|read|write|pool> timeout"}`.

Each attempt goes to the replica with the lowest `(in_flight + 1) * latency_ewma`; replicas
that returned a 5xx or transport error are skipped for 1s. Failed attempts fail over to another
replica (up to one attempt per replica). Slow attempts are hedged within the hedge budget and
the first valid answer wins; losing attempts are cancelled. When every attempt fails the router
returns `504` (all timed out), `503` (no replica reachable) or `502` (`{"detail": "upstream error"}`).
`GET /v1/upstream/stats` reports requests, attempts, hedges, hedge wins, failovers and
exhaustions so upstream amplification is visible.

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

## Response cache

`/v1/generate` and `/v1/chat` share an in-process response cache (`app/cache.py`) wrapped around the upstream pool. Successful (200) upstream bodies are cached verbatim (including `signature`) keyed by `(prompt, max_tokens)`; errors are never cached, and concurrent identical misses share one upstream call.

| Env var                      | Default | Description                                  |
|------------------------------|---------|----------------------------------------------|
| `RESPONSE_CACHE_TTL_S`       | `60`    | Entry lifetime in seconds; `0` disables cache |
| `RESPONSE_CACHE_MAX_ENTRIES` | `1024`  | Max cached entries (LRU eviction); `0` disables cache |

## Layout

- `app/` — FastAPI gateway: generate/chat/info routes, response cache, conversation store, markdown export, request logging middleware
- `bench/` — k6 load test script and weighted workload
- `docker-compose.yml` — backend fleet service definitions
- `frontend/` — React + TypeScript playground UI (Vite, Vitest, Testing Library, Playwright)
- `Makefile` — `make services` / `make ui` / `make dev` / `make bench` / `make e2e`
- `pyproject.toml` — Python project and dependencies (uv)
- `scripts/` — helper scripts (e2e stack bootstrap)
- `tests/` — backend unit tests (pytest)
- `uv.lock` — locked Python dependencies
