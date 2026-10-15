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

`/v1/generate` and `/v1/chat` go through `UpstreamPool` (`app/upstream.py`). Each request goes to the fastest group of replicas, ranked by median recent latency and rotated round-robin within that group. If it hasn't answered within the hedge delay (the fastest replica's median latency, capped at `LLM_HEDGE_DELAY_MS`), a backup request goes to the next best replica. The first non-5xx answer wins and the other requests are cancelled. A 5xx or connection error moves to the next replica right away. Every 50th request may send a background probe to a replica that hasn't answered in 5s, so a recovered replica can rejoin.

| Variable | Default | Meaning |
|----------|---------|---------|
| `LLM_SERVICE_URLS` | `localhost:9001-9003` | Comma-separated backend URLs |
| `LLM_HEDGING` | `1` | `0` turns hedging off and goes back to plain round-robin |
| `LLM_HEDGE_DELAY_MS` | `150` | Longest wait before sending a backup request |
| `LLM_HEDGE_MAX_ATTEMPTS` | `3` | Most backend calls per request |
| `LLM_REQUEST_TIMEOUT_S` | `30` | Total time limit per request (returns 504 when exceeded) |

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
