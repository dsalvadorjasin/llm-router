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

`app/upstream.py` picks a backend replica per request. Configured via env (read in `app/config.py`):

| Variable | Default | Meaning |
|---|---|---|
| `LLM_SERVICE_URLS` | `localhost:9001-9003` | Comma-separated replica URLs |
| `ROUTER_STRATEGY` | `latency` | `latency` (EWMA latency x in-flight, power-of-two-choices) or `round_robin` |
| `ROUTER_EWMA_ALPHA` | `0.2` | Weight of the newest latency sample |
| `ROUTER_EWMA_HALF_LIFE_S` | `2.0` | Stale EWMA history halves in weight after this long without a sample |
| `ROUTER_EXPLORE_RATE` | `0.02` | Fraction of requests sent to a random replica |
| `ROUTER_PROBE_INTERVAL_S` | `5.0` | A replica not picked for this long gets the next request (re-discovers recovered replicas) |
| `ROUTER_CONNECT_TIMEOUT_S` | `1.0` | Upstream connect timeout |
| `ROUTER_ATTEMPT_TIMEOUT_S` | `8.0` | Hard cap per upstream attempt |
| `ROUTER_MAX_ATTEMPTS` | `2` | Attempts per request; a failed/timed-out/5xx attempt is retried on a different replica |

Replicas without a sample are probed one request at a time at cold start; errors count as a timeout-sized latency sample, and the first success afterwards resets the replica's EWMA.

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
