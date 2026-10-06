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

## Response cache

`POST /v1/generate` responses are cached in memory. A response is cached only
when it is a 2xx JSON object with a non-empty `completion` string and a
`signature` matching `^[0-9a-f]{64}$`; the cache key is the full request
payload, so `prompt`, `max_tokens`, and any other field distinguish entries.
`POST /v1/chat` is not cached.

Concurrent requests for the same payload share a single upstream call
(single-flight). If the shared call fails or returns a non-cacheable response,
each waiter falls through and makes its own upstream call — failures are never
cached.

| Env var | Default | Description |
|---------|---------|-------------|
| `RESPONSE_CACHE_ENABLED` | `1` | Set to `0`/`false`/`no`/`off` to disable |
| `RESPONSE_CACHE_TTL_S` | `300` | Seconds a cached entry stays valid (`<= 0` disables storing) |
| `RESPONSE_CACHE_MAX_ENTRIES` | `1024` | LRU capacity (`<= 0` disables storing) |
| `RESPONSE_CACHE_COALESCE` | `1` | Enable single-flight coalescing (only when the cache is enabled) |

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
