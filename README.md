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

`POST /v1/generate` caches successful (HTTP 200) upstream responses in process, keyed on `(prompt, max_tokens)`. Errors and upstream exceptions are never cached. Concurrent requests for the same uncached key share one upstream call (coalescing); if that call fails, every waiter gets the same error and nothing is stored. Cached bodies are deep-copied on store and on read so callers can't mutate each other's responses. Responses carry an `X-Cache: HIT|MISS|COALESCED` header. `/v1/chat` is not cached.

Configured via environment variables:

| Variable | Default | Meaning |
|----------|---------|---------|
| `RESPONSE_CACHE_ENABLED` | `1` | `0`/`false`/`no`/`off` disables the cache: every request is forwarded round-robin as before, with no `X-Cache` header |
| `RESPONSE_CACHE_TTL_S` | `300` | Seconds an entry stays valid |
| `RESPONSE_CACHE_MAX_ENTRIES` | `1024` | Max entries; least recently used are evicted beyond this |
| `RESPONSE_CACHE_COALESCE` | `1` | Set to `0` to send concurrent same-key misses upstream independently |

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
