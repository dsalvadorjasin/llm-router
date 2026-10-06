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

`/v1/generate` and `/v1/chat` go through `UpstreamPool` (`app/upstream.py`). Each request is sent to the
replica with the lowest score (latency EWMA x in-flight attempts; unmeasured replicas get the pool median as a
neutral prior, idle replicas' estimates relax back toward the median so no replica is excluded for good). If no
valid response (2xx JSON with non-empty `completion` and `signature`) arrives within the hedge delay, a duplicate
goes to another replica; the first valid response wins and the rest are cancelled. Failed attempts fail over
immediately. Backend signatures are deterministic across replicas; if the router ever sees a replica return a
different signature for a repeated `(prompt, max_tokens)`, it rejects that response and pins the prompt to the
replica that first served it.

| Env var | Default | Meaning |
|---------|---------|---------|
| `HEDGE_ENABLED` | `1` | `0` = plain round-robin with failover on 5xx/transport errors |
| `HEDGE_DELAY_MS` | `400` | Hedge delay before enough samples exist (or always, if not adaptive) |
| `HEDGE_ADAPTIVE` | `1` | Use a percentile of recent successful attempt latencies as the delay |
| `HEDGE_PERCENTILE` | `0.8` | Percentile for the adaptive delay |
| `HEDGE_MIN_DELAY_MS` / `HEDGE_MAX_DELAY_MS` | `50` / `1000` | Clamp for the delay |
| `HEDGE_MAX_ATTEMPTS` | `3` | Max attempts per request (primary + hedges + failovers) |
| `UPSTREAM_CONNECT_TIMEOUT_S` / `UPSTREAM_ATTEMPT_TIMEOUT_S` | `2` / `10` | Per-attempt timeouts |
| `HEDGE_SIGNATURE_GUARD` | `1` | Reject responses whose signature differs from the one first seen for that prompt |

Less common knobs (`HEDGE_MIN_SAMPLES`, `HEDGE_WINDOW`, `HEDGE_EWMA_ALPHA`, `HEDGE_IDLE_DECAY_HALF_LIFE_S`,
`HEDGE_SIGNATURE_MEMO_SIZE`) are in `app/config.py`.

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
