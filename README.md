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

## Hedged requests

`/v1/generate` and `/v1/chat` forward through `UpstreamPool` (`app/upstream.py`), which hedges by default:

- The first attempt goes to the better of two randomly sampled replicas, scored by a runtime EWMA of observed latency (cancelled attempts count as "at least this slow"; failures are penalised). A small `explore` share of first attempts goes to a random replica so scores keep updating.
- If no valid response (HTTP 200 with a non-empty `completion` and, when present, a non-empty `signature`) has arrived by each offset in `LLM_HEDGE_DELAYS_MS`, a duplicate is sent to the replica expected to answer fastest, preferring ones not yet tried for this request.
- The first valid response wins and is returned unmodified; every other in-flight attempt is cancelled. Transport errors, 5xx and malformed bodies trigger the next attempt immediately; 4xx are returned as-is without duplicates. When every attempt fails, the last upstream error is returned (504 if the deadline expires).

| Variable | Default | Meaning |
|----------|---------|---------|
| `LLM_HEDGE_ENABLED` | `true` | `false`/`0` restores the original behaviour: plain round-robin, one attempt, no timeout |
| `LLM_HEDGE_DELAYS_MS` | `180,250,350` | Offsets (ms from request start) at which duplicates launch; attempts = offsets + 1 (max 8) |
| `LLM_HEDGE_ATTEMPT_TIMEOUT_S` | `10` | Per-attempt httpx timeout |
| `LLM_HEDGE_DEADLINE_S` | `15` | Overall per-request deadline |
| `LLM_HEDGE_EXPLORE` | `0.05` | Share of first attempts sent to a random replica |
| `LLM_HEDGE_EWMA_ALPHA` | `0.2` | Weight of each new latency sample |
| `LLM_HEDGE_AFFINITY` | `off` | `replica` pins each distinct payload to the replica first chosen for it (all its hedges stay there) |
| `LLM_HEDGE_AFFINITY_MAX_KEYS` | `10000` | LRU bound on the affinity table |

Affinity is off by default because probing the three replicas with identical prompts showed identical `signature` values within and across replicas. Turn it on if replicas ever sign differently: the replica is reserved before the first await, so simultaneous requests for the same payload share it, and the reservation is dropped if every attempt fails. The table is per router process.

The default offsets come from measurements on the local fleet: healthy attempts mostly answer in ~100–170 ms, so the first hedge waits past that; the later, tighter offsets cover the ~1% of requests whose first two attempts both hit a multi-second tail.

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
