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

## Routing

`/v1/generate` and `/v1/chat` requests are spread across `LLM_SERVICE_URLS`
(default: the three local replicas) by `app/balancer.py`. The default
`latency` strategy keeps an EWMA of each replica's measured response time and
sends each request to the replica with the lowest
`ewma * (1 + outstanding_weight * in_flight)`. It never permanently excludes a
replica:

- **Cold start** — replicas with no measurement yet are tried first.
- **Exploration / recovery** — a replica not sent any traffic for
  `ROUTER_PROBE_INTERVAL_S` gets the next request, so slow or failing replicas
  are re-measured and win traffic back once they recover.
- **Errors** — a 5xx or transport error is recorded as at least
  `ROUTER_ERROR_PENALTY_S` of latency; the response/error is still passed
  through unchanged (no retries or hedging).

| Variable                    | Default   | Meaning |
|-----------------------------|-----------|---------|
| `ROUTER_STRATEGY`           | `latency` | `latency`, or `round_robin` for the original strict rotation |
| `ROUTER_EWMA_ALPHA`         | `0.3`     | Weight of the newest latency sample (0 < alpha <= 1) |
| `ROUTER_PROBE_INTERVAL_S`   | `5.0`     | Max seconds a replica can go without traffic before it is probed |
| `ROUTER_ERROR_PENALTY_S`    | `5.0`     | Minimum latency sample recorded for a failed request |
| `ROUTER_OUTSTANDING_WEIGHT` | `1.0`     | How strongly in-flight requests raise a replica's cost (0 = latency only) |

Restart the router after changing these.

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

## Layout

- `app/` — FastAPI gateway: generate/chat/info routes, latency-aware upstream balancer, conversation store, markdown export, request logging middleware
- `bench/` — k6 load test script and weighted workload
- `docker-compose.yml` — backend fleet service definitions
- `frontend/` — React + TypeScript playground UI (Vite, Vitest, Testing Library, Playwright)
- `Makefile` — `make services` / `make ui` / `make dev` / `make bench` / `make e2e`
- `pyproject.toml` — Python project and dependencies (uv)
- `scripts/` — helper scripts (e2e stack bootstrap)
- `tests/` — backend unit tests (pytest)
- `uv.lock` — locked Python dependencies
