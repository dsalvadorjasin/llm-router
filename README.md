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

## Latency strategies

Requests flow through the response cache and single-flight coalescer first when the
cache is enabled. A cache miss is dispatched through `UpstreamPool`: the latency
selector chooses a primary replica using latency EWMA multiplied by in-flight
attempts, with cold-start and idle probes, exploration, and power-of-two choices.
If the primary has not returned a well-formed completion by the hedge delay, a
later attempt normally uses the lowest-scored replica that has not failed,
including replicas already tried by this request. In-flight attempts increase a
replica's score; ties prefer untried replicas and then the least recently picked.
Set `HEDGE_REUSE_REPLICAS=0` to retain the behavior of excluding tried replicas.
The first attempt still uses the latency selector, and pinned prompts continue to
use their pinned replica. The first well-formed 2xx response wins and outstanding
attempts are cancelled. Failures fail over immediately; 4xx responses are
returned without a retry. Repeated prompts with divergent backend signatures
remain pinned to the first replica that served them.

`POST /v1/generate` caches only 2xx JSON objects with a non-empty
`completion` and a 64-character hexadecimal `signature`; the full request
payload forms the cache key. Concurrent misses for the same payload share one
upstream call. Failed or non-cacheable responses are never cached, and
`POST /v1/chat` is not cached.

All configuration is read from `app/config.py`:

| Environment variable | Default | Meaning |
|---|---:|---|
| `LLM_SERVICE_URLS` | `http://localhost:9001,http://localhost:9002,http://localhost:9003` | Comma-separated upstream replica URLs |
| `ROUTER_STRATEGY` | `latency` | `latency` (EWMA × in-flight, power-of-two choices) or `round_robin` |
| `ROUTER_EWMA_ALPHA` | `0.2` | Weight of the newest latency sample |
| `ROUTER_EWMA_HALF_LIFE_S` | `2.0` | Time for stale EWMA history to lose half its weight |
| `ROUTER_EXPLORE_RATE` | `0.02` | Fraction of requests sent to a random replica |
| `ROUTER_PROBE_INTERVAL_S` | `5.0` | Idle interval after which a replica is probed again |
| `ROUTER_CONNECT_TIMEOUT_S` | `1.0` | Upstream connection timeout |
| `ROUTER_ATTEMPT_TIMEOUT_S` | `8.0` | Hard cap for one upstream attempt |
| `ROUTER_MAX_ATTEMPTS` | `2` | Maximum sequential attempts when hedging is disabled |
| `HEDGE_ENABLED` | `1` | Enable hedged dispatch |
| `HEDGE_DELAY_MS` | `150` | Cold-start hedge delay in milliseconds |
| `HEDGE_ADAPTIVE` | `1` | Derive hedge delay from recent successful latency samples |
| `HEDGE_PERCENTILE` | `0.5` | Percentile used for the adaptive hedge delay |
| `HEDGE_MIN_DELAY_MS` | `50` | Minimum adaptive hedge delay |
| `HEDGE_MAX_DELAY_MS` | `1000` | Maximum adaptive hedge delay |
| `HEDGE_MIN_SAMPLES` | `5` | Samples required before adaptive delay is used |
| `HEDGE_WINDOW` | `512` | Number of successful latency samples retained |
| `HEDGE_MAX_ATTEMPTS` | `3` | Maximum total attempts in hedged mode |
| `HEDGE_REUSE_REPLICAS` | `1` | Let later attempts reuse the best-scored non-failed replica |
| `HEDGE_SIGNATURE_GUARD` | `1` | Reject a repeated prompt's response when its signature diverges |
| `HEDGE_SIGNATURE_MEMO_SIZE` | `10000` | Maximum repeated-prompt signatures retained |
| `RESPONSE_CACHE_ENABLED` | `1` | Enable response caching |
| `RESPONSE_CACHE_TTL_S` | `300` | Cache entry lifetime in seconds |
| `RESPONSE_CACHE_MAX_ENTRIES` | `1024` | Maximum number of cached responses |
| `RESPONSE_CACHE_COALESCE` | `1` | Coalesce concurrent cache misses for the same request |
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
