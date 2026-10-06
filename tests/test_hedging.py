import asyncio
import random
import time

import httpx
import pytest

from app.config import HedgeConfig, hedge_config
from app.upstream import UpstreamPool, is_valid_completion

URLS = ["http://r1:9000", "http://r2:9000", "http://r3:9000"]


def _sig(name: str) -> str:
    return (name.encode().hex() * 64)[:64]


class FakeFleet:
    """Mock replicas keyed by host: per-replica latency, status and signature."""

    def __init__(self, latency_s=None, status=None, body=None):
        self.latency_s = latency_s or {}
        self.status = status or {}
        self.body = body or {}
        self.hits: list[str] = []
        self.cancelled: list[str] = []
        self.completed: list[str] = []
        self.inflight = 0

    def transport(self) -> httpx.MockTransport:
        async def handler(request: httpx.Request) -> httpx.Response:
            host = request.url.host
            self.hits.append(host)
            self.inflight += 1
            try:
                delay = self.latency_s.get(host, 0.0)
                if callable(delay):
                    delay = delay()
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                self.cancelled.append(host)
                raise
            finally:
                self.inflight -= 1
            self.completed.append(host)
            status = self.status.get(host, 200)
            body = self.body.get(host, {"completion": f"from {host}", "signature": _sig(host)})
            if isinstance(body, str):
                return httpx.Response(status, text=body)
            return httpx.Response(status, json=body)

        return httpx.MockTransport(handler)


def make_pool(fleet: FakeFleet, **cfg) -> UpstreamPool:
    params = {"delays_ms": (30.0, 60.0), "explore": 0.0, "deadline_s": 5.0}
    params.update(cfg)
    return UpstreamPool(urls=URLS, transport=fleet.transport(),
                        hedge=HedgeConfig(**params), rng=random.Random(0))


def run(coro):
    return asyncio.run(coro)


# -- config ---------------------------------------------------------------------


def test_hedging_enabled_by_default(monkeypatch):
    for name in ("LLM_HEDGE_ENABLED", "LLM_HEDGE_DELAYS_MS", "LLM_HEDGE_AFFINITY"):
        monkeypatch.delenv(name, raising=False)
    cfg = hedge_config()
    assert cfg.enabled is True
    assert cfg.max_attempts == len(cfg.delays_ms) + 1 >= 2
    assert cfg.attempt_timeout_s < float("inf") and cfg.deadline_s < float("inf")
    assert cfg.affinity == "off"


def test_hedge_config_from_env(monkeypatch):
    monkeypatch.setenv("LLM_HEDGE_ENABLED", "false")
    monkeypatch.setenv("LLM_HEDGE_DELAYS_MS", "100, 250, 400")
    monkeypatch.setenv("LLM_HEDGE_ATTEMPT_TIMEOUT_S", "2.5")
    monkeypatch.setenv("LLM_HEDGE_DEADLINE_S", "4")
    monkeypatch.setenv("LLM_HEDGE_AFFINITY", "Replica")
    cfg = hedge_config()
    assert cfg.enabled is False
    assert cfg.delays_ms == (100.0, 250.0, 400.0)
    assert cfg.max_attempts == 4
    assert (cfg.attempt_timeout_s, cfg.deadline_s, cfg.affinity) == (2.5, 4.0, "replica")


def test_empty_delays_means_single_attempt(monkeypatch):
    monkeypatch.setenv("LLM_HEDGE_DELAYS_MS", "")
    assert hedge_config().max_attempts == 1


@pytest.mark.parametrize("kwargs", [
    {"delays_ms": (200.0, 100.0)},
    {"delays_ms": (-1.0,)},
    {"delays_ms": tuple(float(i) for i in range(8))},
    {"attempt_timeout_s": float("inf")},
    {"deadline_s": 0},
    {"explore": 1.5},
    {"affinity": "sticky"},
])
def test_invalid_hedge_config_rejected(kwargs):
    with pytest.raises(ValueError):
        HedgeConfig(**kwargs)


def test_config_off_has_no_timeout_and_never_duplicates():
    fleet = FakeFleet(latency_s={"r1": 0.15})
    pool = UpstreamPool(urls=URLS, transport=fleet.transport(),
                        hedge=HedgeConfig(enabled=False, delays_ms=(1.0, 2.0)))

    async def go():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    status, _ = run(go())
    assert status == 200
    assert fleet.hits == ["r1"]
    assert pool._client.timeout.read is None


# -- validity -------------------------------------------------------------------


@pytest.mark.parametrize("status,body,ok", [
    (200, {"completion": "x", "signature": "ab"}, True),
    (200, {"completion": "x"}, True),
    (200, {"completion": ""}, False),
    (200, {"completion": "x", "signature": ""}, False),
    (200, {"completion": "x", "signature": None}, False),
    (200, ["not", "a", "dict"], False),
    (503, {"completion": "x"}, False),
])
def test_is_valid_completion(status, body, ok):
    assert is_valid_completion(status, body) is ok


# -- hedging --------------------------------------------------------------------


def test_fast_primary_is_not_duplicated():
    fleet = FakeFleet()
    pool = make_pool(fleet)

    async def go():
        results = [await pool.forward({"prompt": f"p{i}"}) for i in range(5)]
        await asyncio.sleep(0.1)  # past every hedge offset
        await pool.aclose()
        return results

    results = run(go())
    assert all(s == 200 for s, _ in results)
    assert len(fleet.hits) == 5
    assert pool.stats["hedges"] == 0


def test_slow_primary_is_hedged_and_loser_cancelled():
    # first attempt hangs, any later attempt answers immediately
    calls = []

    def latency():
        calls.append(1)
        return 2.0 if len(calls) == 1 else 0.0

    fleet = FakeFleet(latency_s={h: latency for h in ("r1", "r2", "r3")})
    pool = make_pool(fleet)

    async def go():
        t0 = time.monotonic()
        result = await pool.forward({"prompt": "p"})
        elapsed = time.monotonic() - t0
        await pool.aclose()
        return result, elapsed

    (status, body), elapsed = run(go())
    assert status == 200
    assert len(fleet.hits) == 2
    assert fleet.hits[0] != fleet.hits[1], "hedge goes to a different replica"
    assert body["completion"] == f"from {fleet.hits[1]}"
    assert fleet.cancelled == [fleet.hits[0]]
    assert 0.025 <= elapsed < 0.5
    assert fleet.inflight == 0
    assert pool.stats["hedges"] == 1 and pool.stats["wins_attempt_2"] == 1


def test_attempts_are_bounded_and_first_valid_wins():
    # every replica is slow; the one launched first still finishes first
    fleet = FakeFleet(latency_s={"r1": 0.3, "r2": 0.3, "r3": 0.3})
    pool = make_pool(fleet, delays_ms=(20.0, 40.0))

    async def go():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    status, body = run(go())
    assert status == 200
    assert len(fleet.hits) == 3 == pool.hedge.max_attempts
    assert sorted(fleet.hits) == ["r1", "r2", "r3"]
    assert body["completion"] == f"from {fleet.hits[0]}"
    assert sorted(fleet.cancelled) == sorted(fleet.hits[1:])
    assert fleet.inflight == 0


def test_attempt_budget_can_reuse_a_replica():
    fleet = FakeFleet(latency_s={"r1": 0.3})
    pool = UpstreamPool(urls=URLS[:1], transport=fleet.transport(),
                        hedge=HedgeConfig(delays_ms=(10.0, 20.0, 30.0), explore=0.0))

    async def go():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert run(go())[0] == 200
    assert fleet.hits == ["r1"] * 4


def test_server_error_triggers_immediate_retry_elsewhere():
    fleet = FakeFleet(status={"r1": 503, "r2": 503, "r3": 503},
                      body={"r1": {"detail": "overloaded"}, "r2": {"detail": "overloaded"},
                            "r3": {"detail": "overloaded"}})
    pool = make_pool(fleet, delays_ms=(1000.0, 2000.0))

    async def go():
        # every replica except the first one hit becomes healthy
        async def flip():
            while len(fleet.hits) < 1:
                await asyncio.sleep(0)
            for host in URLS:
                h = httpx.URL(host).host
                if h != fleet.hits[0]:
                    fleet.status[h] = 200
                    fleet.body.pop(h, None)

        flipper = asyncio.create_task(flip())
        t0 = time.monotonic()
        result = await pool.forward({"prompt": "p"})
        elapsed = time.monotonic() - t0
        await flipper
        await pool.aclose()
        return result, elapsed

    (status, _), elapsed = run(go())
    assert status == 200
    assert len(fleet.hits) == 2 and fleet.hits[0] != fleet.hits[1]
    assert elapsed < 0.5, "retry must not wait for the 1s hedge delay"
    assert pool.stats["retries"] == 1 and pool.stats["failures"] == 1


def test_malformed_200_is_not_accepted():
    fleet = FakeFleet(body={"r1": {"completion": ""}, "r2": {"completion": ""}, "r3": "not json"})
    fleet.body["r2"] = {"completion": "good", "signature": _sig("r2")}
    pool = make_pool(fleet, delays_ms=(1000.0, 2000.0))

    async def go():
        results = [await pool.forward({"prompt": f"p{i}"}) for i in range(6)]
        await pool.aclose()
        return results

    for status, body in run(go()):
        assert status == 200
        assert body == {"completion": "good", "signature": _sig("r2")}


def test_all_attempts_fail_returns_last_upstream_error():
    fleet = FakeFleet(status={h: 503 for h in ("r1", "r2", "r3")},
                      body={h: {"detail": "model overloaded"} for h in ("r1", "r2", "r3")})
    pool = make_pool(fleet)

    async def go():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert run(go()) == (503, {"detail": "model overloaded"})
    assert len(fleet.hits) == pool.hedge.max_attempts
    assert pool.stats["exhausted"] == 1


def test_client_error_is_returned_without_duplicates():
    fleet = FakeFleet(status={h: 422 for h in ("r1", "r2", "r3")},
                      body={h: {"detail": "bad request"} for h in ("r1", "r2", "r3")})
    pool = make_pool(fleet)

    async def go():
        result = await pool.forward({"prompt": "p"})
        await asyncio.sleep(0.1)
        await pool.aclose()
        return result

    assert run(go()) == (422, {"detail": "bad request"})
    assert len(fleet.hits) == 1


def test_transport_error_is_retried():
    calls = []

    async def handler(request):
        calls.append(request.url.host)
        if len(calls) == 1:
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(200, json={"completion": "ok", "signature": _sig("x")})

    pool = UpstreamPool(urls=URLS, transport=httpx.MockTransport(handler),
                        hedge=HedgeConfig(delays_ms=(1000.0, 2000.0), explore=0.0),
                        rng=random.Random(0))

    async def go():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert run(go())[0] == 200
    assert len(calls) == 2 and calls[0] != calls[1]


def test_deadline_bounds_total_time_and_cleans_up():
    fleet = FakeFleet(latency_s={h: 30.0 for h in ("r1", "r2", "r3")})
    pool = make_pool(fleet, delays_ms=(10.0, 20.0), deadline_s=0.2)

    async def go():
        t0 = time.monotonic()
        result = await pool.forward({"prompt": "p"})
        elapsed = time.monotonic() - t0
        await pool.aclose()
        return result, elapsed

    (status, _), elapsed = run(go())
    assert status == 504
    assert elapsed < 1.0
    assert len(fleet.cancelled) == 3 and fleet.inflight == 0


def test_caller_cancellation_cancels_inflight_attempts():
    fleet = FakeFleet(latency_s={h: 30.0 for h in ("r1", "r2", "r3")})
    pool = make_pool(fleet, delays_ms=(10.0, 20.0))

    async def go():
        task = asyncio.create_task(pool.forward({"prompt": "p"}))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await pool.aclose()

    run(go())
    assert len(fleet.hits) == 3
    assert len(fleet.cancelled) == 3 and fleet.inflight == 0


def test_measured_slow_replica_stops_being_primary():
    """Latency is learned at runtime, including from cancelled (censored) attempts."""
    fleet = FakeFleet(latency_s={"r2": 0.5})
    pool = make_pool(fleet, delays_ms=(30.0, 60.0))

    async def go():
        for i in range(30):
            await pool.forward({"prompt": f"warm{i}"})
        fleet.hits.clear()
        before = pool.stats["hedges"]
        for i in range(30):
            await pool.forward({"prompt": f"p{i}"})
        await pool.aclose()
        return pool.stats["hedges"] - before

    hedges = run(go())
    assert "r2" not in fleet.hits
    assert hedges == 0
    lat = pool.replica_latency_ms()
    assert lat["http://r2:9000"] > lat["http://r1:9000"]
    assert lat["http://r2:9000"] > lat["http://r3:9000"]


class _AlwaysExplore(random.Random):
    """Forces the first attempt onto replica 0."""

    def random(self):
        return 0.0

    def randrange(self, *args, **kwargs):
        return 0


def test_unmeasured_replica_with_stuck_attempt_is_not_rehedged():
    """Cold start: a replica with no completed sample yet is scored by its pending attempts."""
    fleet = FakeFleet(latency_s={"r1": 1.0})
    pool = UpstreamPool(urls=URLS, transport=fleet.transport(),
                        hedge=HedgeConfig(delays_ms=(30.0, 60.0, 90.0), explore=1.0),
                        rng=_AlwaysExplore())
    pool._ewma_ms = [None, 20.0, 20.0]

    async def go():
        t0 = time.monotonic()
        result = await pool.forward({"prompt": "p"})
        elapsed = time.monotonic() - t0
        await pool.aclose()
        return result, elapsed

    (status, body), elapsed = run(go())
    assert status == 200
    assert fleet.hits[0] == "r1"
    assert fleet.hits.count("r1") == 1
    assert body["completion"] != "from r1"
    assert elapsed < 0.5
    assert pool._inflight == [{}, {}, {}]


def test_signature_is_passed_through_untouched():
    body = {"completion": "c", "signature": _sig("orig"), "id": "cmpl-1", "usage": {"t": 1}}
    fleet = FakeFleet(body={h: body for h in ("r1", "r2", "r3")})
    pool = make_pool(fleet)

    async def go():
        result = await pool.forward({"prompt": "p"})
        await pool.aclose()
        return result

    assert run(go()) == (200, body)


# -- per-prompt affinity ----------------------------------------------------------
# FakeFleet replicas sign with their own host name, i.e. signatures differ across
# replicas, which is the case LLM_HEDGE_AFFINITY=replica exists for.


def test_affinity_pins_prompt_to_first_replica_and_signature_is_stable():
    fleet = FakeFleet()
    pool = make_pool(fleet, affinity="replica")

    async def go():
        a = [await pool.forward({"prompt": "same", "max_tokens": 8}) for _ in range(10)]
        others = [await pool.forward({"prompt": f"other{i}", "max_tokens": 8}) for i in range(20)]
        await pool.aclose()
        return a, others

    same, others = run(go())
    assert len({b["signature"] for _, b in same}) == 1
    assert len({b["signature"] for _, b in others}) > 1, "different prompts still spread"


def test_affinity_handles_simultaneous_same_prompt_requests():
    fleet = FakeFleet(latency_s={h: 0.05 for h in ("r1", "r2", "r3")})
    pool = make_pool(fleet, affinity="replica", delays_ms=(10.0, 20.0))

    async def go():
        results = await asyncio.gather(*[pool.forward({"prompt": "same"}) for _ in range(12)])
        await pool.aclose()
        return results

    results = run(go())
    assert all(s == 200 for s, _ in results)
    assert len({b["signature"] for _, b in results}) == 1
    assert len(set(fleet.hits)) == 1, "hedges for a pinned prompt stay on its replica"


def test_affinity_hedges_on_the_pinned_replica():
    calls = []

    def latency():
        calls.append(1)
        return 2.0 if len(calls) == 1 else 0.0

    fleet = FakeFleet(latency_s={h: latency for h in ("r1", "r2", "r3")})
    pool = make_pool(fleet, affinity="replica")

    async def go():
        result = await pool.forward({"prompt": "same"})
        await pool.aclose()
        return result

    status, body = run(go())
    assert status == 200
    assert len(fleet.hits) == 2 and fleet.hits[0] == fleet.hits[1]
    assert body["signature"] == _sig(fleet.hits[0])


def test_affinity_reservation_released_when_all_attempts_fail():
    fleet = FakeFleet(status={h: 503 for h in ("r1", "r2", "r3")},
                      body={h: {"detail": "down"} for h in ("r1", "r2", "r3")})
    pool = make_pool(fleet, affinity="replica")

    async def go():
        result = await pool.forward({"prompt": "same"})
        await pool.aclose()
        return result

    assert run(go())[0] == 503
    assert len(pool._affinity) == 0


def test_affinity_table_is_bounded():
    fleet = FakeFleet()
    pool = make_pool(fleet, affinity="replica", affinity_max_keys=3)

    async def go():
        for i in range(10):
            await pool.forward({"prompt": f"p{i}"})
        await pool.aclose()

    run(go())
    assert len(pool._affinity) == 3
