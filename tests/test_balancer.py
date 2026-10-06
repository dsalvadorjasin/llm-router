import random

import pytest

from app.balancer import LatencyAwareBalancer, RoundRobinBalancer


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _balancer(size=3, **kwargs) -> tuple[LatencyAwareBalancer, FakeClock]:
    clock = FakeClock()
    kwargs.setdefault("probe_interval_s", 5.0)
    return LatencyAwareBalancer(size, clock=clock, rng=random.Random(0), **kwargs), clock


def _serve(balancer, clock, latencies, ok=True, step=0.01) -> int:
    clock.now += step
    index = balancer.acquire()
    balancer.release(index, latencies[index], ok)
    return index


def test_round_robin_balancer_cycles():
    rr = RoundRobinBalancer(3)
    assert [rr.acquire() for _ in range(7)] == [0, 1, 2, 0, 1, 2, 0]


def test_cold_start_measures_every_replica_once():
    balancer, clock = _balancer()
    latencies = [0.1, 0.1, 0.1]
    assert [_serve(balancer, clock, latencies) for _ in range(3)] == [0, 1, 2]
    assert all(s.ewma_s == pytest.approx(0.1) for s in balancer.stats)


def test_concurrent_cold_start_spreads_before_any_response():
    balancer, _ = _balancer()
    assert [balancer.acquire() for _ in range(3)] == [0, 1, 2]


def test_prefers_lowest_latency_replica():
    balancer, clock = _balancer()
    latencies = [0.5, 0.1, 1.3]
    picks = [_serve(balancer, clock, latencies) for _ in range(100)]
    assert picks[3:].count(1) == 97


def test_ewma_tracks_latency_changes():
    balancer, clock = _balancer(alpha=0.5)
    latencies = [0.1, 0.2, 0.3]
    for _ in range(3):
        _serve(balancer, clock, latencies)
    balancer.release(balancer.acquire(), 0.9, True)
    assert balancer.stats[0].ewma_s == pytest.approx(0.5)


def test_outstanding_requests_shift_load_off_busy_replica():
    balancer, clock = _balancer()
    for _ in range(3):
        _serve(balancer, clock, [0.1, 0.1, 0.25])
    held = [balancer.acquire() for _ in range(5)]
    assert sorted(held[:4]) == [0, 0, 1, 1]
    assert held[4] == 2
    assert [s.outstanding for s in balancer.stats] == [2, 2, 1]


def test_outstanding_weight_zero_ignores_in_flight_load():
    balancer, clock = _balancer(outstanding_weight=0.0)
    for _ in range(3):
        _serve(balancer, clock, [0.1, 0.2, 0.3])
    assert [balancer.acquire() for _ in range(5)] == [0, 0, 0, 0, 0]


def test_release_never_drives_outstanding_negative():
    balancer, _ = _balancer(size=1)
    balancer.release(0, 0.1, True)
    assert balancer.stats[0].outstanding == 0


def test_errors_are_penalised_and_traffic_moves_away():
    balancer, clock = _balancer(error_penalty_s=5.0)
    for _ in range(3):
        _serve(balancer, clock, [0.1, 0.2, 0.3])
    clock.now += 0.01
    index = balancer.acquire()
    assert index == 0
    balancer.release(index, 0.01, False)
    assert balancer.stats[0].ewma_s > 1.0
    assert _serve(balancer, clock, [0.1, 0.2, 0.3]) == 1


def test_idle_replica_is_probed_after_interval():
    balancer, clock = _balancer(probe_interval_s=5.0)
    latencies = [0.1, 0.1, 1.3]
    for _ in range(3):
        _serve(balancer, clock, latencies)
    picks = [_serve(balancer, clock, latencies, step=0.1) for _ in range(120)]
    probe_positions = [i for i, p in enumerate(picks) if p == 2]
    assert len(probe_positions) == 2
    assert all(b - a >= 49 for a, b in zip(probe_positions, probe_positions[1:]))


def test_no_replica_is_permanently_excluded_even_after_failures():
    balancer, clock = _balancer(probe_interval_s=1.0, error_penalty_s=30.0)
    for _ in range(3):
        _serve(balancer, clock, [0.1, 0.1, 0.1])
    for _ in range(5):
        clock.now += 0.01
        index = balancer.acquire()
        balancer.release(index, 0.1, ok=index != 2)
    assert balancer.stats[2].ewma_s > 5.0
    picks = [_serve(balancer, clock, [0.1, 0.1, 0.1], step=0.05) for _ in range(200)]
    assert set(picks) == {0, 1, 2}
    assert picks.count(2) >= 9


def test_recovered_replica_wins_traffic_back():
    balancer, clock = _balancer(probe_interval_s=1.0, alpha=0.5)
    slow = [0.1, 0.4, 2.0]
    for _ in range(3):
        _serve(balancer, clock, slow)
    for _ in range(50):
        _serve(balancer, clock, slow, step=0.05)
    assert balancer.stats[2].ewma_s > 1.0
    recovered = [0.4, 0.4, 0.05]
    picks = [_serve(balancer, clock, recovered, step=0.05) for _ in range(200)]
    tail = picks[-50:]
    assert tail.count(2) >= 44
    assert tail.count(0) <= 3 and tail.count(1) <= 3


def test_rejects_invalid_parameters():
    with pytest.raises(ValueError):
        LatencyAwareBalancer(0)
    with pytest.raises(ValueError):
        LatencyAwareBalancer(3, alpha=0)
