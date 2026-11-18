from __future__ import annotations

from datetime import timedelta

import pytest

from yellowdog_ray.core.wait import poll
from yellowdog_ray.testing import FakeClock


def test_returns_at_once_when_already_done() -> None:
    clock = FakeClock()
    start = clock.now()
    assert poll(clock, lambda: 7, lambda v: v == 7, timedelta(seconds=60)) == 7
    assert clock.now() == start


def test_returns_the_value_that_satisfied_done() -> None:
    clock = FakeClock()
    start = clock.now()
    values = iter(range(100))
    result = poll(
        clock, lambda: next(values), lambda v: v >= 3, timedelta(seconds=60),
        interval=timedelta(seconds=5),
    )  # fmt: skip
    assert result == 3
    assert (clock.now() - start).total_seconds() == 15


def test_times_out_exactly_at_the_deadline_showing_the_last_value() -> None:
    clock = FakeClock()
    start = clock.now()
    probes: list[float] = []

    def probe() -> str:
        probes.append((clock.now() - start).total_seconds())
        return "PENDING"

    with pytest.raises(TimeoutError, match="PENDING"):
        poll(clock, probe, lambda v: v == "DONE", timedelta(seconds=12),
             interval=timedelta(seconds=5))  # fmt: skip
    assert (clock.now() - start).total_seconds() == 12
    assert probes == [0, 5, 10, 12]  # one last look at the deadline, never beyond it


def test_zero_timeout_probes_once() -> None:
    clock = FakeClock()
    calls = []
    with pytest.raises(TimeoutError):
        poll(clock, lambda: calls.append(1), lambda v: False, timedelta(0))
    assert len(calls) == 1


def test_a_failing_probe_propagates() -> None:
    def probe() -> int:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        poll(FakeClock(), probe, lambda v: True, timedelta(seconds=5))
