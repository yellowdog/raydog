"""Waiting for something to become true, without waiting forever."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import TypeVar

from yellowdog_ray.core.clock import Clock

T = TypeVar("T")


def poll(
    clock: Clock,
    probe: Callable[[], T],
    done: Callable[[T], bool],
    timeout: timedelta,
    interval: timedelta = timedelta(seconds=5),
) -> T:
    """
    Call ``probe`` until ``done`` accepts its value, and return that value.

    Raises ``TimeoutError`` once ``timeout`` has passed. The probe is always called
    at least once, and once more exactly at the deadline, never after it.
    """
    deadline = clock.now() + timeout
    while True:
        value = probe()
        if done(value):
            return value
        remaining = (deadline - clock.now()).total_seconds()
        if remaining <= 0:
            raise TimeoutError(f"timed out after {timeout}; last value: {value!r}")
        clock.sleep(min(interval.total_seconds(), remaining))
