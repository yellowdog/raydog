"""
Fixtures shared by all tests.

The contract tests run against two platforms through the same ``Harness``: the
in-memory fake (always) and the real YellowDog platform (only when asked for with
``-m online`` or ``-m online_instances``, and only if credentials are present).
"""

from __future__ import annotations

import functools
import os
import random
import string
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import TypeVar

import pytest
from dotenv import dotenv_values

from yellowdog_ray.core.clock import Clock, SystemClock
from yellowdog_ray.core.platform import Platform, PoolSpec
from yellowdog_ray.testing import FakePlatform

REPO_ROOT = Path(__file__).resolve().parents[1]
ONLINE_NAME_PREFIX = "raydog-test-"
CLEANUP_TIMEOUT_S = 300.0

T = TypeVar("T")


def _setting(name: str, default: str | None = None) -> str | None:
    """An online-test setting: the environment first, then the repo-root .env."""
    # An unset CI variable arrives as an empty string: treat that as not set
    return (
        os.environ.get(name) or dotenv_values(REPO_ROOT / ".env").get(name) or default
    )


@dataclass
class Harness:
    """A platform plus what a test needs to use it without knowing which it is."""

    platform: Platform
    namespace: str
    template: str
    images_id: str | None
    userdata: str | None
    is_fake: bool
    image_family: str | None = None  # namespace/name of a family that exists
    work_requirements: list[str] = field(default_factory=list)
    pools: list[str] = field(default_factory=list)
    names: list[str] = field(default_factory=list)

    @property
    def clock(self) -> Clock:
        """The clock that drives this platform: the fake's own, or the real one."""
        if isinstance(self.platform, FakePlatform):
            return self.platform.clock
        return SystemClock()

    def name(self, label: str) -> str:
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        name = f"{ONLINE_NAME_PREFIX}{label}-{suffix}"
        self.names.append(name)
        return name

    def pool_spec(
        self, name: str, target: int = 0, max_nodes: int = 1, **overrides: object
    ) -> PoolSpec:
        settings: dict[str, object] = {
            "name": name,
            "namespace": self.namespace,
            "template": self.template,
            "worker_tag": name,
            "target_instances": target,
            "max_nodes": max_nodes,
            "images_id": self.images_id,
            "userdata": self.userdata,
            "tag": "raydog-test",
            # A backstop: nothing a test starts outlives this
            "idle_node_shutdown": timedelta(minutes=10),
            "idle_pool_shutdown": timedelta(minutes=30),
        }
        settings.update(overrides)
        return PoolSpec(**settings)  # type: ignore[arg-type]

    def wait_until(
        self,
        probe: Callable[[], T],
        accept: Callable[[T], bool],
        timeout_s: float,
        step_s: float = 1.0,
    ) -> T:
        """
        Poll ``probe`` until ``accept`` likes its value, or fail showing the last.

        Against the fake this advances its clock; against the real platform it sleeps.
        """
        waited = 0.0
        value = probe()
        while not accept(value):
            if waited >= timeout_s:
                pytest.fail(f"still {value!r} after {timeout_s:.0f}s")
            self._pause(step_s)
            waited += step_s
            value = probe()
        return value

    def _pause(self, seconds: float) -> None:
        if isinstance(self.platform, FakePlatform):
            self.platform.clock.advance(seconds)
        else:
            time.sleep(seconds)

    def clean_up(self) -> list[str]:
        """
        Remove everything the test created; return descriptions of leftovers.

        Works from the names the test generated as well as the IDs it recorded, so
        that something created by a call the test expected to fail is still found.
        Every step is attempted whatever the earlier ones did.
        """
        problems: list[str] = []
        p = self.platform

        def attempt(what: str, action: Callable[[], T]) -> T | None:
            try:
                return action()
            except Exception as e:
                problems.append(f"{what}: {type(e).__name__}: {e}")
                return None

        work_requirements = set(self.work_requirements)
        pools = set(self.pools)
        for name in self.names:
            found_wrs = attempt(
                f"list work requirements like {name}",
                functools.partial(
                    p.list_work_requirements, self.namespace, name, live_only=True
                ),
            )
            work_requirements.update(w.id for w in found_wrs or [])
            found_pools = attempt(
                f"list pools like {name}",
                functools.partial(p.list_pools, self.namespace, name, live_only=True),
            )
            pools.update(x.id for x in found_pools or [])

        for wr_id in work_requirements:
            attempt(
                f"cancel {wr_id}", functools.partial(p.cancel_work_requirement, wr_id)
            )
        for pool_id in pools:
            attempt(f"remove {pool_id}", functools.partial(p.remove_pool, pool_id))

        waited = 0.0
        for pool_id in pools:
            while True:
                count = attempt(
                    f"count instances of {pool_id}",
                    functools.partial(p.live_instance_count, pool_id),
                )
                if not count:
                    break
                if waited >= CLEANUP_TIMEOUT_S:
                    problems.append(f"{pool_id} still has {count} live instance(s)")
                    break
                self._pause(5)
                waited += 5
        return problems


def _fake_harness() -> Harness:
    platform = FakePlatform()
    platform.add_image_family("images", "ubuntu-amd64")
    return Harness(
        platform=platform,
        namespace="ns",
        template="ns/template",
        images_id=None,
        userdata=None,
        is_fake=True,
        image_family="images/ubuntu-amd64",
    )


def _real_harness() -> Harness:
    from yellowdog_ray.core.yellowdog import DEFAULT_API_URL, YellowDogPlatform

    key_id, key_secret = _setting("YD_API_KEY_ID"), _setting("YD_API_KEY_SECRET")
    template = _setting("RAYDOG_TEST_WORKER_TEMPLATE")
    missing = [
        name
        for name, value in [
            ("YD_API_KEY_ID", key_id),
            ("YD_API_KEY_SECRET", key_secret),
            ("RAYDOG_TEST_WORKER_TEMPLATE", template),
        ]
        if not value
    ]
    if missing:
        pytest.skip("online tests need " + ", ".join(missing))
    assert key_id and key_secret and template
    userdata_file = _setting("RAYDOG_TEST_USERDATA_FILE")
    return Harness(
        platform=YellowDogPlatform.connect(
            key_id,
            key_secret,
            _setting("YD_API_URL", DEFAULT_API_URL) or DEFAULT_API_URL,
        ),
        namespace=_setting("RAYDOG_TEST_NAMESPACE", "ray-test") or "ray-test",
        template=template,
        images_id=_setting("RAYDOG_TEST_IMAGES_ID"),
        userdata=Path(userdata_file).read_text() if userdata_file else None,
        is_fake=False,
        image_family=_setting("RAYDOG_TEST_IMAGE_FAMILY"),
    )


def _harness(kind: str) -> Iterator[Harness]:
    harness = _fake_harness() if kind == "fake" else _real_harness()
    try:
        yield harness
    finally:
        problems = harness.clean_up()
        try:
            harness.platform.close()
        finally:
            if problems:
                pytest.fail("cleanup left resources behind: " + "; ".join(problems))


@pytest.fixture(params=["fake", pytest.param("yellowdog", marks=pytest.mark.online)])
def harness(request: pytest.FixtureRequest) -> Iterator[Harness]:
    """Both platforms, for tests that start no instances."""
    yield from _harness(request.param)


@pytest.fixture(
    params=["fake", pytest.param("yellowdog", marks=pytest.mark.online_instances)]
)
def instance_harness(request: pytest.FixtureRequest) -> Iterator[Harness]:
    """Both platforms, for tests that start instances on the real one."""
    yield from _harness(request.param)
