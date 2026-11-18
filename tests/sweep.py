"""
Remove everything online tests may have left behind, and prove it has gone.

    python -m tests.sweep

Cancels every live work requirement and removes every pool named ``raydog-test-…``
in the test namespace, then waits until no such pool has a live instance. Exits
non-zero if anything is still running after five minutes. Safe to run at any time.
"""

from __future__ import annotations

import sys
import time

from tests.conftest import CLEANUP_TIMEOUT_S, ONLINE_NAME_PREFIX, _setting
from yellowdog_ray.core.yellowdog import DEFAULT_API_URL, YellowDogPlatform


def main() -> int:
    key_id, key_secret = _setting("YD_API_KEY_ID"), _setting("YD_API_KEY_SECRET")
    if not (key_id and key_secret):
        print("YD_API_KEY_ID and YD_API_KEY_SECRET are needed", file=sys.stderr)
        return 2
    namespace = _setting("RAYDOG_TEST_NAMESPACE", "ray-test") or "ray-test"
    platform = YellowDogPlatform.connect(
        key_id, key_secret, _setting("YD_API_URL") or DEFAULT_API_URL
    )
    failed = False
    try:
        for wr in platform.list_work_requirements(
            namespace, name_contains=ONLINE_NAME_PREFIX, live_only=True
        ):
            print(f"cancelling work requirement {wr.name}")
            platform.cancel_work_requirement(wr.id)
        pools = platform.list_pools(
            namespace, name_contains=ONLINE_NAME_PREFIX, live_only=True
        )
        for pool in pools:
            print(f"removing pool {pool.name}")
            try:
                platform.remove_pool(pool.id)
            except Exception as e:  # keep going: the other pools matter too
                print(f"  failed: {e}", file=sys.stderr)
                failed = True
        deadline = time.monotonic() + CLEANUP_TIMEOUT_S
        remaining = {p.name: platform.live_instance_count(p.id) for p in pools}
        while any(remaining.values()) and time.monotonic() < deadline:
            time.sleep(10)
            remaining = {p.name: platform.live_instance_count(p.id) for p in pools}
        alive = {name: n for name, n in remaining.items() if n}
        if alive:
            print(f"still alive: {alive}", file=sys.stderr)
            failed = True
    finally:
        platform.close()
    print("not clean" if failed else "clean")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
