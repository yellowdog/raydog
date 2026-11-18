# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**RayDog** (`yellowdog-ray` on PyPI) runs [Ray](https://ray.io/) clusters on the [YellowDog](https://yellowdog.ai/) platform, through two front ends over one core: the **Autoscaler** (`RayDogNodeProvider`, for Ray's own `ray up`), which is the default presented to users, and the **Builder** (`RayDogCluster`, a Python API), plus the `raydog` command.

**Read [`DEVELOPMENT.md`](DEVELOPMENT.md) first.** It is the single source for setup, commands, conventions, the architecture, the rules that must stay true, what RayDog relies on YellowDog and Ray to do, testing and debugging. Keep it current: when a change teaches something about the platform or Ray that the code depends on, add it there. Releases are in [`RELEASING.md`](RELEASING.md), the test tiers and online settings in [`tests/README.md`](tests/README.md).

Version 3 is a ground-up rewrite of version 2; `CHANGELOG.md` and `docs/migration.rst` say what changed.

## Notes for working here

- `make check` must pass before anything is called done; online tests skip silently without their settings, so read pytest's summary (`-rs`) before claiming they passed.
- Install with `uv pip`, never `pip`.
- `next-version` is the integration branch and `main` always matches PyPI. Commit or push only when asked.
