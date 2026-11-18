"""Showing RayDog's log lines on a terminal: for `raydog -v` and for `ray up`."""

from __future__ import annotations

import logging
import os
import sys

PACKAGE = "yellowdog_ray"
FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
ENV_LOG_LEVEL = "RAYDOG_LOG_LEVEL"

LOG = logging.getLogger(__name__)


def stderr_handler() -> logging.Handler:
    """A handler that writes RayDog's lines to stderr, one per line, timestamped."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(FORMAT))
    return handler


class _FromEnvironment(logging.StreamHandler):  # type: ignore[type-arg]
    """The handler added for RAYDOG_LOG_LEVEL, recognisable so it is added once."""


def show_from_environment() -> None:
    """
    If RAYDOG_LOG_LEVEL names a level (INFO, DEBUG, ...), show RayDog's lines at
    that level on stderr. For programs that set no logging up themselves, such as
    ``ray up``, whose command line sets up only Ray's own logger. Doing it again
    changes nothing; a value that is not a level is reported and ignored.
    """
    value = os.environ.get(ENV_LOG_LEVEL, "").strip()
    if not value:
        return
    level = logging.getLevelName(value.upper())
    if not isinstance(level, int):
        LOG.warning(
            "%s=%s is not a logging level (try INFO or DEBUG); ignored",
            ENV_LOG_LEVEL,
            value,
        )
        return
    package = logging.getLogger(PACKAGE)
    package.setLevel(level)
    if not any(isinstance(h, _FromEnvironment) for h in package.handlers):
        handler = _FromEnvironment(sys.stderr)
        handler.setFormatter(logging.Formatter(FORMAT))
        package.addHandler(handler)
