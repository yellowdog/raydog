"""Every exception RayDog raises deliberately."""

from __future__ import annotations

from collections.abc import Sequence


class RayDogError(Exception):
    """Base class for all RayDog errors."""


class ConfigError(RayDogError):
    """A cluster specification, YAML config or name is invalid."""


class CredentialsError(RayDogError):
    """YellowDog credentials are missing or were rejected."""


class ClusterExistsError(RayDogError):
    """A live cluster with this name already exists."""


class ClusterNotFoundError(RayDogError):
    """No cluster with this name was found."""


class ClusterStateError(RayDogError):
    """The operation is not valid in the cluster's current state."""


class NodeStartError(RayDogError):
    """A node's task failed or was cancelled before it started running."""


class BuildTimeoutError(RayDogError, TimeoutError):
    """A node did not start within the build timeout."""


class TeardownError(RayDogError):
    """
    Teardown ran to the end, but some steps failed.

    Something may still be running. ``failures`` pairs a description of each step
    that failed with the exception it raised.
    """

    def __init__(self, failures: Sequence[tuple[str, Exception]]) -> None:
        self.failures: list[tuple[str, Exception]] = list(failures)
        detail = "; ".join(f"{what}: {error}" for what, error in self.failures)
        super().__init__(f"teardown incomplete ({len(self.failures)} failed): {detail}")


class TunnelError(RayDogError):
    """An SSH tunnel could not be opened."""


class PlatformError(RayDogError):
    """A call to the YellowDog platform failed."""

    def __init__(
        self,
        message: str,
        *,
        error_type: str | None = None,
        status_code: int | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.status_code = status_code
        self.retryable = retryable


class NotFoundError(PlatformError):
    """The object does not exist."""


class InvalidRequestError(PlatformError):
    """The platform rejected the request as invalid."""


class NameInUseError(InvalidRequestError):
    """The name is already taken, possibly by an object that has ended."""
