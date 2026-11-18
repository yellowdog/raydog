import pytest

from yellowdog_ray import errors


def test_build_timeout_is_also_a_timeout_error() -> None:
    err = errors.BuildTimeoutError("head node did not start")
    assert isinstance(err, errors.RayDogError)
    assert isinstance(err, TimeoutError)


def test_teardown_error_lists_every_failure() -> None:
    err = errors.TeardownError(
        [
            ("pool demo_ab_h", RuntimeError("boom")),
            ("pool demo_ab_w-a", ValueError("x")),
        ]
    )
    assert len(err.failures) == 2
    assert "pool demo_ab_h" in str(err)
    assert "boom" in str(err)
    assert "pool demo_ab_w-a" in str(err)


def test_platform_error_carries_details() -> None:
    err = errors.PlatformError(
        "rate limited", error_type="TooManyRequests", status_code=429, retryable=True
    )
    assert (err.error_type, err.status_code, err.retryable) == (
        "TooManyRequests",
        429,
        True,
    )
    plain = errors.PlatformError("something")
    assert (plain.error_type, plain.status_code, plain.retryable) == (None, None, False)


@pytest.mark.parametrize(
    "cls",
    [errors.NameInUseError, errors.NotFoundError, errors.InvalidRequestError],
)
def test_specific_platform_errors_are_platform_errors(cls: type) -> None:
    assert issubclass(cls, errors.PlatformError)


@pytest.mark.parametrize(
    "name",
    [
        "ConfigError",
        "CredentialsError",
        "ClusterExistsError",
        "ClusterNotFoundError",
        "ClusterStateError",
        "NodeStartError",
        "BuildTimeoutError",
        "TeardownError",
        "PlatformError",
    ],
)
def test_everything_derives_from_raydog_error(name: str) -> None:
    assert issubclass(getattr(errors, name), errors.RayDogError)
