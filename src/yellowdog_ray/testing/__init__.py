"""Support for testing code that uses RayDog, without a YellowDog account."""

from yellowdog_ray.testing.clock import FakeClock
from yellowdog_ray.testing.fake_platform import FakePlatform

__all__ = ["FakeClock", "FakePlatform"]
