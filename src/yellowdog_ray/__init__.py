"""RayDog: build and autoscale Ray clusters on the YellowDog platform."""

from yellowdog_ray.builder import (
    ClusterSpec,
    HeadNode,
    NodeSpec,
    RayDogCluster,
    WorkerPool,
)
from yellowdog_ray.credentials import Credentials, load_credentials
from yellowdog_ray.errors import (
    BuildTimeoutError,
    ClusterExistsError,
    ClusterNotFoundError,
    ClusterStateError,
    ConfigError,
    CredentialsError,
    NodeStartError,
    PlatformError,
    RayDogError,
    TeardownError,
    TunnelError,
)
from yellowdog_ray.tunnels import PortForward, RayTunnels

# Set by bin/release.py: a development version between releases
__version__ = "3.0.0.dev0"

__all__ = [
    "BuildTimeoutError",
    "ClusterExistsError",
    "ClusterNotFoundError",
    "ClusterSpec",
    "ClusterStateError",
    "ConfigError",
    "Credentials",
    "CredentialsError",
    "HeadNode",
    "NodeSpec",
    "NodeStartError",
    "PlatformError",
    "PortForward",
    "RayDogCluster",
    "RayDogError",
    "RayTunnels",
    "TeardownError",
    "TunnelError",
    "WorkerPool",
    "__version__",
    "load_credentials",
]
