"""
General RayDog utility methods.
"""

from yellowdog_client import PlatformClient
from yellowdog_client.model import InstanceSearch, Node, WorkerPool


def get_public_ip_from_node(client: PlatformClient, node: Node) -> str | None:
    """
    Return the public IP address of the specified node, or None
    if no address is present.
    """

    # This will always be a provisioned pool
    worker_pool: WorkerPool = client.worker_pool_client.get_worker_pool_by_id(
        node.workerPoolId
    )

    for instance in client.compute_client.get_instances(
        InstanceSearch(computeRequirementId=worker_pool.computeRequirementId)
    ).list_all():
        if instance.id.instanceId == node.details.instanceId:
            return instance.publicIpAddress

    return None
