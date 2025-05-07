"""
Simple class to set up SSH tunnel port-forwarding from a Ray
head node to the local host.
"""

from sshtunnel import SSHTunnelForwarder

LOCALHOST = "127.0.0.1"


class RayTunnels:
    """
    Set up SSH port forwarding for Ray.
    """

    def __init__(
        self,
        ray_head_ip_address: str,
        ssh_user: str,
        private_key_file: str,
        ray_ports: tuple[int, ...] = (10001, 8265),
    ):
        """
        Set SSH tunnel parameters.

        :param ray_head_ip_address: the network-accessible IP address for
            the Ray head node
        :param ssh_user: the SSH user to use
        :param private_key_file: the file containing the private key to use
        :param ray_ports: the ports on which to establish tunnels
        """
        self._ray_ip = ray_head_ip_address
        self._ssh_user = ssh_user
        self._private_key_file = private_key_file
        self._ray_ports = ray_ports
        self._ray_tunnels: list[SSHTunnelForwarder] = []

    def start_tunnels(self):
        """
        Start the SSH tunnels.
        """
        for port in self._ray_ports:
            tunnel = SSHTunnelForwarder(
                self._ray_ip,
                ssh_username=self._ssh_user,
                ssh_pkey=self._private_key_file,
                remote_bind_address=(LOCALHOST, port),
                local_bind_address=(LOCALHOST, port),
            )
            tunnel.start()
            self._ray_tunnels.append(tunnel)

    def stop_tunnels(self, ignore_errors: bool = True):
        """
        Stop the SSH tunnels.

        :param ignore_errors: suppress exceptions and proceed
        """
        for tunnel in self._ray_tunnels:
            try:
                tunnel.stop(force=True)
            except:
                if ignore_errors:
                    pass
