"""
Simple class to set up SSH tunnel port-forwarding from a Ray
head node to the local host.
"""

from dataclasses import dataclass

from sshtunnel import SSHTunnelForwarder


@dataclass
class SSHTunnelSpec:
    """
    Defines the parameters of an SSH tunnel.
    """

    local_bind_hostname: str
    local_bind_ip_address: int
    remote_bind_hostname: str
    remote_bind_ip_address: int


class RayTunnels:
    """
    Set up SSH port forwarding for Ray.
    """

    def __init__(
        self,
        ray_head_ip_address: str,
        ssh_user: str,
        password: str | None = None,
        private_key_file: str | None = None,
        private_key_password: str | None = None,
        ray_tunnel_specs: list[SSHTunnelSpec] | None = None,
    ):
        """
        Set SSH tunnel parameters. One of private_key_file or password must be
        supplied.

        :param ray_head_ip_address: the network-accessible IP address for
            the Ray head node
        :param ssh_user: the SSH user to use
        :param password: the user's password
        :param private_key_password: the password for the private key
        :param private_key_file: the file containing the private key to use
        :param ray_tunnel_specs: the ports on which to establish tunnels as a list of
            SSHTunnelSpec objects
        """

        self._ray_ip = ray_head_ip_address
        self._ssh_user = ssh_user
        self._private_key_file = private_key_file
        self._password = password
        self._private_key_password = private_key_password
        self._ray_tunnel_specs = (
            [
                self.basic_port_forward(10001),
                self.basic_port_forward(8265),
            ]
            if ray_tunnel_specs is None
            else ray_tunnel_specs
        )
        self._ray_tunnels: list[SSHTunnelForwarder] = []

    @staticmethod
    def basic_port_forward(port: int) -> SSHTunnelSpec:
        """
        Create a basic port-forwarding tunnel specification.
        
        :param port: the port to forward
        :return: An SSHTunnelSpec object
        """
        localhost = "localhost"
        return SSHTunnelSpec(localhost, port, localhost, port)

    def start_tunnels(self):
        """
        Start the SSH tunnels.
        """

        for tunnel_spec in self._ray_tunnel_specs:
            tunnel = SSHTunnelForwarder(
                self._ray_ip,
                ssh_username=self._ssh_user,
                ssh_password=self._password,
                ssh_pkey=self._private_key_file,
                ssh_private_key_password=self._private_key_password,
                local_bind_address=(
                    tunnel_spec.local_bind_hostname,
                    tunnel_spec.local_bind_ip_address,
                ),
                remote_bind_address=(
                    tunnel_spec.remote_bind_hostname,
                    tunnel_spec.remote_bind_ip_address,
                ),
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
