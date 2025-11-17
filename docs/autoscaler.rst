RayDog Autoscaler
=================

Overview
--------

The RayDog Autoscaler enables dynamic provisioning and scaling of Ray clusters on the YellowDog platform, using Ray's built-in autoscaling capabilities. Unlike the RayDog Builder, which manually controls the provisioning of Ray clusters, the Autoscaler dynamically adjusts the number of worker nodes based on Ray workload demands, optimizing resource usage and cost.

This documentation covers the setup, configuration, and usage of the RayDog Autoscaler, intended for users familiar with Ray and YellowDog.

Configuration
-------------

The RayDog Autoscaler uses a YAML configuration file to define the cluster’s properties. The configuration is passed to Ray’s autoscaler via the ``ray up`` and ``ray down`` commands.

A documented example of a Ray configuration file is found at: https://github.com/yellowdog/raydog/tree/main/examples/autoscaler/raydog.yaml. Please take a look at the documentation in the example to explore the possible configuration options.

YellowDog Credentials
+++++++++++++++++++++

The Autoscaler requires YellowDog Application credentials to be set in the following environment variables (or in a ``.env`` file):

- ``YD_API_KEY_ID``
- ``YD_API_KEY_SECRET``

.. note::

    These credentials are currently passed as environment variables to the head node in the YellowDog task that starts the Ray processes, so they can be seen in the task detail in the YellowDog portal or via the API.

If running on a dedicated YellowDog platform, the API URL must also be set:

- ``YD_API_URL``: (defaults to ``https://api.yellowdog.ai``)

Virtual Machine Configuration
+++++++++++++++++++++++++++++

The nodes provisioned for use by the RayDog autoscaler must be configured as follows:

- The **YellowDog Agent** must be installed with its default settings (user ``yd-agent``, etc.)
- **SSH access** via private key must be enabled for user ``yd-agent``
- Nodes must advertise the ``bash`` **task type** to run Bash scripts
- A suitable version of **Python** (3.10+) must be installed
- A Python **virtual environment** must be created, accessible to the ``yd-agent`` user; the later task scripts assume this is located in ``/opt/yellowdog/agent/venv``
- The following **Python packages** must be installed in the virtual environment: ``ray[client] ray[default] yellowdog-ray``
- The **firewall** must be disabled to allow inter-node Ray communications
- The **head node** must also have the Valkey/Redis package available

An example node configuration script for head and worker nodes suitable for use with a base Ubuntu image is found at: https://github.com/yellowdog/raydog/tree/main/examples/autoscaler/scripts/example-node-setup.sh

An example node configuration script to install Valkey on the head node is found at: https://github.com/yellowdog/raydog/tree/main/examples/autoscaler/scripts/install-valkey.sh.

.. note::

    Nodes can be configured dynamically by using base images and supplying configuration scripts to the node configuration via the ``userdata`` and ``extra_userdata`` properties in the configuration file. For production and/or large-scale use, however, we recommend creating custom VM images with the requirements pre-installed.

Head & Worker Node Task Scripts
+++++++++++++++++++++++++++++++

The RayDog autoscaler uses YellowDog tasks to start the required Ray processes on the head and worker nodes. Examples of these scripts can be found in the configuration file, in the ``ray_head_node_task_script`` and ``ray_worker_node_task_script`` provider properties.

The task scripts:

1. Ensure that an EXIT trap is set that stops Ray gracefully on task exit or abort, e.g.::

    trap "ray stop; echo Ray stopped" EXIT

2. Use the ``--block`` option with the ``ray start`` command, to ensure the YellowDog task lifecycle matches that of the Ray processes.


3. For the script used to set up Ray worker nodes, the private IP address of the head node to connect to will be found in the environment variable ``RAY_HEAD_IP``. For example, use the command line option ``--address=$RAY_HEAD_IP:6379`` when starting the Ray processes.

4. The head node starts the Valkey server on a non-standard port (configurable, but set to the default of ``16667``) prior to starting Ray. The use of the non-standard port is required to avoid conflicting with Ray itself.

.. note::

    The properties above (along with ``userdata`` and ``extra_userdata`` in the node type configuration) are used instead of the usual Ray properties for Ray node initialisation and setup.

Client Requirements
-------------------

The system used to invoke ``ray`` commands must have Python 3.10+, and the Python packages ``ray[client]`` and ``yellowdog-ray``.

.. note::

    Ray is very unforgiving about the versions of Python and Ray on the client and cluster respectively, so make sure these match.

Usage
-----

The RayDog Autoscaler integrates with Ray’s CLI to manage clusters, so clusters are controlled via the usual ``ray`` commands.

Running Ray in Docker Containers
--------------------------------

It's possible to run the Ray processes in containers rather than on the host. The task start scripts need to be modified to run the desired container instead of running the Ray processes directly on the host. They must also stop the container on exit. Script examples can be found within the Ray configuration file at: https://github.com/yellowdog/raydog/tree/main/examples/autoscaler/raydog-docker.yaml.

The container image(s) must include the required versions of Python and Ray, and must also include RayDog and Valkey for the head node container image.
