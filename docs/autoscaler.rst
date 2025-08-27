RayDog Autoscaler
=================

Overview
--------

The RayDog Autoscaler enables dynamic provisioning and scaling of Ray clusters on the YellowDog platform, using Ray's built-in autoscaling capabilities. Unlike the RayDog Builder, which manually controls the provisioning of Ray clusters, the Autoscaler dynamically adjusts the number of worker nodes based on Ray workload demands, optimizing resource usage and cost.

This documentation covers the setup, configuration, and usage of the RayDog Autoscaler, intended for users familiar with Ray and YellowDog who want to scale distributed workloads efficiently.

Configuration
-------------

The RayDog Autoscaler uses a YAML configuration file to define the cluster’s properties, including the YellowDog namespace, node scripts, and authentication details. The configuration is passed to Ray’s autoscaler via the ``ray up`` and ``ray down`` commands.

A documented example of a Ray configuration file is found at: https://github.com/yellowdog/raydog/tree/main/examples/autoscaler/raydog.yaml. Please take a look at the example to explore the possible configuration options.

YellowDog Credentials
+++++++++++++++++++++

The Autoscaler expects to find YellowDog Application credentials in the following environment variables:

- ``YD_API_KEY_ID``
- ``YD_API_KEY_SECRET``

.. note::

    These credentials are currenly passed as environment variables to the head node in the YellowDog task that starts the Ray processes, so they can be seen in the task detail in the YellowDog portal or via the API.

If running on a dedicated YellowDog platform, the API URL must also be set in the following variable:

- ``YD_API_URL``: this will default to ``https://api.yellowdog.ai`` if the variable is not set

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

    Nodes can be configured dynamically by using base images and supplying configuration scripts to the node configuration via the ``userdata`` and ``extra_userdata`` properties in the configuration file. For production and/or large-scale use, however, we recommend creaing custom VM images with the requirements pre-installed.

Head/Worker Node Task Scripts
+++++++++++++++++++++++++++++

The RayDog autoscaler uses YellowDog tasks to start the required Ray processes on the head and worker nodes. Examples of these scripts can be found in the configuration file, in the ``ray_head_node_task_script`` and ``ray_worker_node_task_script`` provider properties.

The task scripts:

1. Ensure that an EXIT trap is set that stops Ray gracefully on task exit or abort, e.g.::

    trap "ray stop; echo Ray stopped" EXIT

2. Use the ``--block`` option with the ``ray start`` command, to ensure the YellowDog task lifecycle matches that of the Ray processes.


3. For the script used to set up Ray worker nodes, the private IP address of the head node to connect to will be found in the environment variable ``RAY_HEAD_IP``. For example, use the command line option ``--address=$RAY_HEAD_IP:6379`` when starting the Ray processes.

4. The head node starts the Valkey server on a non-standard port (configurable, but set to the default of ``16667``) prior to starting Ray.

.. note::

    The properties above are used instead of the usual Ray properties for Ray initialisation and setup.

Usage
-----

The RayDog Autoscaler integrates with Ray’s CLI to manage clusters. Below are the main steps to start, monitor, and terminate a cluster.

1. **Create and Start the Cluster**:

   Use the ``ray up`` command with the YAML configuration:

   .. code-block:: bash

      ray up raydog.yaml

This:

- Creates a head node using the specified ``compute_requirement_template`` and ``head_start_ray_script``.
- Sets up a Redis-based tag store on the head node for tracking node metadata.
- Provisions worker nodes as needed, based on Ray’s autoscaling policies (up to ``max_workers``).
- Uploads any files listed in ``files_to_upload`` to the head node.

2. **Monitor the Cluster**:

   Ray automatically scales worker nodes based on workload demands. To check the cluster status:

   .. code-block:: bash

      ray status

   This shows running nodes, resource usage, and autoscaling status.

3. **Submit Workloads**:

   Submit Ray jobs or scripts as usual, and the autoscaler will provision worker nodes dynamically:

   .. code-block:: bash

      ray submit raydog.yaml my_script.py

4. **Terminate the Cluster**:

   Shut down the cluster and clean up YellowDog resources:

   .. code-block:: bash

      ray down raydog.yaml

   This cancels the YellowDog work requirement and terminates all nodes.

.. note::
   - If interrupted (e.g., Ctrl+C during ``ray up``), the autoscaler attempts to clean up YellowDog resources automatically.

Troubleshooting
---------------

- **InvalidRequestException**:
  - **Cause**: May occur if a previous cluster run left a lingering work requirement.
  - **Solution**: The autoscaler automatically cancels conflicting work requirements. If the issue persists, manually cancel the work requirement in the YellowDog Portal.

- **Timeout Waiting for Head Node**:
  - **Cause**: The head node failed to reach the ``EXECUTING`` state within the timeout (default: 5 minutes).
  - **Solution**: Check the YellowDog Portal for task status. Ensure the ``compute_requirement_template`` is valid and the ``head_start_ray_script`` runs without errors.

- **FileNotFoundError**:
  - **Cause**: A ``file:`` referenced script or file in the YAML does not exist.
  - **Solution**: Verify all file paths in the YAML are correct and accessible.

- **SSH Connection Issues**:
  - **Cause**: Incorrect ``ssh_user`` or ``ssh_private_key`` configuration.
  - **Solution**: Ensure the SSH key is valid and the public key is in the head node’s ``~/.ssh/authorized_keys`` (handled by ``user_data`` script).

- **Autoscaling Not Triggering**:
  - **Cause**: Workload demands may not exceed current resources, or ``max_workers`` is too low.
  - **Solution**: Check ``ray status`` for resource usage. Increase ``max_workers`` in the YAML if needed.
