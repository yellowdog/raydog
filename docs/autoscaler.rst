RayDog Autoscaler
=================

Overview
--------

The RayDog Autoscaler enables dynamic provisioning and scaling of Ray clusters on the YellowDog platform, leveraging Ray's built-in autoscaling capabilities. Unlike the RayDog Builder, which manually provisions Ray clusters, the autoscaler dynamically adjusts the number of worker nodes based on workload demands, optimizing resource usage and cost. It integrates with YellowDog to create head and worker nodes, manages node metadata via a Redis-based tag store, and supports SSH tunneling for secure communication.

This documentation covers the setup, configuration, and usage of the RayDog Autoscaler, intended for users familiar with Ray and YellowDog who want to scale distributed workloads efficiently.

Configuration
-------------

The RayDog Autoscaler uses a YAML configuration file to define the cluster’s properties, including the YellowDog namespace, node scripts, and authentication details. The configuration is passed to Ray’s autoscaler via the ``ray up`` command.

A documented example of a Ray configuration file is found at: https://github.com/yellowdog/raydog/tree/main/examples/autoscaler/raydog.yaml.

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
   - Set environment variables (``YD_API_KEY_ID``, ``YD_API_KEY_SECRET``) before running commands, as described in :ref:`raydog-builder`.

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
