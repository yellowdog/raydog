Testing code that uses RayDog
=============================

RayDog ships the simulated YellowDog platform that its own tests use, so that code which builds clusters can be tested without an account, without instances and without waiting.

.. code-block:: python

   from yellowdog_ray import ClusterSpec, NodeSpec, RayDogCluster
   from yellowdog_ray.testing import FakePlatform

   def test_my_pipeline_shuts_its_cluster_down():
       platform = FakePlatform()
       node = NodeSpec(compute_requirement_template="ns/any", start_script="ray start --block")
       spec = ClusterSpec(name="demo", namespace="ns", head=node)

       with RayDogCluster(spec, platform=platform, clock=platform.clock) as cluster:
           cluster.add_worker_pool("workers", node, count=3)
           cluster.build()
           platform.clock.advance(300)             # let the workers start
           assert platform.live_instances == 4

       platform.clock.advance(120)
       assert platform.live_instances == 0

:class:`~yellowdog_ray.testing.FakePlatform` behaves as the real platform was observed to: instances take time to boot, nodes register, tasks start, cancellation is asynchronous, pools grow to meet demand, names stay reserved after use. Nothing happens until its clock is advanced, and waiting inside RayDog (for a head node to start, say) advances the clock rather than sleeping, so tests run in milliseconds.

It can also misbehave on request:

.. code-block:: python

   platform.fail_next("provision_pool", PlatformError("no capacity"))   # the next call fails
   platform.never_register("demo")           # instances run but nodes never appear
   platform.node_boot_time(600)              # slow nodes
   platform.fail_task(task_id)               # a start script exits with an error
   platform.lose_node(task_id)               # an instance is reclaimed

Its docstring lists the few behaviours that are assumptions rather than observations.

.. automodule:: yellowdog_ray.testing.fake_platform
   :members: FakePlatform
   :no-index:

.. automodule:: yellowdog_ray.testing.clock
   :members: FakeClock
