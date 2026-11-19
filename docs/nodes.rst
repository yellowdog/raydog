Preparing nodes
===============

RayDog provides the instances; what runs on them is up to you. This page says what a node needs and how to arrange it.

What a node needs
-----------------

- The **YellowDog Agent**, installed with its default settings (user ``yd-agent``, home ``/opt/yellowdog/agent``) and offering the ``bash`` task type. It is what runs the start scripts.
- **Python and Ray**, in the same versions as on your own machine, where the start script can find them.
- For the Autoscaler's **head node** only: **RayDog** itself (``yellowdog-ray``) in the same Python environment as Ray; an SSH server that accepts your key for the Agent's user; and ``rsync``.
- A network in which the nodes can reach each other on Ray's ports, and in which the head node can reach the YellowDog API.

Worker nodes need neither RayDog nor SSH access.

Network requirements
--------------------

.. list-table::
   :header-rows: 1
   :widths: 24 24 52

   * - From
     - To
     - What for
   * - Every node
     - The YellowDog platform, port 443 (HTTPS), outbound
     - The Agent registers the node and collects its task. A node that cannot make this connection never joins the cluster. The platform never connects in to a node.
   * - Every node
     - Every other node of the cluster, all TCP ports
     - Ray. Workers connect to the head on port 6379, and nodes then talk to each other on many ports, most of them chosen at random. The simplest arrangement is a security group, or its equivalent, that allows all traffic between its own members.
   * - The Autoscaler's head node
     - The YellowDog platform, port 443, outbound
     - Ray's autoscaler runs on the head node and adds and removes workers through the platform's API.
   * - Your machine
     - The head node, port 22 (SSH)
     - With the Autoscaler, ``ray up`` and Ray's other commands, always. With either front end, tunnels to the dashboard and the Ray client. Not needed to create or run a Builder cluster.
   * - Every node, while it is being set up
     - The internet, outbound
     - Only if nodes are set up at boot or pull a container image: to download Python, Ray, the Agent or the image. Not needed with an image that has everything installed.
   * - Your machine
     - The YellowDog platform, port 443, outbound
     - Everything RayDog does.

Some consequences:

- **Workers need no public addresses** and no inbound access from outside the cluster. They do need a route out, such as a NAT gateway, to reach the platform.
- **The head node needs an address your machine can reach** only for SSH. With the Autoscaler that is a public address unless you run ``ray up`` from inside the network and set ``use_internal_ips: true``.
- **Nothing of Ray's needs to be open to the outside.** The dashboard and job server have no authentication of their own; leave them on the head node's loopback interface, as the examples do, and use a tunnel.
- **All of a cluster's nodes should be in one network** where they can reach each other by private address, whatever mix of instance types the templates allow.

Sizing nodes
------------

What instances a node may run on is decided by its compute requirement template; how much work Ray puts on it is decided by the **resources** Ray believes the node has. The two have to agree.

**What Ray believes.** When Ray starts on a node it counts the machine's CPUs and GPUs and measures its memory, unless the start script tells it otherwise (``ray start --num-cpus=4``, ``--num-gpus``, ``--resources``). Ray then runs as many tasks at once as those numbers allow: a task asks for one CPU unless it says otherwise.

**The head node.** The examples give the head no CPUs (``--num-cpus=0`` in its start script, and ``{"CPU": 0}`` as its declaration in the Autoscaler's YAML), so that no task is placed on it and it is left to run the cluster. A modest instance is enough for the head of a small cluster; a job's driver also runs there, so allow for what your drivers do.

**With the Builder,** that is all there is to it: a pool of eight nodes is eight instances, and Ray uses whatever each turns out to have.

**With the Autoscaler,** each node type in the YAML also *declares* its resources, and Ray plans with the declaration: to place waiting work that needs ten CPUs on a type declared as ``{"CPU": 2}``, it asks for five nodes. As Ray's own cluster launcher does, RayDog hands each worker its type's declaration (and ``labels``, if the type has them), and ``ray start`` uses it in place of what it finds on the machine, overriding the start script's own options too. A worker declared as ``{"CPU": 2}`` runs two one-CPU tasks at once, whatever its instance has. RayDog does the same for the head node, which Ray's launcher leaves to its start commands, so that the head's declaration and what its Ray offers cannot disagree. A node type that declares nothing gets what Ray finds. So:

- Declare for each node type what its instances really have: declare more and tasks share the CPUs, or run out of memory; declare less and the rest goes unused.
- If a template can provide instance types of different sizes, declare for the smallest, or narrow the template so that they are the same size.
- ``max_workers`` for a node type is also the limit of its YellowDog worker pool.

**GPUs** are counted by Ray only if the instance's image has working drivers. **Memory** matters as much as CPUs: Ray keeps a share of each node's memory for its object store, and a node with too little will fail tasks that a count of its CPUs says it can run.

Choosing the image
------------------

``images_id`` names the image a node's instance starts from, if its template does not fix one. It can be:

- a **YellowDog image family**, by name as ``namespace/name`` (for example ``my-namespace/ubuntu-26-04``) or by ID (``ydid:imgfam:…``). This is the best choice: a family holds an image for each region and cloud it covers, and YellowDog starts each instance from the one that suits where it runs, so a template that spans regions or clouds needs nothing more. RayDog looks a name up when it creates the node's worker pool, and reports a name that does not exist rather than starting nothing;
- a YellowDog image group or image, by ID;
- a cloud's own image ID, such as an AMI (``ami-…``). An AMI exists in one region only, and must be shared with the account the template's compute sources launch instances in.

Image families are managed in the YellowDog portal, under images.

Setting nodes up at boot
------------------------

The simplest arrangement uses a plain Ubuntu image and gives each node type a setup script as ``userdata``, which runs once, as root, when an instance first boots. The examples include one: `examples/autoscaler/scripts/node-setup.sh <https://github.com/yellowdog/raydog/tree/main/examples/autoscaler/scripts/node-setup.sh>`_. It installs Python and Ray with ``uv`` into ``/opt/raydog/venv``, installs the YellowDog Agent, and authorises your SSH public key for the Agent's user.

One detail of that script matters if you write your own: **install the Agent last**. The moment the Agent starts, the node is offered work, so everything a start script needs must already be in place.

Two more matter on AWS. EC2 accepts at most 16 KB of userdata once it is base64-encoded, and YellowDog adds to what you give it, so keep a node's script to about 9 KB: 11 KB has been refused. The Builder example strips comments and blank lines from its setup script before sending it for this reason, and gives each node only the parts it runs. And Ubuntu's own images have a small root disk, under 7 GB usable, which Python, Ray and their download caches fill quickly: clear ``uv``'s cache after installing (``uv cache clean``), or give the template a larger volume.

Setting up at boot adds a few minutes to the start of every node.

Baking an image
---------------

For clusters that are started often, or that are large, build a machine image with everything installed and name it as ``images_id``, ideally as part of an image family. The repository includes a Packer template for AWS in `packer/ <https://github.com/yellowdog/raydog/tree/main/packer>`_. With such an image a node is ready within a minute or so of its instance starting, and ``userdata`` is needed only for what differs between clusters, such as the SSH key.

Containers
----------

Ray can run in a container on each node: the start scripts run ``docker run`` rather than ``ray start``. The node then needs Docker and the image (pulled at boot, not at node start) instead of Python and Ray. Both examples have a container variant: ``raydog-docker.yaml`` for the Autoscaler (with the notes in :doc:`autoscaler`), and ``RAYDOG_USE_CONTAINERS=1`` for the Builder examples. Three things to get right: the instances need a few gigabytes of free disk for the image; with the Builder, the Ray client on your machine must match the image's Python and Ray versions exactly; and containers should use host networking, so that Ray's ports are where the rest of the cluster expects them.

Versions
--------

Ray is strict about versions: the Ray, and the minor version of Python, on your machine must match those on the nodes, or connecting a client or submitting some kinds of job will fail. Pin both in your setup script or image, and change them together with your own environment.
