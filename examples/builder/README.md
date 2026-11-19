# RayDog Builder examples

Two scripts that create Ray clusters on YellowDog from Python.

- `usage-example.py` builds a cluster with a pool of workers, connects to it through an SSH tunnel, runs a small Ray job, and shuts the cluster down.
- `manage-example.py` manages a running cluster: it adds worker pools to it, resizes and removes them, saves the cluster's state to a file, then picks the cluster up again from that file, as another process would, to shut it down.

## What you need

- A YellowDog account with an application key, a namespace, and compute requirement templates for the head node and for workers.
- On your own machine: Python with `yellowdog-ray` installed (which brings Ray) and the OpenSSH client. The Ray version here must match the one on the nodes (`RAY_VERSION` in `scripts/node-setup-userdata.sh`).
- An SSH key pair. The setup script puts the public key on the nodes; `usage-example.py` uses the private key to tunnel to the head node, which must have a public address and accept SSH from your machine.

## Settings

Set these in your shell or in a `.env` file. `settings.py` reads them and describes each.

| Variable | Meaning |
|---|---|
| `YD_API_KEY_ID`, `YD_API_KEY_SECRET` | your YellowDog application key |
| `YD_API_URL` | only for a dedicated YellowDog platform |
| `RAYDOG_NAMESPACE` | the YellowDog namespace to create clusters in |
| `RAYDOG_HEAD_TEMPLATE`, `RAYDOG_WORKER_TEMPLATE` | compute requirement templates |
| `RAYDOG_SSH_PRIVATE_KEY` | path of the private key; `<path>.pub` must be beside it |
| `RAYDOG_IMAGES_ID` | image, if the templates do not fix one: an image family as `namespace/name`, or an image ID (plain Ubuntu works with the setup script) |
| `RAYDOG_SSH_USER`, `RAYDOG_CLUSTER_NAME` | optional |
| `RAYDOG_USE_CONTAINERS` | set to `1` to run Ray in containers on the nodes: see below |
| `RAYDOG_OBSERVABILITY` | `1` for graphs in Ray's dashboard, from Prometheus and Grafana on the head node; `node` for an observability node that also collects every node's logs: see below |

## Running them

```
python usage-example.py
python manage-example.py
RAYDOG_OBSERVABILITY=1 python usage-example.py
RAYDOG_OBSERVABILITY=node python usage-example.py
```

Both shut their cluster down when they finish, and also if they fail or are interrupted. Each cluster is given a one-hour lifetime as a backstop. If in doubt, `raydog list -n <namespace>` shows what is running and `raydog down <name> -n <namespace>` ends it.

## The scripts

`scripts/node-setup-userdata.sh` prepares a plain Ubuntu instance at first boot: Python and Ray, then the YellowDog Agent, then your SSH key. `scripts/head-node-task-script.sh` and `scripts/worker-node-task-script.sh` start Ray on the nodes; each runs as a YellowDog task for as long as its node lives.

With `RAYDOG_USE_CONTAINERS=1`, Ray runs in a container on each node instead. The nodes are then set up by `../autoscaler/scripts/node-setup-docker.sh` (Docker if the image lacks it, the `rayproject/ray` image, the Agent, your key) and Ray is started by the `-docker` variants of the two start scripts. The Ray client is strict about versions: your own Python and Ray must match the image's (Python 3.12 and Ray 2.60.0 as shipped; change the image tag in the places `RELEASING.md` lists). The nodes need a few gigabytes of free disk for the image. The observability option is not available with containers.

Setting nodes up at boot takes a few minutes per node. For regular use, bake a machine image instead: see `../../packer`. With such an image, the setup script is no longer needed: in `settings.py`, replace the `userdata` given to each node (in `node()`) with just the lines that authorise your SSH public key for the Agent's user (`/opt/yellowdog/agent/.ssh/authorized_keys`), or with nothing if the image already has your key. The image from `../../packer` has Grafana Alloy but not Prometheus, Loki or Grafana, so a head node with `RAYDOG_OBSERVABILITY=1`, and the observability node, keep their setup script unless you give them an image of their own with those installed.

## Graphs in Ray's dashboard

`RAYDOG_OBSERVABILITY` offers two ways of giving Ray's dashboard its graphs.

With `RAYDOG_OBSERVABILITY=1`, the head node's setup also installs Prometheus and Grafana, as the Autoscaler example does and as Ray's own guide for clusters on VMs does. Prometheus collects metrics from every node, found through the list of metrics endpoints that Ray keeps up to date on the head; Grafana shows the dashboards Ray writes there; and `scripts/head-node-task-script.sh` points Ray's dashboard at both. Workers need nothing extra, and the cluster needs no extra node. If the services cannot be installed, the head carries on without them and says so in its console output.

With `RAYDOG_OBSERVABILITY=node`, `usage-example.py` adds an observability node instead, which collects every node's logs as well as its metrics, and keeps that work off the head. It is started before the head, and its address is given to the other nodes' start scripts. The setup script installs Prometheus, Loki and Grafana on it, with Ray's dashboards, before the Agent, and `scripts/observability-node-task-script.sh` checks they are running and holds the node. On the other nodes the setup script installs Grafana Alloy, which the start scripts run to send the node's metrics and logs there (`scripts/config.alloy`); a node carries on without it if it cannot be installed. The observability node needs a few gigabytes of disk beyond what Ray needs: Ubuntu's own images have under 7 GB, which is barely enough.

`settings.py` tells each node's setup script its part through `OBSERVABILITY` in `scripts/node-setup-userdata.sh`, and adds to it only the functions from `scripts/observability/` that the node runs: EC2 limits the size of userdata, and the whole lot would not fit. Neither way is available with containers.

After its job, the example keeps the cluster and its SSH tunnel until you press Enter, so that you can look: Ray's dashboard at http://localhost:8265 shows Grafana's graphs, and Grafana itself is at http://localhost:3000, forwarded through the head node. Grafana is open to view without signing in; its admin password is in `~/grafana-admin-password` on the node that runs it.
