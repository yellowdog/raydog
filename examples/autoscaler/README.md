# RayDog Autoscaler example

A Ray cluster on YellowDog that grows and shrinks with its workload: Ray's own autoscaler decides how many workers are needed, and RayDog provides them as YellowDog instances.

## What you need

- A YellowDog account with an application key, a namespace, and compute requirement templates for the head node and for workers.
- On your own machine: Python with `yellowdog-ray` installed (which brings Ray), the OpenSSH client and `rsync`. The Ray version here must match the one on the nodes (`RAY_VERSION` in `scripts/node-setup.sh`).
- An SSH key pair. Ray uses the private key to reach the head node; the setup script puts the public key on the nodes. The head node must accept SSH from your machine (port 22).

## Settings

Set these in your shell or in a `.env` file in this directory.

| Variable | Meaning |
|---|---|
| `YD_API_KEY_ID`, `YD_API_KEY_SECRET` | your YellowDog application key |
| `YD_API_URL` | only for a dedicated YellowDog platform |
| `RAYDOG_NAMESPACE` | the YellowDog namespace to create the cluster in |
| `RAYDOG_HEAD_TEMPLATE`, `RAYDOG_WORKER_TEMPLATE` | compute requirement templates |
| `RAYDOG_SSH_PRIVATE_KEY` | path of the private key; `<path>.pub` is used as the public key unless `RAYDOG_SSH_PUBLIC_KEY` is set |
| `RAYDOG_IMAGES_ID` | image, if the templates do not fix one: an image family as `namespace/name`, or an image ID (a plain Ubuntu image works with the setup script) |
| `RAYDOG_CLUSTER_NAME` | optional; defaults to `raydog-example` |
| `RAYDOG_TAG` | optional; a tag for the cluster's work requirement and compute requirements |
| `RAYDOG_PIP_SPEC` | optional; what to install as RayDog on the head node, by default the released `yellowdog-ray` |
| `RAYDOG_OBSERVABILITY` | optional; `1` for graphs in Ray's dashboard, from Prometheus and Grafana on the head node: see below |

## Running it

```
./up.sh            # create the cluster: returns when the head node is running Ray
./submit-job.sh    # run job/sleep_job.py; watch workers appear, then go
./down.sh          # shut everything down, and wait until every instance has gone
```

`raydog.yaml` is the cluster definition, with every RayDog setting explained in place. It is a template: the scripts fill in the `${RAYDOG_...}` settings (Ray's YAML cannot read the environment itself) and write the result to `.rendered/`. Any Ray command can then be used on it, for example `ray exec .rendered/raydog.yaml 'ray status'` or `ray dashboard .rendered/raydog.yaml`.

To run Ray in containers instead, pass the other template: `./up.sh raydog-docker.yaml`, and likewise for the other scripts.

## Graphs in Ray's dashboard

With `RAYDOG_OBSERVABILITY=1`, the head node's setup (`scripts/node-setup.sh`) also installs Prometheus and Grafana, following Ray's own guide for clusters on VMs. Prometheus collects metrics from every node, found through the list of metrics endpoints that Ray keeps up to date on the head; Grafana shows the dashboards Ray writes there; and `scripts/head-start.sh` points Ray's dashboard at both. Workers need nothing extra. To see the graphs, forward Grafana's port with the dashboard's, and open http://localhost:8265:

```
raydog tunnel -n <namespace> -i <private key> -p 10001 -p 8265 -p 3000 raydog-example
```

Grafana itself is then at http://localhost:3000, open to view without signing in; its admin password is in `~/grafana-admin-password` on the head node. Prometheus keeps at most 256 MB of data. If the services cannot be installed, the head carries on without them and says so in its console output. The container variant does not offer this.

For more detail, `up.sh` and `down.sh` pass any options after the template on to Ray (`./up.sh raydog.yaml -v`), and `RAYDOG_LOG_LEVEL=INFO` (or `DEBUG`, which adds every call to YellowDog and how long it took) shows RayDog's own lines among Ray's: `RAYDOG_LOG_LEVEL=INFO ./up.sh`.

## How it works

`ray up` creates a YellowDog work requirement for the cluster and a worker pool with one instance for the head node, then starts the head by running `scripts/head-start.sh` on it as a YellowDog task. Ray copies the cluster config to the head node over SSH, along with a small file holding your YellowDog credentials, which the autoscaler there needs in order to add workers; the credentials never pass through YellowDog itself. When Ray wants more workers, the autoscaler on the head node creates a worker pool for that node type and one task per worker, each running `scripts/worker-start.sh`. When Ray no longer needs a worker, its task is cancelled and its instance is released.

Nodes are set up at first boot by `scripts/node-setup.sh`, given as instance userdata. That is convenient but slow (a few minutes per node); for regular use, bake a machine image with everything installed (see `../../packer`) and remove `userdata` from the node types.

## If something goes wrong

- `ray up` waits up to `build_timeout_minutes` for the head node. If it gives up, it removes what it created. The node's own log of the setup script is `/var/log/raydog-node-setup.log`.
- The cluster has a lifetime (`lifetime_hours`, two hours in this example), after which it is stopped whatever state it is in, so that a forgotten cluster does not run for ever.
- `./down.sh` can be run at any time, more than once, and from any machine that has the same settings.
