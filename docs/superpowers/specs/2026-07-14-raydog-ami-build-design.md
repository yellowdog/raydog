# RayDog AMI Build Pipeline — Design

**Date:** 2026-07-14
**Status:** Approved pending spec review

## Goal

Provide an automated, locally-runnable way to build AWS AMIs with everything
heavy pre-installed for RayDog cluster nodes, so that node boot-time userdata
shrinks to per-node YellowDog agent configuration only. This replaces the slow
per-boot installation currently done by
`examples/builder/scripts/node-setup-userdata.sh` and
`examples/autoscaler/scripts/example-node-setup.sh`.

## Decisions (from brainstorming)

- **Cloud scope:** AWS only for now; design stays open to adding other clouds
  later (Packer supports Azure/GCP/OCI builders in the same template).
- **Trigger:** Manual, run locally via a `make ami` target. No CI integration.
- **AMI contents:** Bake everything heavy — YellowDog agent binaries, uv,
  Python venv, Ray, RayDog, Grafana Alloy (installed but disabled). Per-node
  agent configuration (worker pool token, task types) remains a boot-time
  concern handled by YellowDog-injected userdata.
- **YellowDog registration:** None — the build outputs the AMI ID; image
  families / compute templates are updated manually.
- **Versioning:** Single parameterized build per run: `python_version`,
  `ray_version`, `raydog_version` (empty ⇒ latest). Re-run for other combos.
- **Location:** This repo, in a new `packer/` directory.
- **Base OS:** Ubuntu 26.04 LTS (most recent official Canonical AMI, x86_64).
- **Tooling:** HashiCorp Packer with the `amazon-ebs` builder.

## Components

### `packer/raydog-ami.pkr.hcl`

Packer template with:

- **Variables:** `python_version` (default current, e.g. `3.12.11`),
  `ray_version` (default current, e.g. `2.49.2`), `raydog_version`
  (default `""` = latest from PyPI), `aws_region`, `instance_type`
  (default a cheap builder, e.g. `t3.large`), `ssh_public_key`
  (public key text to authorize for `yd-agent`; default `""` = skip).
- **Source AMI:** `source_ami_filter` for the most recent official Canonical
  Ubuntu 26.04 LTS server image (owner `099720109477`), amd64, EBS/gp3.
- **AMI naming:** `raydog-ray{ray_version}-py{python_version}-{timestamp}`,
  tagged with Python/Ray/RayDog versions, base AMI ID, and build date.
- **Provisioner:** shell provisioner running `packer/scripts/provision.sh`
  with the version variables passed as environment variables.

### `packer/scripts/provision.sh`

Derived from the existing node-setup scripts, minus per-node concerns:

1. Install the YellowDog agent via the standard installer from
   `yellowdog/resources`, in install-only mode (no per-node configuration) —
   exact mechanism to be confirmed during implementation; fallback is a
   normal install whose configuration is overwritten at boot.
2. Add `yd-agent` to passwordless sudoers.
3. Authorize the injected SSH public key for `yd-agent` (parameterized —
   the hard-coded key in the current example scripts is not baked in).
4. Install uv; create the Python venv at `/opt/yellowdog/agent/venv` with the
   requested Python version.
5. `uv pip install "ray[default,client]==$RAY_VERSION"` and `yellowdog-ray`
   (pinned if `raydog_version` given, otherwise latest).
6. Install Grafana Alloy from the Grafana apt repo, write the Alloy config
   (as in the current builder userdata script), leave the service disabled.
7. Disable the firewall (`ufw disable`).
8. Set ownership of the agent home to `yd-agent`.
9. Clean up for imaging: `apt-get clean`, truncate logs, and
   `cloud-init clean` so instances launched from the AMI run cloud-init
   (and thus YellowDog userdata) as first boot.

`set -euo pipefail` throughout — any failure aborts the Packer build so no
half-baked AMI is produced. Packer terminates the builder instance on failure.

### Makefile target

```
make ami [PYTHON_VERSION=…] [RAY_VERSION=…] [RAYDOG_VERSION=…] [AWS_REGION=…]
```

Wraps `packer init` + `packer validate` + `packer build`, passing variables
through. Prints the resulting AMI ID (Packer's normal output).

### Boot-time userdata after this change

Nodes launched from the AMI need only the YellowDog-injected agent
configuration step. The example userdata scripts remain in `examples/` for
users who don't want a custom image, with a README note pointing at the AMI
alternative. Where observability is used, `OBSERVABILITY_HOST` is set and
Alloy enabled/started by the task script, unchanged from today.

### Documentation

- `packer/README.md`: prerequisites (Packer ≥ 1.10, AWS credentials with EC2
  build permissions), usage, variables, what's baked in, how to point a
  YellowDog compute template at the resulting AMI.
- Short note in the main `README.md` referencing `packer/`.

## Risks / open questions

- **Ubuntu 26.04 recency:** released April 2026. Confirm (a) the Canonical
  AMI name pattern for the `source_ami_filter`, and (b) that the Grafana apt
  repo and the YD agent installer work on 26.04. If a third-party repo lags,
  the build fails loudly at provision time (acceptable — fix or temporarily
  pin base OS then).
- **YD agent install-only mode:** confirm installer env-var support; the
  fallback (install normally, reconfigure at boot) is functionally fine.

## Testing

1. `packer validate` in the Makefile catches template errors before spend.
2. Build an AMI; confirm it is created, tagged, and the builder instance is
   terminated.
3. Point a YellowDog compute template at the AMI and run the autoscaler
   example (`examples/autoscaler/up.sh`) with a minimal userdata script;
   confirm nodes register and the Ray cluster forms, and note boot-time
   improvement vs. the full userdata install.
4. Failure path: intentionally break a version pin and confirm the build
   aborts without producing an AMI.
