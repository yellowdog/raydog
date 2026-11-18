#!/usr/bin/env bash
# Create the cluster (or update it): fills in raydog.yaml and runs `ray up`.
#   ./up.sh [template.yaml] [options for `ray up`, e.g. -v]
#
# For RayDog's own log lines as well, set RAYDOG_LOG_LEVEL=INFO (or DEBUG, which
# adds every call to YellowDog and how long it took).
#
# Ray asks before it creates anything. It also asks before replacing the head node,
# which it does if the head's part of the configuration has changed, and which here
# means replacing the whole cluster.
set -euo pipefail
cd "$(dirname "$0")"

# Ray adds two empty legacy settings to the configuration of every external provider,
# then warns that it is dropping them. Hide just those lines of its error output. The
# filter ignores Ctrl-C, so that what Ray and RayDog say as they stop still shows.
ray_quietly() {
    { ray "$@" 2>&1 1>&3 3>&- |
        { trap '' INT; grep -v --line-buffered "Dropping the empty legacy field" >&2 || true; }; } 3>&1
}
trap 'echo "up.sh: interrupted. If the head node had started, the cluster is still running: \`raydog list\` shows it, and ./down.sh removes it." >&2; exit 130' INT
TEMPLATE=raydog.yaml
if [[ $# -gt 0 && "$1" != -* ]]; then
    TEMPLATE="$1"
    shift
fi
python render.py "$TEMPLATE" ".rendered/$TEMPLATE"
# --no-config-cache: Ray would otherwise not notice a change to a script named in
# the template with "file:"
ray_quietly up --no-config-cache ".rendered/$TEMPLATE" "$@"
