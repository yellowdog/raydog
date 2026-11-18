#!/usr/bin/env bash
# Shut the cluster down and wait until all of its instances have gone.
#   ./down.sh [template.yaml] [options for `ray down`, e.g. -v]
#
# For RayDog's own log lines as well, set RAYDOG_LOG_LEVEL=INFO (or DEBUG, which
# adds every call to YellowDog and how long it took).
set -euo pipefail
cd "$(dirname "$0")"

# Ray adds two empty legacy settings to the configuration of every external provider,
# then warns that it is dropping them. Hide just those lines of its error output. The
# filter ignores Ctrl-C, so that what Ray and RayDog say as they stop still shows.
ray_quietly() {
    { ray "$@" 2>&1 1>&3 3>&- |
        { trap '' INT; grep -v --line-buffered "Dropping the empty legacy field" >&2 || true; }; } 3>&1
}
trap 'echo "down.sh: interrupted, so the cluster may be partly removed: run ./down.sh again to finish." >&2; exit 130' INT
TEMPLATE=raydog.yaml
if [[ $# -gt 0 && "$1" != -* ]]; then
    TEMPLATE="$1"
    shift
fi
python render.py "$TEMPLATE" ".rendered/$TEMPLATE"
ray_quietly down --yes ".rendered/$TEMPLATE" "$@"
