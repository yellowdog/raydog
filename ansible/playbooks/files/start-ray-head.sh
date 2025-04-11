#!/usr/bin/env bash

set -euo pipefail

VENV=/opt/yellowdog/agent/venv

VIRTUAL_ENV_DISABLE_PROMPT=true
source $VENV/bin/activate

# shut down Ray when this script is interrupted
trap "ray stop; echo Ray stopped" EXIT
trap "echo Quitting; exit 0" TERM INT

# start ray
ray start --disable-usage-stats --head --port=6379

# keep the task alive until it is killed
if [[ -e /usr/lib/bash/sleep ]] ; then
    # if the sleep command from the bash-builtins package is available, use it
    enable sleep
    while true; do sleep 10000; done
else
    # we have to spin more with the external sleep command, because SIGTERM won't 
    # get processed until after sleep exits
    while true; do sleep 10; done
fi
