#!/bin/bash -x

HERE=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

VIRTUAL_ENV_DISABLE_PROMPT=true
source $HERE/../venv/bin/activate

./copy-to-gdrive.sh

export PYTHONPATH=$HERE/..:$PYTHONPATH
ray --logging-level 'debug' up raydog-autoscaler.yaml