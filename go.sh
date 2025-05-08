#!/bin/bash -x

./copy-to-gdrive.sh

export PYTHONPATH=.:$PYTHONPATH
#ray --logging-level 'debug' up --no-config-cache raydog-autoscaler.yaml
ray --logging-level 'debug' up raydog-autoscaler.yaml