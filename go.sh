#!/bin/bash -x

./copy-to-gdrive.sh

export PYTHONPATH=.:$PYTHONPATH
ray up --no-config-cache raydog-autoscaler.yaml