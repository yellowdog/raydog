#!/bin/bash -x

./copy-to-gdrive.sh

HERE=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export PYTHONPATH=$HERE/..:$PYTHONPATH
ray --logging-level 'debug' up raydog-autoscaler.yaml