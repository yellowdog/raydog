#!/bin/bash -x

HERE=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export PYTHONPATH=$HERE/..:$PYTHONPATH
ray --logging-level 'debug' down raydog-autoscaler.yaml