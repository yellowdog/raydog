#!/usr/bin/env bash

# This script makes the following assumptions:
# - You have a virtualenv with Ray on the path
# - You are running the script from the root of the repo
# 
# Run this script when you want to extract the dashboards from Ray.

ray start --head --disable-usage-stats

cp /tmp/ray/session_latest/metrics/grafana/dashboards/* ./scripts/dashboards/

ray stop