#!/usr/bin/env bash

# ToDo: remove
# Temporarily upload the current version of the autoscaler to S3
aws s3 cp ../../src/yellowdog_ray/raydog/autoscaler.py \
          s3://tech.yellowdog.devsandbox.raydog.autoscaler/autoscaler.py

ray disable-usage-stats
ray --logging-level=info up --yes --no-config-cache raydog.yaml
