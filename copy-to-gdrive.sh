#!/bin/bash -x

GDRIVE="$HOME/My Drive/raydog"

tar -czf raydog.tgz raydog example-node-setup.sh raydog-autoscaler.yaml
cp raydog.tgz "$GDRIVE"

