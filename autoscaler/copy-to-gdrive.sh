#!/bin/bash -x

GDRIVE="$HOME/My Drive/raydog"

tar -czf /tmp/raydog.tgz -C .. raydog/autoscaler.py 
cp /tmp/raydog.tgz "$GDRIVE"

