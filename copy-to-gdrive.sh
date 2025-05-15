#!/bin/bash -x

GDRIVE="$HOME/My Drive/raydog"

tar -czf raydog.tgz raydog/autoscaler.py
cp raydog.tgz "$GDRIVE"

