#!/usr/bin/env bash

# Convenience script to run 'ray up'

ray disable-usage-stats
ray --logging-level=info up --yes --no-config-cache raydog.yaml
