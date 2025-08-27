#!/usr/bin/env bash
echo "Ensure there's an SSH tunnel to the head node on port 8265:"
echo "  e.g.: ssh -i private-key -L 8265:localhost:8265 yd-agent@<head-node-IP-address"

ray job submit --address http://localhost:8265 --working-dir . -- python ray-sleep-job.py
