#!/usr/bin/env bash
    
# This script makes the following assumptions:
# - You have rclone on your $PATH
# - You have AWS CLI configured according to: https://yellowdog.atlassian.net/wiki/spaces/DEVT/pages/3382935553/Use+K8s+Dev+Environments
# - You are running from the repo root, eg calling as "scripts/sync.sh"
# 
# Run this script when you want to copy all the k3s scripts and manifest to the
# s3 bucket. These are used when scale-example runs in k3s mode as the scripts 
# and manifests are used to build the cluster.

export AWS_PROFILE=DevSandbox
rclone -v sync ./scripts/k3s :s3,provider=AWS,region=eu-west-2,env_auth:tech.yellowdog.devsandbox.dev-platform/raydog/setup