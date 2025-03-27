#!/bin/bash
cluster_root = "192.168.0.1"
make start &
sleep 2
# Deploy all functions locally
# ./scripts/upload.sh
# ./scripts/upload.sh
# ./scripts/upload.sh
# ./scripts/upload.sh
curl -X POST -H "X-tinyFaaS-joincluster: $cluster_root" http://127.0.0.1:8000