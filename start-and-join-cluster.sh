#!/bin/bash
make start &
# TODO: await make being done and tinyfaas being responsive
# Deploy all required functions
# ./scripts/upload.sh
# ./scripts/upload.sh
# ./scripts/upload.sh
# ./scripts/upload.sh
curl -X POST -H "X-tinyFaaS-joincluster: 192.168.0.1" http://127.0.0.1:8000