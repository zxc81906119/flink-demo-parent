#!/usr/bin/env bash
set -euo pipefail

CONTAINER_PREFIX="flink"
CLIENT_CONTAINER="${CONTAINER_PREFIX}-client"

JOBMANAGER_ADDRESS="jobmanager:8081"

echo "[INFO] Listing jobs"
echo "[INFO] Target: ${JOBMANAGER_ADDRESS}"
echo ""

podman exec "${CLIENT_CONTAINER}" \
    flink list -a -m "${JOBMANAGER_ADDRESS}"