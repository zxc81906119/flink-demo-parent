#!/usr/bin/env bash
set -euo pipefail

CONTAINER_PREFIX="flink"
CLIENT_CONTAINER="${CONTAINER_PREFIX}-client"

JOB_ID="$1"

shift
EXTRA_ARGS="${*:-}"

JOBMANAGER_ADDRESS="jobmanager:8081"

echo "[INFO] Cancelling job: ${JOB_ID}"
echo "[INFO] Target: ${JOBMANAGER_ADDRESS}"
echo "[INFO] Extra args: ${EXTRA_ARGS:-<none>}"
echo ""

export MSYS_NO_PATHCONV=1

podman exec "${CLIENT_CONTAINER}" \
    flink cancel -m "${JOBMANAGER_ADDRESS}" \
    ${EXTRA_ARGS} \
    "${JOB_ID}"