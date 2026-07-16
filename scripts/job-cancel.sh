#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -eq 0 ]]; then
  echo "[ERROR] Missing required argument: <job_id>"
  echo "[INFO] Usage: $0 <job_id>"
  exit 1
fi

JOB_ID="$1"
CONTAINER_PREFIX="flink"
CLIENT_CONTAINER="${CONTAINER_PREFIX}-client"
JOBMANAGER_ADDRESS="jobmanager:8081"

echo "[INFO] Cancelling job: ${JOB_ID}"
echo "[INFO] Target: ${JOBMANAGER_ADDRESS}"
echo ""

podman exec "${CLIENT_CONTAINER}" \
    flink cancel -m "${JOBMANAGER_ADDRESS}" "${JOB_ID}"
