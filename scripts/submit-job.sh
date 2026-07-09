#!/usr/bin/env bash
set -euo pipefail

##############################################################################
# Apache Flink Session Cluster — 提交 Job 腳本
# 用法: ./submit-job.sh <jar-filename> [flink-run-args...]
#
# 範例:
#   ./submit-job.sh my-flink-job-1.0-SNAPSHOT.jar
#   ./submit-job.sh my-flink-job-1.0-SNAPSHOT.jar --class com.example.MyJob
##############################################################################

CONTAINER_PREFIX="flink"
CLIENT_CONTAINER="${CONTAINER_PREFIX}-client"
JOBMANAGER_ADDRESS="jobmanager:8081"
USRLIB_PATH="/opt/flink/usrlib"

if [ $# -lt 1 ]; then
    echo "用法: $0 <jar-filename> [flink-run-args...]"
    echo ""
    echo "  jar-filename: 位於專案 target/ 目錄下的 jar 檔名"
    echo ""
    echo "範例:"
    echo "  $0 untitled1-1.0-SNAPSHOT.jar"
    echo "  $0 untitled1-1.0-SNAPSHOT.jar --class org.example.Main"
    exit 1
fi

JAR_FILE="$1"
shift
EXTRA_ARGS="${*:-}"

echo "[INFO] Submitting job: ${JAR_FILE}"
echo "[INFO] Target: ${JOBMANAGER_ADDRESS}"
echo "[INFO] Extra args: ${EXTRA_ARGS:-<none>}"
echo ""

podman exec "${CLIENT_CONTAINER}" \
    flink run -m "${JOBMANAGER_ADDRESS}" \
    ${EXTRA_ARGS} \
    "${USRLIB_PATH}/${JAR_FILE}"

echo ""
echo "[DONE] Job submitted successfully."

