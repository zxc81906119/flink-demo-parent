#!/usr/bin/env bash
set -euo pipefail

##############################################################################
# Apache Flink Session Cluster — Podman 啟動腳本
# Flink LTS Version: 1.20.1
# 架構: 1 JobManager + 2 TaskManagers + 1 Flink Client
##############################################################################

FLINK_VERSION="1.20.1"
FLINK_IMAGE="docker.io/apache/flink:${FLINK_VERSION}"
NETWORK_NAME="flink-network"
CONTAINER_PREFIX="flink"

# 專案根目錄 (腳本位於 scripts/ 下)
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=========================================="
echo " Flink Session Cluster (Podman)"
echo " Flink Version : ${FLINK_VERSION}"
echo " Network       : ${NETWORK_NAME}"
echo "=========================================="

# ------------------------------------------------------------------
# 1. 建立 Podman Network (若不存在)
# ------------------------------------------------------------------
if ! podman network exists "${NETWORK_NAME}" 2>/dev/null; then
    echo "[INFO] Creating podman network: ${NETWORK_NAME}"
    podman network create "${NETWORK_NAME}"
else
    echo "[INFO] Network '${NETWORK_NAME}' already exists, skipping creation."
fi

# ------------------------------------------------------------------
# 2. 啟動 JobManager
# ------------------------------------------------------------------
echo "[INFO] Starting JobManager..."
podman run -d \
    --name "${CONTAINER_PREFIX}-jobmanager" \
    --network "${NETWORK_NAME}" \
    --hostname jobmanager \
    -p 8081:8081 \
    -e JOB_MANAGER_RPC_ADDRESS=jobmanager \
    "${FLINK_IMAGE}" \
    jobmanager

echo "[INFO] JobManager started. WebUI available at http://localhost:8081"

# ------------------------------------------------------------------
# 3. 啟動 TaskManager 1
# ------------------------------------------------------------------
echo "[INFO] Starting TaskManager 1..."
podman run -d \
    --name "${CONTAINER_PREFIX}-taskmanager-1" \
    --network "${NETWORK_NAME}" \
    -e JOB_MANAGER_RPC_ADDRESS=jobmanager \
    -e TASK_MANAGER_NUMBER_OF_TASK_SLOTS=2 \
    "${FLINK_IMAGE}" \
    taskmanager

# ------------------------------------------------------------------
# 4. 啟動 TaskManager 2
# ------------------------------------------------------------------
echo "[INFO] Starting TaskManager 2..."
podman run -d \
    --name "${CONTAINER_PREFIX}-taskmanager-2" \
    --network "${NETWORK_NAME}" \
    -e JOB_MANAGER_RPC_ADDRESS=jobmanager \
    -e TASK_MANAGER_NUMBER_OF_TASK_SLOTS=2 \
    "${FLINK_IMAGE}" \
    taskmanager

# ------------------------------------------------------------------
# 7. 啟動 Flink Client (掛載專案 target/ 目錄)
#    容器保持執行狀態，使用者可透過 podman exec 進入提交 job
# ------------------------------------------------------------------
echo "[INFO] Starting Flink Client..."
podman run -d \
    --name "${CONTAINER_PREFIX}-client" \
    --network "${NETWORK_NAME}" \
    -e JOB_MANAGER_RPC_ADDRESS=jobmanager \
    -e HADOOP_CONF_DIR=/opt/flink/conf \
    -v "${PROJECT_DIR}/target:/opt/flink/usrlib:z" \
    "${FLINK_IMAGE}" \
    sleep infinity

echo ""
echo "=========================================="
echo " Flink Session Cluster is UP!"
echo "=========================================="
echo ""
echo " HDFS NameNode    : http://localhost:9870"
echo " JobManager WebUI : http://localhost:8081"
echo " TaskManagers     : 2 (each with 2 task slots)"
echo " Flink Client     : ${CONTAINER_PREFIX}-client"
echo ""
echo " 提交 Job 範例:"
echo "   podman exec ${CONTAINER_PREFIX}-client flink run -m jobmanager:8081 /opt/flink/usrlib/<your-jar>.jar"
echo ""
echo " 或使用 scripts/submit-job.sh <jar-filename>"
echo "=========================================="

