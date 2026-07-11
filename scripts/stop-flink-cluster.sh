#!/usr/bin/env bash
set -euo pipefail

##############################################################################
# Apache Flink Session Cluster — Podman 停止 & 清理腳本
##############################################################################

NETWORK_NAME="flink-network"
CONTAINER_PREFIX="flink"

echo "[INFO] Stopping and removing Flink containers..."

for name in "${CONTAINER_PREFIX}-client" \
            "${CONTAINER_PREFIX}-taskmanager-2" \
            "${CONTAINER_PREFIX}-taskmanager-1" \
            "${CONTAINER_PREFIX}-jobmanager" \
            "kafka-ui" \
            "kafka" \
            "postgres" \
            "hadoop-datanode" \
            "hadoop-namenode"; do
    if podman container exists "${name}" 2>/dev/null; then
        echo "  -> Stopping ${name}..."
        podman stop "${name}" 2>/dev/null || true
        podman rm -f "${name}" 2>/dev/null || true
    else
        echo "  -> ${name} not found, skipping."
    fi
done

echo "[INFO] Removing network: ${NETWORK_NAME}"
if podman network exists "${NETWORK_NAME}" 2>/dev/null; then
    podman network rm "${NETWORK_NAME}" 2>/dev/null || true
fi

echo ""
echo "[DONE] Flink Session Cluster has been stopped and cleaned up."

