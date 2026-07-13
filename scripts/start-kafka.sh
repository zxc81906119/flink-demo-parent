#!/usr/bin/env bash
set -euo pipefail

KAFKA_VERSION="7.9.0"
KAFKA_IMAGE="docker.io/confluentinc/cp-kafka:${KAFKA_VERSION}"
KAFKA_UI_IMAGE="docker.io/kafbat/kafka-ui:latest"
NETWORK_NAME="flink-network"
TZ="Asia/Taipei"

# 專案根目錄 (腳本位於 scripts/ 下)
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Kafka 資料持久化目錄
KAFKA_DATA_HOST="${PROJECT_DIR}/data/kafka"

export MSYS_NO_PATHCONV=1

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
# 4a. 啟動 Kafka Broker (Confluent CP-Kafka, KRaft Combined Mode)
# ------------------------------------------------------------------
mkdir -p "${KAFKA_DATA_HOST}"

# 持久化 Cluster ID：首次產生後寫入檔案，後續重啟直接讀取
KAFKA_CLUSTER_ID_FILE="${KAFKA_DATA_HOST}/.cluster-id"
if [ -f "${KAFKA_CLUSTER_ID_FILE}" ]; then
    KAFKA_CLUSTER_ID=$(cat "${KAFKA_CLUSTER_ID_FILE}")
    echo "[INFO] Reusing existing Kafka Cluster ID: ${KAFKA_CLUSTER_ID}"
else
    echo "[INFO] Generating new Kafka Cluster ID..."
    KAFKA_CLUSTER_ID=$(podman run --rm "${KAFKA_IMAGE}" kafka-storage random-uuid)
    echo "${KAFKA_CLUSTER_ID}" > "${KAFKA_CLUSTER_ID_FILE}"
    echo "[INFO] New Kafka Cluster ID: ${KAFKA_CLUSTER_ID} (saved to ${KAFKA_CLUSTER_ID_FILE})"
fi

echo "[INFO] Starting Kafka Broker (KRaft combined mode)..."
podman run -d \
    --restart always \
    --name "kafka" \
    --network "${NETWORK_NAME}" \
    --hostname kafka \
    -p 9092:9092 \
    -v "${KAFKA_DATA_HOST}:/var/lib/kafka/data:z" \
    -e TZ=${TZ} \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@kafka:9093" \
    -e KAFKA_LISTENERS="PLAINTEXT://:9092,CONTROLLER://:9093" \
    -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://kafka:9092" \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
    -e CLUSTER_ID="${KAFKA_CLUSTER_ID}" \
    "${KAFKA_IMAGE}"

echo "[INFO] Waiting for Kafka Broker to start (10 seconds)..."
sleep 10

# ------------------------------------------------------------------
# 4b. 啟動 Kafka UI (kafbat/kafka-ui)
# ------------------------------------------------------------------
echo "[INFO] Starting Kafka UI..."
podman run -d \
    --restart always \
    --name "kafka-ui" \
    --network "${NETWORK_NAME}" \
    --hostname kafka-ui \
    -p 9080:8080 \
    -e TZ=${TZ} \
    -e KAFKA_CLUSTERS_0_NAME=local \
    -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092 \
    "${KAFKA_UI_IMAGE}"

echo "[INFO] Kafka UI started. WebUI available at http://localhost:9080"

