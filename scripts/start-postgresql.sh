#!/usr/bin/env bash
set -euo pipefail

POSTGRES_VERSION="15"
POSTGRES_IMAGE="docker.io/library/postgres:${POSTGRES_VERSION}"
POSTGRES_DB="carddb"
POSTGRES_USER="carduser"
POSTGRES_PASSWORD="cardpass"
NETWORK_NAME="flink-network"
TZ="Asia/Taipei"

# 專案根目錄 (腳本位於 scripts/ 下)
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# PostgreSQL 資料持久化目錄 + init sql 目錄
POSTGRES_DATA_HOST="pg_data"
POSTGRES_INIT_HOST="${PROJECT_DIR}/conf/postgres/init"

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

echo "[INFO] Starting PostgreSQL..."
podman run -d \
    --restart always \
    --name "postgres" \
    --network "${NETWORK_NAME}" \
    --hostname postgres \
    -p 5432:5432 \
    -e TZ=${TZ} \
    -e POSTGRES_DB=${POSTGRES_DB} \
    -e POSTGRES_USER=${POSTGRES_USER} \
    -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
    -v "${POSTGRES_DATA_HOST}:/var/lib/postgresql/data:z" \
    -v "${POSTGRES_INIT_HOST}:/docker-entrypoint-initdb.d:z" \
    "${POSTGRES_IMAGE}" \
    -c wal_level=logical \
    -c max_replication_slots=4 \
    -c max_wal_senders=4

echo "[INFO] Waiting for PostgreSQL to initialize (10 seconds)..."
sleep 10
echo "[INFO] PostgreSQL started. JDBC URL: jdbc:postgresql://postgres:5432/${POSTGRES_DB}"
echo "[INFO] PostgreSQL logical replication enabled (wal_level=logical), ready for Flink CDC."

