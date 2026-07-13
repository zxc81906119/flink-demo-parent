#!/usr/bin/env bash
set -euo pipefail

CONTAINER_PREFIX="flink"
CLIENT_CONTAINER="${CONTAINER_PREFIX}-client"

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

APP_DIR="${PROJECT_DIR}/app"
JAR_DIR="${APP_DIR}/jars"

APP_NAME="$1"
shift
EXTRA_ARGS="${*:-}"

mvn clean package -f "${PROJECT_DIR}/pom.xml" -am -pl "app/${APP_NAME}" -DskipTests

mkdir -p "${JAR_DIR}/"

JAR_NAME="${APP_NAME}.jar"

JAR_FILE="${APP_DIR}/${APP_NAME}/target/${JAR_NAME}"

cp "${JAR_FILE}" "${JAR_DIR}/"

JOBMANAGER_ADDRESS="jobmanager:8081"

echo "[INFO] Submitting job: ${JAR_NAME}"
echo "[INFO] Target: ${JOBMANAGER_ADDRESS}"
echo "[INFO] Extra args: ${EXTRA_ARGS:-<none>}"
echo ""

USRLIB_PATH="/opt/flink/usrlib"

export MSYS_NO_PATHCONV=1

podman exec "${CLIENT_CONTAINER}" \
    flink run -m "${JOBMANAGER_ADDRESS}" \
    -d \
    -p 3 \
    ${EXTRA_ARGS} \
    "${USRLIB_PATH}/${JAR_NAME}"

podman exec "${CLIENT_CONTAINER}" \
    flink list -a -m "${JOBMANAGER_ADDRESS}"