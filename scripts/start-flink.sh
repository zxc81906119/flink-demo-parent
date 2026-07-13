#!/usr/bin/env bash
set -euo pipefail

FLINK_VERSION="1.20.1"
FLINK_IMAGE="docker.io/apache/flink:${FLINK_VERSION}"
NETWORK_NAME="flink-network"
CONTAINER_PREFIX="flink"
TZ="Asia/Taipei"

HADOOP_CLIENT_VERSION="3.3.6"
COMMONS_LOGGING_VERSION="1.2"

# 格式: "jar檔名|完整下載URL"
HADOOP_DOWNLOAD_LIST=(
    "hadoop-client-api-${HADOOP_CLIENT_VERSION}.jar|https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_CLIENT_VERSION}/hadoop-client-api-${HADOOP_CLIENT_VERSION}.jar"
    "hadoop-client-runtime-${HADOOP_CLIENT_VERSION}.jar|https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_CLIENT_VERSION}/hadoop-client-runtime-${HADOOP_CLIENT_VERSION}.jar"
    "commons-logging-${COMMONS_LOGGING_VERSION}.jar|https://repo1.maven.org/maven2/commons-logging/commons-logging/${COMMONS_LOGGING_VERSION}/commons-logging-${COMMONS_LOGGING_VERSION}.jar"
)

# 專案根目錄 (腳本位於 scripts/ 下)
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Hadoop 設定檔目錄
HADOOP_CONF_HOST="${PROJECT_DIR}/conf/hadoop"
# Flink 設定檔目錄
FLINK_CONF_HOST="${PROJECT_DIR}/conf/flink"
# Hadoop 設定檔在容器內的掛載路徑
HADOOP_CONF_CONTAINER="/opt/flink/hadoop-conf"
# Hadoop jars 本地存放目錄
HADOOP_LIB_HOST="${PROJECT_DIR}/lib/hadoop"
# Hadoop jars 在 Flink 容器內的掛載路徑
HADOOP_LIB_CONTAINER="/opt/flink/hadoop-lib"

# ------------------------------------------------------------------
# 0. 從 Maven Central 下載 Hadoop 3.x Client JARs + 依賴 (若不存在)
#    hadoop-client-api + hadoop-client-runtime + commons-logging
# ------------------------------------------------------------------
mkdir -p "${HADOOP_LIB_HOST}"
echo "[INFO] Checking Hadoop client jars..."
for entry in "${HADOOP_DOWNLOAD_LIST[@]}"; do
    jar_name="${entry%%|*}"
    download_url="${entry##*|}"
    jar_path="${HADOOP_LIB_HOST}/${jar_name}"
    if [ ! -f "${jar_path}" ]; then
        echo "[INFO] Downloading ${jar_name} from Maven Central..."
        echo "       URL: ${download_url}"

        unset MSYS_NO_PATHCONV  # 避免 Windows Git Bash 對路徑轉換
        curl -fSL -o "${jar_path}" "${download_url}"
        if [ -f "${jar_path}" ]; then
            echo "[INFO] Downloaded: ${jar_name} ($(ls -lh "${jar_path}" | awk '{print $5}'))"
        else
            echo "[ERROR] Failed to download ${jar_name}!"
            echo "[ERROR] Please manually download from: ${download_url}"
            echo "[ERROR] And place it in: ${HADOOP_LIB_HOST}/"
            exit 1
        fi
    else
        echo "[INFO] Already exists: ${jar_name}"
    fi
done

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
# 5. 啟動 JobManager (掛載 Hadoop 設定檔 + Hadoop 3.x jars)
# ------------------------------------------------------------------
echo "[INFO] Starting JobManager..."
podman run -d \
    --restart always \
    --name "${CONTAINER_PREFIX}-jobmanager" \
    --network "${NETWORK_NAME}" \
    --hostname jobmanager \
    -p 8081:8081 \
    -e TZ=${TZ} \
    -e JOB_MANAGER_RPC_ADDRESS=jobmanager \
    -e HADOOP_CONF_DIR=${HADOOP_CONF_CONTAINER} \
    -e HADOOP_CLASSPATH="${HADOOP_LIB_CONTAINER}/*" \
    -v "${HADOOP_CONF_HOST}:${HADOOP_CONF_CONTAINER}:z" \
    -v "${FLINK_CONF_HOST}/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:z" \
    -v "${HADOOP_LIB_HOST}:${HADOOP_LIB_CONTAINER}:z" \
    "${FLINK_IMAGE}" \
    jobmanager

echo "[INFO] JobManager started. WebUI available at http://localhost:8081"

# ------------------------------------------------------------------
# 6. 啟動 TaskManager 1 (掛載 Hadoop 設定檔 + Hadoop 3.x jars)
#    Remote Debug Port: 5005
# ------------------------------------------------------------------
echo "[INFO] Starting TaskManager 1 (debug port: 5005)..."
podman run -d \
    --restart always \
    --name "${CONTAINER_PREFIX}-taskmanager-1" \
    --network "${NETWORK_NAME}" \
    -p 5005:5005 \
    -e TZ=${TZ} \
    -e JOB_MANAGER_RPC_ADDRESS=jobmanager \
    -e TASK_MANAGER_NUMBER_OF_TASK_SLOTS=3 \
    -e HADOOP_CONF_DIR=${HADOOP_CONF_CONTAINER} \
    -e HADOOP_CLASSPATH="${HADOOP_LIB_CONTAINER}/*" \
    -e FLINK_ENV_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005" \
    -v "${HADOOP_CONF_HOST}:${HADOOP_CONF_CONTAINER}:z" \
    -v "${FLINK_CONF_HOST}/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:z" \
    -v "${HADOOP_LIB_HOST}:${HADOOP_LIB_CONTAINER}:z" \
    "${FLINK_IMAGE}" \
    taskmanager

# ------------------------------------------------------------------
# 7. 啟動 TaskManager 2 (掛載 Hadoop 設定檔 + Hadoop 3.x jars)
#    Remote Debug Port: 5006
# ------------------------------------------------------------------
echo "[INFO] Starting TaskManager 2 (debug port: 5006)..."
podman run -d \
    --restart always \
    --name "${CONTAINER_PREFIX}-taskmanager-2" \
    --network "${NETWORK_NAME}" \
    -p 5006:5006 \
    -e TZ=${TZ} \
    -e JOB_MANAGER_RPC_ADDRESS=jobmanager \
    -e TASK_MANAGER_NUMBER_OF_TASK_SLOTS=3 \
    -e HADOOP_CONF_DIR=${HADOOP_CONF_CONTAINER} \
    -e HADOOP_CLASSPATH="${HADOOP_LIB_CONTAINER}/*" \
    -e FLINK_ENV_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5006" \
    -v "${HADOOP_CONF_HOST}:${HADOOP_CONF_CONTAINER}:z" \
    -v "${FLINK_CONF_HOST}/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:z" \
    -v "${HADOOP_LIB_HOST}:${HADOOP_LIB_CONTAINER}:z" \
    "${FLINK_IMAGE}" \
    taskmanager

# ------------------------------------------------------------------
# 8. 啟動 Flink Client (掛載專案 target/ + Hadoop 設定檔 + Hadoop 3.x jars)
#    容器保持執行狀態，使用者可透過 podman exec 進入提交 job
# ------------------------------------------------------------------
echo "[INFO] Starting Flink Client..."
mkdir -p "${PROJECT_DIR}/app/jars"
podman run -d \
    --restart always \
    --name "${CONTAINER_PREFIX}-client" \
    --network "${NETWORK_NAME}" \
    -e TZ=${TZ} \
    -e JOB_MANAGER_RPC_ADDRESS=jobmanager \
    -e HADOOP_CONF_DIR=${HADOOP_CONF_CONTAINER} \
    -e HADOOP_CLASSPATH="${HADOOP_LIB_CONTAINER}/*" \
    -v "${PROJECT_DIR}/app/jars:/opt/flink/usrlib:z" \
    -v "${HADOOP_CONF_HOST}:${HADOOP_CONF_CONTAINER}:z" \
    -v "${FLINK_CONF_HOST}/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:z" \
    -v "${HADOOP_LIB_HOST}:${HADOOP_LIB_CONTAINER}:z" \
    "${FLINK_IMAGE}" \
    sleep infinity

