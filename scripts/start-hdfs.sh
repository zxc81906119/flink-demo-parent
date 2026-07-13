#!/usr/bin/env bash
set -euo pipefail

HADOOP_VERSION="3"
HADOOP_IMAGE="docker.io/apache/hadoop:${HADOOP_VERSION}"
NETWORK_NAME="flink-network"
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
# Hadoop jars 本地存放目錄
HADOOP_LIB_HOST="${PROJECT_DIR}/lib/hadoop"

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
# 2. 啟動 Hadoop HDFS — NameNode
# ------------------------------------------------------------------
echo "[INFO] Starting HDFS NameNode..."
podman run -d \
    --restart always \
    --name "hadoop-namenode" \
    --network "${NETWORK_NAME}" \
    --hostname namenode \
    -p 9870:9870 \
    -p 9000:9000 \
    -e TZ=${TZ} \
    -e HADOOP_HOME=/opt/hadoop \
    -e ENSURE_NAMENODE_DIR="/tmp/hadoop-root/dfs/name" \
    -v "${HADOOP_CONF_HOST}/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:z" \
    -v "${HADOOP_CONF_HOST}/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:z" \
    "${HADOOP_IMAGE}" \
    hdfs namenode

echo "[INFO] Waiting for NameNode to start (15 seconds)..."
sleep 15

# ------------------------------------------------------------------
# 3. 啟動 Hadoop HDFS — DataNode
# ------------------------------------------------------------------
echo "[INFO] Starting HDFS DataNode..."
podman run -d \
    --restart always \
    --name "hadoop-datanode" \
    --network "${NETWORK_NAME}" \
    --hostname datanode \
    -e TZ=${TZ} \
    -e HADOOP_HOME=/opt/hadoop \
    -v "${HADOOP_CONF_HOST}/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:z" \
    -v "${HADOOP_CONF_HOST}/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:z" \
    "${HADOOP_IMAGE}" \
    hdfs datanode

echo "[INFO] Waiting for DataNode to register (10 seconds)..."
sleep 10

# ------------------------------------------------------------------
# 4. 在 HDFS 上建立 Flink checkpoint / savepoint 目錄
# ------------------------------------------------------------------
echo "[INFO] Creating HDFS directories for Flink checkpoints..."
podman exec hadoop-namenode hdfs dfs -mkdir -p /flink/checkpoints/fraud-detection
podman exec hadoop-namenode hdfs dfs -mkdir -p /flink/savepoints
podman exec hadoop-namenode hdfs dfs -chmod -R 777 /flink
echo "[INFO] HDFS directories created."
