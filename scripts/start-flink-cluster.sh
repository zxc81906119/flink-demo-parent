#!/usr/bin/env bash
set -euo pipefail

##############################################################################
# Apache Flink Session Cluster — Podman 啟動腳本
# Flink LTS Version: 1.20.1
# 架構: 1 HDFS NameNode + 1 DataNode + 1 JobManager + 2 TaskManagers + 1 Flink Client
##############################################################################

FLINK_VERSION="1.20.1"
FLINK_IMAGE="docker.io/apache/flink:${FLINK_VERSION}"
HADOOP_VERSION="3"
HADOOP_IMAGE="docker.io/apache/hadoop:${HADOOP_VERSION}"
KAFKA_VERSION="7.9.0"
KAFKA_IMAGE="docker.io/confluentinc/cp-kafka:${KAFKA_VERSION}"
KAFKA_UI_IMAGE="docker.io/kafbat/kafka-ui:latest"
POSTGRES_VERSION="17"
POSTGRES_IMAGE="docker.io/library/postgres:${POSTGRES_VERSION}"
POSTGRES_DB="carddb"
POSTGRES_USER="carduser"
POSTGRES_PASSWORD="cardpass"
NETWORK_NAME="flink-network"
CONTAINER_PREFIX="flink"
TZ="Asia/Taipei"

# Hadoop 3.x Client JARs + 必要依賴 (從 Maven Central 下載)
# hadoop-client-api    : Hadoop client 所有 API 介面
# hadoop-client-runtime: 執行時期 uber-jar（已 shaded，不與 Flink 衝突）
# commons-logging      : Hadoop FileSystem 直接依賴（未被 runtime jar shaded）
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
# Kafka 資料持久化目錄
KAFKA_DATA_HOST="${PROJECT_DIR}/data/kafka"
# PostgreSQL 資料持久化目錄 + init sql 目錄
POSTGRES_DATA_HOST="${PROJECT_DIR}/data/postgres"
POSTGRES_INIT_HOST="${PROJECT_DIR}/conf/postgres/init"

echo "=========================================="
echo " Flink Session Cluster + HDFS + Kafka (Podman)"
echo " Flink Version    : ${FLINK_VERSION}"
echo " Hadoop Image     : ${HADOOP_IMAGE}"
echo " Hadoop Client    : ${HADOOP_CLIENT_VERSION}"
echo " Kafka Image      : ${KAFKA_IMAGE}"
echo " Kafka UI Image   : ${KAFKA_UI_IMAGE}"
echo " Postgres Image   : ${POSTGRES_IMAGE}"
echo " Network          : ${NETWORK_NAME}"
echo "=========================================="

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

# ------------------------------------------------------------------
# 4c. 啟動 PostgreSQL (LTS 穩定版本，用於存放信用卡當月額度上限)
#     使用官方 init sql 機制：掛載 conf/postgres/init 至
#     /docker-entrypoint-initdb.d，容器首次啟動（資料目錄為空）時
#     會依檔名順序自動執行其中的 DDL / DML sql
# ------------------------------------------------------------------
mkdir -p "${POSTGRES_DATA_HOST}"

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
    "${POSTGRES_IMAGE}"

echo "[INFO] Waiting for PostgreSQL to initialize (10 seconds)..."
sleep 10
echo "[INFO] PostgreSQL started. JDBC URL: jdbc:postgresql://postgres:5432/${POSTGRES_DB}"

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
#echo "[INFO] Starting TaskManager 2 (debug port: 5006)..."
#podman run -d \
#    --restart always \
#    --name "${CONTAINER_PREFIX}-taskmanager-2" \
#    --network "${NETWORK_NAME}" \
#    -p 5006:5006 \
#    -e TZ=${TZ} \
#    -e JOB_MANAGER_RPC_ADDRESS=jobmanager \
#    -e TASK_MANAGER_NUMBER_OF_TASK_SLOTS=2 \
#    -e HADOOP_CONF_DIR=${HADOOP_CONF_CONTAINER} \
#    -e HADOOP_CLASSPATH="${HADOOP_LIB_CONTAINER}/*" \
#    -e FLINK_ENV_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5006" \
#    -v "${HADOOP_CONF_HOST}:${HADOOP_CONF_CONTAINER}:z" \
#    -v "${FLINK_CONF_HOST}/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:z" \
#    -v "${HADOOP_LIB_HOST}:${HADOOP_LIB_CONTAINER}:z" \
#    "${FLINK_IMAGE}" \
#    taskmanager

# ------------------------------------------------------------------
# 8. 啟動 Flink Client (掛載專案 target/ + Hadoop 設定檔 + Hadoop 3.x jars)
#    容器保持執行狀態，使用者可透過 podman exec 進入提交 job
# ------------------------------------------------------------------
echo "[INFO] Starting Flink Client..."
podman run -d \
    --restart always \
    --name "${CONTAINER_PREFIX}-client" \
    --network "${NETWORK_NAME}" \
    -e TZ=${TZ} \
    -e JOB_MANAGER_RPC_ADDRESS=jobmanager \
    -e HADOOP_CONF_DIR=${HADOOP_CONF_CONTAINER} \
    -e HADOOP_CLASSPATH="${HADOOP_LIB_CONTAINER}/*" \
    -v "${PROJECT_DIR}/target:/opt/flink/usrlib:z" \
    -v "${HADOOP_CONF_HOST}:${HADOOP_CONF_CONTAINER}:z" \
    -v "${FLINK_CONF_HOST}/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:z" \
    -v "${HADOOP_LIB_HOST}:${HADOOP_LIB_CONTAINER}:z" \
    "${FLINK_IMAGE}" \
    sleep infinity

echo ""
echo "=========================================="
echo " Flink Session Cluster + HDFS + Kafka is UP!"
echo "=========================================="
echo ""
echo " HDFS NameNode    : http://localhost:9870"
echo " HDFS RPC         : hdfs://namenode:9000"
echo " Kafka Bootstrap  : localhost:9092 (internal: kafka:9092)"
echo " Kafka UI         : http://localhost:9080"
echo " Kafka Data Dir   : ${KAFKA_DATA_HOST}"
echo " Postgres JDBC    : jdbc:postgresql://localhost:5432/${POSTGRES_DB} (internal: postgres:5432)"
echo " Postgres User/Pw : ${POSTGRES_USER} / ${POSTGRES_PASSWORD}"
echo " JobManager WebUI : http://localhost:8081"
echo " TaskManagers     : 2 (each with 2 task slots)"
echo " TM-1 Debug Port  : localhost:5005"
echo " TM-2 Debug Port  : localhost:5006"
echo " Flink Client     : ${CONTAINER_PREFIX}-client"
echo " Checkpoint Path  : hdfs://namenode:9000/flink/checkpoints/fraud-detection"
echo " Hadoop Client    : hadoop-client-api + hadoop-client-runtime ${HADOOP_CLIENT_VERSION}"
echo ""
echo " 驗證 HDFS 狀態:"
echo "   podman exec hadoop-namenode hdfs dfsadmin -report"
echo "   podman exec hadoop-namenode hdfs dfs -ls /flink/checkpoints"
echo ""
echo " 驗證 Kafka 狀態:"
echo "   podman exec kafka kafka-topics --bootstrap-server kafka:9092 --list"
echo "   podman exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic test --partitions 1 --replication-factor 1"
echo ""
echo " 驗證 PostgreSQL 狀態:"
echo "   podman exec postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c 'SELECT * FROM card_limit;'"
echo ""
echo " 提交 Job 範例:"
echo "   podman exec ${CONTAINER_PREFIX}-client flink run -m jobmanager:8081 /opt/flink/usrlib/<your-jar>.jar"
echo ""
echo " Remote Debug (IntelliJ):"
echo "   Run → Edit Configurations → + → Remote JVM Debug"
echo "   Host: localhost, Port: 5005 (TM-1) or 5006 (TM-2)"
echo ""
echo " 或使用 scripts/submit-job.sh <jar-filename>"
echo "=========================================="
