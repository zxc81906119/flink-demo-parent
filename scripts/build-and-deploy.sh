#!/usr/bin/env bash
# 构建和部署 Flink 应用脚本

set -euo pipefail

echo "=========================================="
echo " Flink 信用卡风控系统 - 构建部署脚本"
echo "=========================================="

# 1. 清理并编译
echo "[1/4] 清理并编译项目..."
#JAVA_HOME="/c/Users/2302022/.jdks/corretto-11.0.31-1"
#PATH="$JAVA_HOME/bin:$PATH"


mvn clean package -DskipTests

# 2. 检查 JAR 文件
if [ ! -f "target/untitled1-1.0.0.jar" ]; then
    echo "[ERROR] JAR 文件未生成，编译失败！"
    exit 1
fi

echo "[SUCCESS] JAR 文件生成成功: target/untitled1-1.0.0.jar"
ls -lh target/untitled1-1.0.0.jar

# 3. 检查 Flink 集群是否运行
echo ""
echo "[2/4] 检查 Flink 集群状态..."
if ! podman container exists flink-jobmanager 2>/dev/null; then
    echo "[WARNING] Flink 集群未运行，正在启动..."
    bash scripts/start-all.sh
    echo "[INFO] 等待 Flink 集群初始化 (10 秒)..."
    sleep 10
else
    echo "[INFO] Flink 集群已在运行"
fi

# 3.5 检查 HDFS 是否运行
echo ""
echo "[2.5/4] 检查 HDFS 状态..."
if ! podman container exists hadoop-namenode 2>/dev/null; then
    echo "[ERROR] HDFS NameNode 未运行！请先执行 scripts/start-flink-cluster.sh"
    exit 1
else
    echo "[INFO] HDFS NameNode 已在运行"
    # 验证 HDFS 健康状态
    if podman exec hadoop-namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; then
        echo "[INFO] HDFS 已离开安全模式，状态正常"
    else
        echo "[WARNING] HDFS 可能仍在安全模式中，等待 10 秒..."
        sleep 10
    fi
fi

# 4. 提交 Job
echo ""
echo "[3/4] 提交 Flink Job..."
bash scripts/submit-job.sh untitled1-1.0.0.jar

echo ""
echo "=========================================="
echo " 部署完成！"
echo "=========================================="
echo ""
echo " Flink WebUI: http://localhost:8081"
echo " HDFS WebUI:  http://localhost:9870"
echo ""
echo "后续步骤："
echo "  1. 访问 Flink WebUI 查看 Job 状态"
echo "  2. 向 Kafka topic 'credit-card-transactions' 发送测试数据"
echo "  3. 从 Kafka topic 'fraud-alerts' 消费告警事件"
echo ""
echo "示例命令（需要 Kafka CLI）："
echo "  # 生产测试数据"
echo "  kafka-console-producer.sh --bootstrap-server kafka:9092 --topic credit-card-transactions"
echo ""
echo "  # 消费告警事件"
echo "  kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic fraud-alerts --from-beginning"
echo "=========================================="

