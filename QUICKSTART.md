# 快速入门指南

本指南帮助您快速部署和测试 Flink 信用卡风控系统。

## 前置条件

1. ✅ **Podman** 已安装并运行
2. ✅ **Maven** 已安装（`mvn -version` 验证）
3. ✅ **Kafka** 集群运行在 `kafka:9092`（或在网络中可访问）
4. ✅ **Python 3** （可选，用于生成测试数据）

## 5 分钟快速启动

### Step 1: 编译项目

```bash
cd D:\ij_proj\untitled1
mvn clean package
```

**预期输出**: `BUILD SUCCESS`，生成 `target/untitled1-1.0-SNAPSHOT.jar`

### Step 2: 启动 Flink 集群（含 HDFS）

```bash
bash scripts/start-flink-cluster.sh
```

**验证**:
- 访问 http://localhost:8081 （Flink WebUI）
- 访问 http://localhost:9870 （HDFS WebUI）

### Step 3: 创建 Kafka Topics

```bash
# 创建交易输入 topic
kafka-topics.sh --create \
  --bootstrap-server kafka:9092 \
  --topic credit-card-transactions \
  --partitions 3 \
  --replication-factor 1

# 创建告警输出 topic
kafka-topics.sh --create \
  --bootstrap-server kafka:9092 \
  --topic fraud-alerts \
  --partitions 3 \
  --replication-factor 1
```

### Step 4: 提交 Flink Job

```bash
bash scripts/submit-job.sh untitled1-1.0-SNAPSHOT.jar
```

**验证**: 在 Flink WebUI 中看到 Job 状态为 `RUNNING`

### Step 5: 发送测试数据

**方法 1: 使用 Python 脚本生成（推荐）**

```bash
# 生成测试数据并查看
python scripts/generate_test_data.py

# 生成并发送到 Kafka（需要 Kafka CLI）
python scripts/generate_test_data.py | kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic credit-card-transactions
```

**方法 2: 手动输入测试数据**

```bash
# 启动 Kafka producer
kafka-console-producer.sh --bootstrap-server kafka:9092 --topic credit-card-transactions

# 粘贴以下 JSON（每行一个，模拟同一用户连续 3 笔交易）
{"transactionId":"TXN_001","userId":"USER_001","amount":100.0,"currency":"USD","merchantId":"M_001","timestamp":1720512345000,"cardNumber":"****1234"}
{"transactionId":"TXN_002","userId":"USER_001","amount":200.0,"currency":"USD","merchantId":"M_002","timestamp":1720512350000,"cardNumber":"****1234"}
{"transactionId":"TXN_003","userId":"USER_001","amount":300.0,"currency":"USD","merchantId":"M_003","timestamp":1720512355000,"cardNumber":"****1234"}
```

### Step 6: 消费告警事件

```bash
kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic fraud-alerts \
  --from-beginning
```

**预期输出**（JSON 格式）:

```json
{
  "alertId":"USER_001-1720512345000",
  "userId":"USER_001",
  "alertType":"RAPID_CONSECUTIVE_TRANSACTIONS",
  "transactionIds":["TXN_001","TXN_002","TXN_003"],
  "totalAmount":600.0,
  "windowStart":1720512345000,
  "windowEnd":1720512355000,
  "detectedTime":1720512360000,
  "message":"用户 USER_001 在 1 分钟内发生 3 笔严格连续交易，总金额: 600.00"
}
```

## 一键部署（Windows PowerShell）

```powershell
.\scripts\build-and-deploy.ps1
```

## 一键部署（Linux/Mac Bash）

```bash
bash scripts/build-and-deploy.sh
```

## 测试场景说明

### 场景 1: 触发告警 ✅

**条件**: 同一用户（USER_001）在 1 分钟内严格连续 3 笔交易

```
TXN_001 (T+0s)  -> TXN_002 (T+10s) -> TXN_003 (T+20s)
```

**结果**: 生成告警事件到 `fraud-alerts` topic

### 场景 2: 不触发告警 ❌

**原因 1**: 交易间隔超过 1 分钟

```
TXN_004 (T+0s)  -> TXN_005 (T+90s)  # 超过 60 秒
```

**原因 2**: 非严格连续（中间有其他用户的交易）

```
USER_A: TXN_006 (T+0s)
USER_B: TXN_007 (T+5s)   # 中断了 USER_A 的连续性
USER_A: TXN_008 (T+10s)
USER_A: TXN_009 (T+15s)
```

**原因 3**: 同一用户但总时长超过 1 分钟

```
TXN_010 (T+0s)  -> TXN_011 (T+30s) -> TXN_012 (T+70s)  # 总计 70 秒
```

## 监控与调试

### 查看 Flink Job 详情

访问 http://localhost:8081，可查看：
- Job 拓扑图
- TaskManager 资源使用
- Checkpoint 状态
- 异常信息

### 查看容器日志

```bash
# JobManager 日志
podman logs flink-jobmanager

# TaskManager 日志
podman logs flink-taskmanager-1
podman logs flink-taskmanager-2

# HDFS NameNode 日志
podman logs hadoop-namenode
```

### 查看 HDFS Checkpoint 文件

访问 http://localhost:9870，导航到：
```
/flink/checkpoints/fraud-detection/
```

## 停止服务

```bash
# 停止 Flink 集群和 HDFS
bash scripts/stop-flink-cluster.sh
```

## 常见问题

### Q1: Kafka 连接失败

**问题**: `Could not resolve hostname kafka`

**解决**: 
1. 确保 Kafka 运行在 `flink-network` 网络中
2. 或修改 `Main.java` 中的 `KAFKA_BOOTSTRAP_SERVERS` 为实际地址（如 `localhost:9092`）

### Q2: HDFS 连接失败

**问题**: Checkpoint 失败，提示无法连接 HDFS

**解决**:
1. 访问 http://localhost:9870 确认 HDFS 运行正常
2. 检查 NameNode 是否完成初始化（等待 10-20 秒）
3. 查看 `podman logs hadoop-namenode` 日志

### Q3: 编译失败

**问题**: Maven 依赖下载失败

**解决**:
1. 检查网络连接
2. 配置 Maven 镜像（如阿里云镜像）
3. 删除 `~/.m2/repository` 缓存后重新编译

### Q4: 没有告警产生

**检查清单**:
- [ ] Flink Job 是否在 Running 状态？
- [ ] Kafka 数据是否正确发送？（检查 topic 消息数）
- [ ] 交易的 `timestamp` 是否在合理范围内？
- [ ] 是否满足严格连续 3 笔的条件？
- [ ] 查看 Flink WebUI 的异常信息

## 性能测试

### 生成高频测试数据

```python
# 创建 load_test.py
import json
import time
import random

def generate_load_test_data(num_users=100, transactions_per_user=10):
    base_timestamp = int(time.time() * 1000)
    
    for user_id in range(num_users):
        for txn_id in range(transactions_per_user):
            txn = {
                "transactionId": f"TXN_U{user_id}_T{txn_id}",
                "userId": f"USER_{user_id:03d}",
                "amount": round(random.uniform(10, 1000), 2),
                "currency": "USD",
                "merchantId": f"MERCHANT_{random.randint(1, 50)}",
                "timestamp": base_timestamp + (txn_id * 5000),  # 每 5 秒一笔
                "cardNumber": f"****{random.randint(1000, 9999)}"
            }
            print(json.dumps(txn))

if __name__ == "__main__":
    generate_load_test_data(num_users=100, transactions_per_user=10)
```

运行:
```bash
python load_test.py | kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic credit-card-transactions
```

## 下一步

- 📖 阅读 [README.md](README.md) 了解详细架构和配置
- 🔧 调整 CEP 检测规则（修改 `Main.java` 中的 Pattern）
- 📊 集成 Prometheus + Grafana 监控
- 🚀 部署到生产环境（K8s、Standalone Cluster）

## 参考资料

- [Apache Flink 官方文档](https://flink.apache.org/)
- [Flink CEP 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/)
- [Kafka Connector 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)

---

**祝您使用愉快！如有问题，请查看日志或提交 Issue。**

