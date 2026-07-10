# Apache Flink 信用卡交易实时风控系统

基于 Apache Flink CEP (Complex Event Processing) 的信用卡交易欺诈检测系统，实时检测同一用户在 1 分钟内严格连续发生 3 笔交易的异常行为。

## 系统架构

```
Kafka (credit-card-transactions)
          ↓
    Flink CEP 检测引擎
    - Event Time 语义
    - RocksDB 状态后端
    - HDFS Checkpoint
    - 严格连续模式匹配
    - 告警去重机制
          ↓
Kafka (fraud-alerts)
```

## 技术栈

- **Apache Flink**: 1.20.1 (LTS)
- **Flink CEP**: 复杂事件处理
- **Kafka Connector**: 3.1.0
- **状态后端**: RocksDB (EmbeddedRocksDBStateBackend)
- **Checkpoint 存储**: HDFS (hdfs://namenode:9000)
- **序列化**: Jackson JSON
- **并行度**: 3 (匹配 Kafka partition 数量)

## 数据模型

### 输入：信用卡交易事件 (CreditCardTransaction)

从 Kafka topic `credit-card-transactions` 读取，JSON 格式：

```json
{
  "transactionId": "TXN_20260709_001",
  "userId": "USER_12345",
  "amount": 199.99,
  "currency": "USD",
  "merchantId": "MERCHANT_456",
  "timestamp": 1720512345000,
  "cardNumber": "****1234"
}
```

**字段说明：**
- `transactionId`: 交易唯一标识
- `userId`: 用户 ID（用于分组检测）
- `amount`: 交易金额
- `currency`: 币种（USD, CNY, EUR 等）
- `merchantId`: 商户 ID
- `timestamp`: 交易时间戳（毫秒，Event Time）
- `cardNumber`: 卡号（脱敏，仅后4位）

### 输出：欺诈告警事件 (FraudAlert)

发送到 Kafka topic `fraud-alerts`，JSON 格式：

```json
{
  "alertId": "USER_12345-1720512345000",
  "userId": "USER_12345",
  "alertType": "RAPID_CONSECUTIVE_TRANSACTIONS",
  "transactionIds": ["TXN_001", "TXN_002", "TXN_003"],
  "totalAmount": 599.97,
  "windowStart": 1720512345000,
  "windowEnd": 1720512385000,
  "detectedTime": 1720512400000,
  "message": "用户 USER_12345 在 1 分钟内发生 3 笔严格连续交易，总金额: 599.97"
}
```

**字段说明：**
- `alertId`: 告警唯一 ID（格式: `{userId}-{firstTxnTimestamp}`，用于去重）
- `userId`: 触发告警的用户 ID
- `alertType`: 告警类型（RAPID_CONSECUTIVE_TRANSACTIONS）
- `transactionIds`: 涉及的 3 笔交易 ID 列表
- `totalAmount`: 3 笔交易的总金额
- `windowStart`: 时间窗口开始时间（第一笔交易时间）
- `windowEnd`: 时间窗口结束时间（第三笔交易时间）
- `detectedTime`: 告警检测时间（系统时间）
- `message`: 告警描述信息

## CEP 检测规则

使用 Flink CEP 定义的模式：

```java
Pattern.<CreditCardTransaction>begin("first")
    .times(3)           // 连续 3 次
    .consecutive()      // 严格连续（中间不能有其他交易）
    .within(Time.minutes(1)); // 1 分钟内
```

**规则说明：**
- **严格连续**: 必须是连续的 3 笔交易，中间不能有其他交易事件
- **时间窗口**: 从第一笔到第三笔交易的时间间隔不超过 1 分钟
- **用户隔离**: 按 `userId` 分组，不同用户的交易互不影响
- **告警去重**: 使用 `alertId = userId + "-" + firstTxnTimestamp` 确保相同窗口只产生一次告警

## 部署与运行

### 前置条件

1. Podman 已安装
2. Maven 已安装
3. Kafka 集群运行在 `kafka:9092`（或修改 `Main.java` 中的配置）

### 1. 启动 Flink 集群（含 HDFS）

```bash
cd D:\ij_proj\untitled1
bash scripts/start-flink-cluster.sh
```

启动后可访问：
- **Flink WebUI**: http://localhost:8081
- **HDFS WebUI**: http://localhost:9870

### 2. 编译打包

```bash
mvn clean package
```

生成 JAR：`target/untitled1-1.0-SNAPSHOT.jar`

### 3. 提交 Flink Job

```bash
bash scripts/submit-job.sh untitled1-1.0-SNAPSHOT.jar
```

或直接指定主类：

```bash
bash scripts/submit-job.sh untitled1-1.0-SNAPSHOT.jar --class org.example.Main
```

### 4. 监控 Job 状态

访问 Flink WebUI: http://localhost:8081

- 查看 Job 状态、并行度、Checkpoint
- 查看 TaskManager 资源使用情况
- 查看任务拓扑图

### 5. 停止集群

```bash
bash scripts/stop-flink-cluster.sh
```

## Kafka Topic 准备

### 创建 Topics（示例，使用 Kafka CLI）

```bash
# 创建交易输入 topic（3 partitions）
kafka-topics.sh --create \
  --bootstrap-server kafka:9092 \
  --topic credit-card-transactions \
  --partitions 3 \
  --replication-factor 1

# 创建告警输出 topic（3 partitions）
kafka-topics.sh --create \
  --bootstrap-server kafka:9092 \
  --topic fraud-alerts \
  --partitions 3 \
  --replication-factor 1
```

### 测试数据生产（示例）

向 `credit-card-transactions` topic 发送测试数据：

```bash
# 使用 kafka-console-producer
kafka-console-producer.sh --bootstrap-server kafka:9092 --topic credit-card-transactions

# 手动输入以下 JSON（模拟同一用户连续 3 笔交易）
{"transactionId":"TXN_001","userId":"USER_001","amount":100.0,"currency":"USD","merchantId":"M_001","timestamp":1721512345000,"cardNumber":"****1234"}
{"transactionId":"TXN_002","userId":"USER_001","amount":200.0,"currency":"USD","merchantId":"M_002","timestamp":1721512350000,"cardNumber":"****1234"}
{"transactionId":"TXN_003","userId":"USER_001","amount":300.0,"currency":"USD","merchantId":"M_003","timestamp":1721512355000,"cardNumber":"****1234"}
```

### 消费告警事件（示例）

```bash
kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic fraud-alerts --from-beginning
```

预期输出：

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

## 配置说明

### Checkpoint 配置

- **存储路径**: `hdfs://namenode:9000/flink/checkpoints/fraud-detection`
- **间隔**: 60 秒
- **语义**: Exactly-Once
- **超时**: 10 分钟
- **保留策略**: Job 取消后保留 checkpoint

### 状态后端

- **类型**: EmbeddedRocksDBStateBackend
- **增量 Checkpoint**: 启用

### 重启策略

- **类型**: Fixed Delay Restart
- **最大重启次数**: 3 次
- **重启间隔**: 10 秒

## 项目结构

```
untitled1/
├── pom.xml                                   # Maven 配置
├── README.md                                 # 项目说明（本文件）
├── scripts/
│   ├── start-flink-cluster.sh               # 启动 Flink + HDFS 集群
│   ├── stop-flink-cluster.sh                # 停止集群
│   └── submit-job.sh                        # 提交 Job 脚本
└── src/main/java/org/example/
    ├── Main.java                            # 主程序（CEP 检测逻辑）
    ├── model/
    │   ├── CreditCardTransaction.java       # 交易事件 POJO
    │   └── FraudAlert.java                  # 告警事件 POJO
    └── serialization/
        ├── TransactionDeserializer.java     # Kafka 反序列化器
        └── AlertSerializer.java             # Kafka 序列化器
```

## 关键特性

### 1. 严格连续检测

使用 `.consecutive()` 确保检测的是严格连续的 3 笔交易，避免漏检或误检。

### 2. 告警去重

通过 `alertId = userId + "-" + firstTxnTimestamp` 确保相同时间窗口的交易只产生一次告警，即使后续有第 4、5 笔交易也不会重复告警。

### 3. Event Time 语义

基于交易的 `timestamp` 字段进行时间窗口计算，不受处理延迟影响。

### 4. 高可用保障

- **RocksDB 状态后端**: 支持大规模状态存储
- **HDFS Checkpoint**: 提供故障恢复能力
- **Exactly-Once 语义**: 确保告警不丢失、不重复

## 性能优化建议

1. **并行度调优**: 当前设置为 3（匹配 Kafka partitions），可根据数据量调整
2. **Checkpoint 间隔**: 根据数据吞吐量调整（高吞吐场景可增加到 5-10 分钟）
3. **RocksDB 参数**: 可通过 `state.backend.rocksdb.*` 参数优化性能
4. **Kafka Consumer**: 调整 `fetch.min.bytes`、`max.poll.records` 等参数

## 监控与告警

建议集成以下监控：

1. **Flink Metrics**: 通过 Prometheus + Grafana 监控 Job 指标
2. **Checkpoint 监控**: 监控 checkpoint 成功率、耗时
3. **Kafka Lag**: 监控消费延迟
4. **告警数量**: 监控 fraud-alerts topic 的消息生产速率

## 扩展建议

1. **多规则支持**: 添加更多欺诈检测规则（如大额交易、异地交易等）
2. **动态配置**: 通过 ConfigMap 或数据库动态调整检测参数
3. **告警分级**: 根据交易金额、频率设置不同告警级别
4. **实时阻断**: 集成实时拦截机制，在告警后立即阻止可疑交易

## 故障排查

### Job 无法启动

1. 检查 Kafka 连接：`telnet kafka 9092`
2. 检查 HDFS 连接：访问 http://localhost:9870
3. 查看 JobManager 日志：`podman logs flink-jobmanager`

### Checkpoint 失败

1. 检查 HDFS 存储空间
2. 验证 checkpoint 路径权限
3. 查看 TaskManager 日志：`podman logs flink-taskmanager-1`

### 无告警产生

1. 确认 Kafka topic 有数据流入
2. 检查数据格式是否正确（JSON）
3. 查看 Flink WebUI 中的异常信息

## 许可证

本项目仅供学习和参考使用。

## 联系方式

如有问题，请提交 Issue 或联系项目维护者。

