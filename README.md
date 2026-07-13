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
bash scripts/start-all.sh
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
    ├── serialization/
    │   ├── TransactionDeserializer.java     # Kafka 反序列化器
    │   └── AlertSerializer.java             # Kafka 序列化器
    └── monthly/                              # Job 2：每月消费统计 + 额度分级
        ├── MonthlyCreditLevelJob.java        # Job 入口
        ├── model/
        │   ├── MonthlyConsumptionFact.java   # Drools input/output 共用 fact
        │   └── CardLimitUpdate.java          # card_limit CDC 變更事件（upsert/delete）
        ├── cdc/
        │   └── CardLimitDebeziumDeserializer.java # Debezium SourceRecord -> CardLimitUpdate
        ├── rule/
        │   └── CardLevelRuleEngine.java      # Drools KieBase/KieSession 封装
        ├── process/
        │   └── MonthlyAggregationFunction.java # 每月累加 + broadcast state join + 月结触发规则引擎
        └── serialization/
            └── LevelResultSerializer.java    # 分级结果 Kafka 序列化器
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

## Job 2：每月信用卡消费统计与额度分级（MonthlyCreditLevelJob）

在既有 CEP 风控 Job 之外，新增独立的 Flink Job，用来即时统计「每张卡、每个月」的消费总金额，并在月结时透过 Drools 决策表规则引擎计算出该卡的额度分级建议。

### 系统架构

```
Kafka (credit-card-transactions)          PostgreSQL (card_limit 表, wal_level=logical)
          ↓                                         ↓ WAL 邏輯複製 (pgoutput)
   依 cardNumber 分组                       Flink CDC (flink-connector-postgres-cdc)
   即时累加当月消费金额（仅 log，不立即输出）      ↓ CardLimitDebeziumDeserializer
          ↓ 处理时间跨月触发 timer           CardLimitUpdate（upsert / delete）
          │                                         ↓ broadcast
          └──────────── KeyedBroadcastProcessFunction ┘
                  （broadcast state 中 materialize 成「額度上限表」）
                          ↓
                Drools 决策表规则引擎（CardLevelRules.xlsx）
                计算 status / description
                          ↓
                Kafka (credit-card-level-results)
```

- **数据来源**：与 `Main.java` 完全相同的 Kafka topic（`credit-card-transactions`）与 `CreditCardTransaction` 资料结构，`Deserializer` 亦沿用 `TransactionDeserializer`。
- **聚合逻辑**：`MonthlyAggregationFunction`（`KeyedBroadcastProcessFunction`，依 `cardNumber` 分组）在 Flink State 中累加当月金额，每笔交易到达时更新并记录 log；使用处理时间 timer，在下个月开始时触发月结运算。
- **额度上限 enrichment（stream + table，Postgres CDC 版）**：不使用逐笔 JDBC 查询，改以 **Flink CDC**（`flink-connector-postgres-cdc`）透过 PostgreSQL **WAL 逻辑复制**（`pgoutput` plugin，PostgreSQL 内建、无需额外安装）即时捕捉 `card_limit` 表的 INSERT / UPDATE / DELETE。CDC 事件经 `CardLimitDebeziumDeserializer` 转换为 `CardLimitUpdate`（含 upsert / delete 语意），再以 broadcast stream 广播给每个 keyed 分区，在 `MonthlyAggregationFunction` 的本地 broadcast state 中 materialize 成一张「额度上限表」，供每次月结时查询。此设计概念上对应 **ksqlDB 的 stream + table join**（card_limit 的 changelog 被同步成本地 KTable），相较定期轮询或逐笔 JDBC 查询：
  - 額度變更為**毫秒級即時同步**，且對 PostgreSQL 幾乎零額外查詢負載（讀取 WAL，而非執行 SELECT）
  - CDC 事件本身即為 changelog，天然具備**異動留痕／可追溯性**，較符合金融業對參考資料異動的稽核要求
  - 啟動時 CDC 會先做一次全表 snapshot（`StartupOptions.initial()`），之後無縫切換到持續監聽 WAL，故不需另外撰寫輪詢邏輯
- **规则引擎**：`CardLevelRuleEngine` 使用 Drools 10（`drools-mvel` + `drools-decisiontables`，embedded 方式以 Maven 引入）载入 `src/main/resources/rules/CardLevelRules.xlsx` 决策表；Fact 物件 `MonthlyConsumptionFact` 同时作为规则的 input（`monthlyAmount`、`cardLimit`）与 output（`status`、`description`），不额外拆分物件。
- **输出**：分级结果以 JSON 序列化（`LevelResultSerializer`），输出到新 Kafka topic `credit-card-level-results`。

### Enrichment 方案比较（为何选择 Postgres CDC）

在设计階段曾比較三種額度上限 enrichment 方式：

| 方案 | 即時性 | 對來源庫負載 | 稽核／留痕 | 適用場景 |
|---|---|---|---|---|
| **A. Postgres CDC（採用）** | 毫秒級 | 低（讀 WAL） | 佳（天然 changelog） | 金融業標準做法，額度等參考資料需要即時性與異動留痕 |
| B. 定期輪詢全表 + Broadcast State | 輪詢間隔級 | 中（週期性全表 SELECT） | 弱 | 資源有限、可接受分鐘級延遲的 PoC/MVP |
| C. Flink SQL JDBC Lookup Join | 快取 TTL 級 | 中低 | 弱 | 高基數、依 key 各自查詢的場景（如商戶資訊），非本情境最佳選擇 |

考量本專案的額度上限資料屬於金融業常見的低頻但關鍵參考資料（變更需可追溯、正確性要求高），最終選擇方案 A（Postgres CDC）。

### 决策表规则（CardLevelRules.xlsx）

| 条件 | status | description |
|---|---|---|
| 卡额度上限 <= 100000 | 1 | 建议提升上限 |
| 100000 < 卡额度上限 <= 500000 且 当月消费金额 >= 上限 - 5000 | 2 | 自动提高上限 |
| 100000 < 卡额度上限 <= 500000 且 当月消费金额 < 上限 - 5000 | 3 | 需促销 |
| 500000 < 卡额度上限 <= 1000000 且 当月消费金额 >= 上限 - 5000 | 4 | 可换发黑卡 |
| 500000 < 卡额度上限 <= 1000000 且 当月消费金额 < 上限 - 5000 | 5 | 不需调整 |
| 卡额度上限 > 1000000 | 5 | 不需调整 |

规则以 Excel 决策表撰写，可直接修改 `CardLevelRules.xlsx` 中的数据行调整门槛，无需改动 Java 代码；修改后请以 `mvn test -Dtest=CardLevelRuleEngineTest` 验证各分支仍正确命中。

### PostgreSQL（card_limit 表 + WAL 邏輯複製）

`scripts/start-flink-cluster.sh` 已加入 PostgreSQL（版本 17，podman 启动，与 Flink 集群同一个 network），并已開啟 **WAL 邏輯複製**（`wal_level=logical`、`max_replication_slots=4`、`max_wal_senders=4`），供 Flink CDC 使用；容器启动时会自动挂载 `conf/postgres/init/` 下的 SQL 建立资料：

- `01-ddl.sql`：建立 `card_limit` 表（`card_number` 为主键、`credit_limit` 为该卡当月额度上限），並設定 `REPLICA IDENTITY FULL`（確保 UPDATE/DELETE 的 WAL 紀錄攜帶完整欄位值）與賦予使用者 `REPLICATION` 權限。
- `02-dml.sql`：写入测试资料，包含与本 README 交易范例相同的卡号（如 `****1234`），以及一批随机产生的卡号与额度上限，供各分级规则分支测试使用。

CDC 連線參數（`MonthlyCreditLevelJob` 中設定，如需調整請同步修改 `scripts/start-flink-cluster.sh`）：

- Host/Port：`postgres:5432`，Database：`carddb`，Table：`public.card_limit`
- 使用者/密码：`carduser` / `cardpass`
- Decoding Plugin：`pgoutput`（PostgreSQL 內建，無需額外安裝 wal2json 等擴充套件）
- Replication Slot：`monthly_card_level_slot`
- Startup Mode：`StartupOptions.initial()`（啟動時先做一次全表 snapshot，之後接續監聽 WAL）

验证邏輯複製是否已啟用：

```bash
podman exec postgres psql -U carduser -d carddb -c 'SHOW wal_level;'
podman exec postgres psql -U carduser -d carddb -c 'SELECT * FROM pg_replication_slots;'
```

### 运行方式

```bash
# 1. 启动集群（含 Flink / HDFS / PostgreSQL）
bash scripts/start-all.sh

# 2. 编译打包
mvn clean package

# 3. 建立新 Kafka topic（分级结果输出用）
kafka-topics.sh --create \
  --bootstrap-server kafka:9092 \
  --topic credit-card-level-results \
  --partitions 3 \
  --replication-factor 1

# 4. 提交 Job（需指定主类，因预设 shade 主类为 Main）
bash scripts/submit-job.sh untitled1-1.0-SNAPSHOT.jar --class org.example.monthly.MonthlyCreditLevelJob

# 5. 消费分级结果
kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic credit-card-level-results --from-beginning
```

### 单元测试

- `CardLevelRuleEngineTest`：验证决策表 5 种分级（status 1~5）各分支皆正确命中。
- `CardLimitDebeziumDeserializerTest`：驗證 Debezium change event（insert/update/delete/初始 snapshot）皆能正確轉換為 `CardLimitUpdate`。
- `MonthlyAggregationFunctionTest`：验证月结前不输出、broadcast state upsert/delete 後的額度上限查詢結果、跨月后才输出正确的分级结果。

```bash
mvn test
```

> **測試限制說明**：本專案開發環境無 Docker/Testcontainers 可用，因此無法針對 Flink CDC 做端對端整合測試（例如啟動一個具備 WAL 邏輯複製的 PostgreSQL 容器，驗證 `MonthlyCreditLevelJob` 從真實 CDC 事件到 Kafka 輸出的完整流程）。上述單元測試僅涵蓋 CDC 轉換邏輯與下游 broadcast state 處理邏輯本身；正式導入前，建議在具備 Docker 的環境依照 `scripts/start-flink-cluster.sh` 啟動完整叢集，手動驗證 `card_limit` 表異動能即時反映到分級結果。

## 许可证

本项目仅供学习和参考使用。

## 联系方式

如有问题，请提交 Issue 或联系项目维护者。

