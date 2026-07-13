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