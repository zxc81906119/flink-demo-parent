# Flink 信用卡风控系统 - Kafka 测试数据生成脚本
# 用于生成符合检测规则的测试数据

import json
import time
from datetime import datetime

def generate_test_transactions():
    """
    生成测试交易数据
    场景 1: 同一用户连续 3 笔交易（触发告警）
    场景 2: 同一用户非连续交易（不触发告警）
    场景 3: 不同用户各自交易（不触发告警）
    """

    base_timestamp = int(time.time() * 1000)

    print("=== 场景 1: USER_001 - 1分钟内严格连续 3 笔交易（应触发告警） ===")
    transactions_scenario1 = [
        {
            "transactionId": "TXN_001",
            "userId": "USER_001",
            "amount": 100.50,
            "currency": "USD",
            "merchantId": "MERCHANT_A",
            "timestamp": base_timestamp,
            "cardNumber": "****1234"
        },
        {
            "transactionId": "TXN_002",
            "userId": "USER_001",
            "amount": 250.75,
            "currency": "USD",
            "merchantId": "MERCHANT_B",
            "timestamp": base_timestamp + 10000,  # +10秒
            "cardNumber": "****1234"
        },
        {
            "transactionId": "TXN_003",
            "userId": "USER_001",
            "amount": 399.99,
            "currency": "USD",
            "merchantId": "MERCHANT_C",
            "timestamp": base_timestamp + 20000,  # +20秒
            "cardNumber": "****1234"
        }
    ]

    for txn in transactions_scenario1:
        print(json.dumps(txn))

    print("\n=== 场景 2: USER_002 - 有间隔的交易（不触发告警） ===")
    transactions_scenario2 = [
        {
            "transactionId": "TXN_004",
            "userId": "USER_002",
            "amount": 50.00,
            "currency": "CNY",
            "merchantId": "MERCHANT_D",
            "timestamp": base_timestamp + 30000,
            "cardNumber": "****5678"
        },
        {
            "transactionId": "TXN_005",
            "userId": "USER_002",
            "amount": 120.00,
            "currency": "CNY",
            "merchantId": "MERCHANT_E",
            "timestamp": base_timestamp + 90000,  # +90秒，超过1分钟
            "cardNumber": "****5678"
        }
    ]

    for txn in transactions_scenario2:
        print(json.dumps(txn))

    print("\n=== 场景 3: USER_003 - 严格连续 3 笔，但超过1分钟（不触发告警） ===")
    transactions_scenario3 = [
        {
            "transactionId": "TXN_006",
            "userId": "USER_003",
            "amount": 80.00,
            "currency": "EUR",
            "merchantId": "MERCHANT_F",
            "timestamp": base_timestamp + 100000,
            "cardNumber": "****9012"
        },
        {
            "transactionId": "TXN_007",
            "userId": "USER_003",
            "amount": 90.00,
            "currency": "EUR",
            "merchantId": "MERCHANT_G",
            "timestamp": base_timestamp + 130000,  # +30秒
            "cardNumber": "****9012"
        },
        {
            "transactionId": "TXN_008",
            "userId": "USER_003",
            "amount": 100.00,
            "currency": "EUR",
            "merchantId": "MERCHANT_H",
            "timestamp": base_timestamp + 170000,  # +40秒，总计70秒，超过60秒
            "cardNumber": "****9012"
        }
    ]

    for txn in transactions_scenario3:
        print(json.dumps(txn))

    print("\n=== 场景 4: USER_004 - 再次测试严格连续 3 笔（应触发告警） ===")
    transactions_scenario4 = [
        {
            "transactionId": "TXN_009",
            "userId": "USER_004",
            "amount": 500.00,
            "currency": "USD",
            "merchantId": "MERCHANT_I",
            "timestamp": base_timestamp + 180000,
            "cardNumber": "****3456"
        },
        {
            "transactionId": "TXN_010",
            "userId": "USER_004",
            "amount": 600.00,
            "currency": "USD",
            "merchantId": "MERCHANT_J",
            "timestamp": base_timestamp + 190000,  # +10秒
            "cardNumber": "****3456"
        },
        {
            "transactionId": "TXN_011",
            "userId": "USER_004",
            "amount": 700.00,
            "currency": "USD",
            "merchantId": "MERCHANT_K",
            "timestamp": base_timestamp + 200000,  # +10秒
            "cardNumber": "****3456"
        }
    ]

    for txn in transactions_scenario4:
        print(json.dumps(txn))

    print("\n" + "="*60)
    print("预期结果:")
    print("  - USER_001 应触发 1 次告警（TXN_001, TXN_002, TXN_003）")
    print("  - USER_002 不触发告警（间隔超过1分钟）")
    print("  - USER_003 不触发告警（总时长超过1分钟）")
    print("  - USER_004 应触发 1 次告警（TXN_009, TXN_010, TXN_011）")
    print("="*60)

    print("\n使用方法:")
    print("  python generate_test_data.py | kafka-console-producer.sh \\")
    print("    --bootstrap-server kafka:9092 \\")
    print("    --topic credit-card-transactions")

if __name__ == "__main__":
    generate_test_transactions()

