package org.example.monthly.process;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.example.model.CreditCardTransaction;
import org.example.monthly.model.CardLimitUpdate;
import org.example.monthly.model.MonthlyConsumptionFact;
import org.example.monthly.rule.CardLevelRuleEngine;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 驗證 MonthlyAggregationFunction（KeyedBroadcastProcessFunction 版本）：
 * - 同一個月內多筆交易應累加，且不立即輸出
 * - 卡片額度上限透過 broadcast state 更新（模擬 CDC upsert 事件廣播），
 *   不再透過 JDBC 逐筆查詢
 * - 收到 CDC delete 事件應從 broadcast state 移除，之後月結改採預設額度上限
 * - 處理時間跨過月份邊界（timer 觸發）後，應輸出規則引擎計算結果並重置狀態
 */
class MonthlyAggregationFunctionTest {

    private static final String CARD_NUMBER = "TEST_CARD_0001";

    @Test
    void monthRollover_shouldEmitRuleResultAndResetState() throws Exception {
        try (KeyedBroadcastOperatorTestHarness<String, CreditCardTransaction, CardLimitUpdate, MonthlyConsumptionFact>
                     harness = newHarness()) {
            harness.open();

            // 廣播卡片額度上限 = 300000（CDC upsert），落在 (100000,500000] 區間
            harness.processBroadcastElement(CardLimitUpdate.upsert(CARD_NUMBER, 300000.0), 0L);

            long jan15 = toEpochMillis(2026, 1, 15);
            long jan20 = toEpochMillis(2026, 1, 20);

            harness.setProcessingTime(jan15);
            harness.processElement(newTxn("TXN_1", 100000.0, jan15), jan15);

            harness.setProcessingTime(jan20);
            harness.processElement(newTxn("TXN_2", 195000.0, jan20), jan20);

            // 尚未跨月，不應有任何輸出
            assertTrue(harness.getOutput().isEmpty(), "月結前不應輸出結果");

            // 推進處理時間到 2 月份，觸發月結 timer
            long feb1 = toEpochMillis(2026, 2, 1) + 1000;
            harness.setProcessingTime(feb1);

            ConcurrentLinkedQueue<Object> output = harness.getOutput();
            assertEquals(1, output.size(), "跨月後應恰好輸出一筆規則引擎結果");

            MonthlyConsumptionFact result = extractFact(output);
            assertEquals(CARD_NUMBER, result.getCardNumber());
            assertEquals("202601", result.getYearMonth());
            assertEquals(295000.0, result.getMonthlyAmount());
            // 295000 >= 300000 - 5000 -> status=2 自動提高上限
            assertEquals(2, result.getStatus());
            assertEquals("自動提高上限", result.getDescription());
        }
    }

    @Test
    void noBroadcastUpdate_shouldFallBackToDefaultCardLimit() throws Exception {
        try (KeyedBroadcastOperatorTestHarness<String, CreditCardTransaction, CardLimitUpdate, MonthlyConsumptionFact>
                     harness = newHarness()) {
            harness.open();
            // 未廣播任何額度上限資料（模擬新卡尚未被 CDC 同步到）

            long jan15 = toEpochMillis(2026, 1, 15);
            harness.setProcessingTime(jan15);
            harness.processElement(newTxn("TXN_1", 50000.0, jan15), jan15);

            long feb1 = toEpochMillis(2026, 2, 1) + 1000;
            harness.setProcessingTime(feb1);

            ConcurrentLinkedQueue<Object> output = harness.getOutput();
            assertEquals(1, output.size());

            MonthlyConsumptionFact result = extractFact(output);
            // 預設額度上限 100000 -> status=1 建議提升上限
            assertEquals(1, result.getStatus());
            assertEquals("建議提升上限", result.getDescription());
        }
    }

    @Test
    void deleteEvent_shouldRemoveFromBroadcastStateAndFallBackToDefault() throws Exception {
        try (KeyedBroadcastOperatorTestHarness<String, CreditCardTransaction, CardLimitUpdate, MonthlyConsumptionFact>
                     harness = newHarness()) {
            harness.open();

            // 先廣播一筆額度上限，再廣播 delete 事件將其移除
            harness.processBroadcastElement(CardLimitUpdate.upsert(CARD_NUMBER, 300000.0), 0L);
            harness.processBroadcastElement(CardLimitUpdate.delete(CARD_NUMBER), 1L);

            long jan15 = toEpochMillis(2026, 1, 15);
            harness.setProcessingTime(jan15);
            harness.processElement(newTxn("TXN_1", 50000.0, jan15), jan15);

            long feb1 = toEpochMillis(2026, 2, 1) + 1000;
            harness.setProcessingTime(feb1);

            ConcurrentLinkedQueue<Object> output = harness.getOutput();
            assertEquals(1, output.size());

            MonthlyConsumptionFact result = extractFact(output);
            // delete 後 broadcast state 查無資料 -> 使用預設額度上限 100000 -> status=1
            assertEquals(1, result.getStatus());
            assertEquals("建議提升上限", result.getDescription());
        }
    }

    private static KeyedBroadcastOperatorTestHarness<String, CreditCardTransaction, CardLimitUpdate, MonthlyConsumptionFact>
            newHarness() throws Exception {
        MonthlyAggregationFunction function = new MonthlyAggregationFunction(new CardLevelRuleEngine());
        CoBroadcastWithKeyedOperator<String, CreditCardTransaction, CardLimitUpdate, MonthlyConsumptionFact> operator =
                new CoBroadcastWithKeyedOperator<>(
                        function, Collections.singletonList(MonthlyAggregationFunction.CARD_LIMIT_STATE_DESCRIPTOR));
        return new KeyedBroadcastOperatorTestHarness<>(
                operator, CreditCardTransaction::getCardNumber, TypeInformation.of(String.class), 1, 1, 0);
    }

    @SuppressWarnings("unchecked")
    private static MonthlyConsumptionFact extractFact(ConcurrentLinkedQueue<Object> output) {
        StreamRecord<MonthlyConsumptionFact> record = (StreamRecord<MonthlyConsumptionFact>) output.peek();
        return record.getValue();
    }

    private static CreditCardTransaction newTxn(String txnId, double amount, long timestamp) {
        CreditCardTransaction txn = new CreditCardTransaction();
        txn.setTransactionId(txnId);
        txn.setUserId("USER_0001");
        txn.setAmount(amount);
        txn.setCurrency("TWD");
        txn.setMerchantId("MERCHANT_0001");
        txn.setTimestamp(timestamp);
        txn.setCardNumber(CARD_NUMBER);
        return txn;
    }

    private static long toEpochMillis(int year, int month, int day) {
        return LocalDateTime.of(year, month, day, 0, 0)
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }
}
