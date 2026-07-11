package org.example.monthly.process;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.example.model.CreditCardTransaction;
import org.example.monthly.model.MonthlyConsumptionFact;
import org.example.monthly.repository.CardLimitRepository;
import org.example.monthly.rule.CardLevelRuleEngine;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 驗證 MonthlyAggregationFunction：
 * - 同一個月內多筆交易應累加，且不立即輸出
 * - 處理時間跨過月份邊界（timer 觸發）後，應輸出規則引擎計算結果並重置狀態
 */
class MonthlyAggregationFunctionTest {

    private static final String CARD_NUMBER = "TEST_CARD_0001";

    /** 測試用假額度倉儲，避免依賴真實 PostgreSQL。 */
    private static class FakeCardLimitRepository extends CardLimitRepository {
        private static final long serialVersionUID = 1L;
        private final double fixedLimit;

        FakeCardLimitRepository(double fixedLimit) {
            super("jdbc:fake", "fake", "fake");
            this.fixedLimit = fixedLimit;
        }

        @Override
        public double getCardLimit(String cardNumber) {
            return fixedLimit;
        }
    }

    @Test
    void monthRollover_shouldEmitRuleResultAndResetState() throws Exception {
        // cardLimit = 300000 落在 (100000,500000] 區間
        FakeCardLimitRepository repository = new FakeCardLimitRepository(300000.0);
        MonthlyAggregationFunction function =
                new MonthlyAggregationFunction(repository, new CardLevelRuleEngine());

        KeyedProcessOperator<String, CreditCardTransaction, MonthlyConsumptionFact> operator =
                new KeyedProcessOperator<>(function);

        try (KeyedOneInputStreamOperatorTestHarness<String, CreditCardTransaction, MonthlyConsumptionFact> harness =
                     new KeyedOneInputStreamOperatorTestHarness<>(
                             operator, CreditCardTransaction::getCardNumber, TypeInformation.of(String.class))) {

            harness.open();

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

            @SuppressWarnings("unchecked")
            StreamRecord<MonthlyConsumptionFact> record = (StreamRecord<MonthlyConsumptionFact>) output.peek();
            MonthlyConsumptionFact result = record.getValue();
            assertEquals(CARD_NUMBER, result.getCardNumber());
            assertEquals("202601", result.getYearMonth());
            assertEquals(295000.0, result.getMonthlyAmount());
            // 295000 >= 300000 - 5000 -> status=2 自動提高上限
            assertEquals(2, result.getStatus());
            assertEquals("自動提高上限", result.getDescription());
        }
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
