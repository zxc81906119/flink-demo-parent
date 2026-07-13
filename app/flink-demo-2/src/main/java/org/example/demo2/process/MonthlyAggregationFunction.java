package org.example.demo2.process;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.demo2.Demo2Job;
import org.example.common.model.CreditCardTransaction;
import org.example.demo2.model.CardLimitUpdate;
import org.example.demo2.model.MonthlyConsumptionFact;
import org.example.demo2.rule.CardLevelRuleEngine;

import java.time.Duration;
import java.time.YearMonth;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public class MonthlyAggregationFunction
        extends KeyedBroadcastProcessFunction<Demo2Job.BusinessKey, CreditCardTransaction, CardLimitUpdate, MonthlyConsumptionFact> {

    private final CardLevelRuleEngine ruleEngine;

    private transient ValueState<Boolean> finishState;
    private transient ValueState<Double> monthTotalState;

    public static final MapStateDescriptor<String, Double> CARD_LIMIT_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("cardLimitBroadcastState", Types.STRING, Types.DOUBLE);

    public static final OutputTag<LateTransactionRecord> LATE_TRANSACTION_TAG =
            new OutputTag<>("late-transaction", TypeInformation.of(LateTransactionRecord.class));

    public static final double DEFAULT_CARD_LIMIT = 100000.0;


    @Override
    public void open(Configuration parameters) {

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofDays(40))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                // todo 尚需了解
                .cleanupInRocksdbCompactFilter(1000)   // RocksDB 可選
                .build();

        ValueStateDescriptor<Double> doubleValueStateDescriptor = new ValueStateDescriptor<>("monthTotal", Double.class);
        doubleValueStateDescriptor.enableTimeToLive(ttlConfig);

        monthTotalState = getRuntimeContext().getState(doubleValueStateDescriptor);

        finishState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("finish", Boolean.class)
        );

    }

    @Override
    public void processElement(CreditCardTransaction txn, ReadOnlyContext ctx, Collector<MonthlyConsumptionFact> out)
            throws Exception {
        // 事件水位線
        System.out.println("processElement 事件水位線: " + ctx.timestamp());
        // 事件時間
        System.out.println("processElement 事件時間: " + txn.getTimestamp());
        // 當前水位線
        System.out.println("processElement 當前水位線: " + ctx.currentWatermark());
        // 當前處理時間
        System.out.println("processElement 當前處理時間: " + ctx.currentProcessingTime());
        // 同 key 同 timer 時間只算一次註冊
        val currentKey = ctx.getCurrentKey();
        if (Boolean.TRUE.equals(finishState.value())) {
            ctx.output(LATE_TRANSACTION_TAG,
                    new LateTransactionRecord(
                            currentKey.getCardNumber(),
                            currentKey.getYearMonth(),
                            txn,
                            "Transaction after month end"
                    )
            );
            return;
        }
        registerNextMonthTimer(ctx, currentKey.getYearMonth());
        double amount = Optional.ofNullable(txn.getAmount()).orElse(0.0);
        double lastMonthTotal = Optional.ofNullable(monthTotalState.value()).orElse(0.0);
        monthTotalState.update(amount + lastMonthTotal);

    }

    @Override
    public void processBroadcastElement(CardLimitUpdate update, Context ctx, Collector<MonthlyConsumptionFact> out)
            throws Exception {
        // 事件水位線
        System.out.println("processBroadcastElement 事件水位線: " + ctx.timestamp());
        // 當前水位線
        System.out.println("processBroadcastElement 當前水位線: " + ctx.currentWatermark());
        // 當前處理時間
        System.out.println("processBroadcastElement 當前處理時間: " + ctx.currentProcessingTime());
        if (update.isDeleted()) {
            ctx.getBroadcastState(CARD_LIMIT_STATE_DESCRIPTOR).remove(update.getCardNumber());
            log.info("card_limit CDC 刪除事件，自 broadcast state 移除: cardNumber={}", update.getCardNumber());
            return;
        }
        ctx.getBroadcastState(CARD_LIMIT_STATE_DESCRIPTOR).put(update.getCardNumber(), update.getCreditLimit());
        log.debug("更新卡片額度上限 broadcast state: cardNumber={}, creditLimit={}",
                update.getCardNumber(), update.getCreditLimit());
    }

    // 根據水位線的值處理
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<MonthlyConsumptionFact> out) throws Exception {
        System.out.println("onTimer 事件水位線: " + timestamp);
        System.out.println("onTimer 當前水位線: " + ctx.currentWatermark());
        System.out.println("onTimer 當前處理時間: " + ctx.currentProcessingTime());

        Demo2Job.BusinessKey currentKey = ctx.getCurrentKey();

        String cardNumber = currentKey.getCardNumber();

        double cardLimit = Optional.ofNullable(
                        ctx.getBroadcastState(CARD_LIMIT_STATE_DESCRIPTOR).get(cardNumber)
                )
                .orElse(DEFAULT_CARD_LIMIT);

        String lastYearMonth = currentKey.getYearMonth();

        double lastMonthTotal = Optional.ofNullable(monthTotalState.value()).orElse(0.0);

        MonthlyConsumptionFact fact = new MonthlyConsumptionFact(
                cardNumber,
                lastYearMonth,
                lastMonthTotal,
                cardLimit,
                null,
                null,
                System.currentTimeMillis()
        );
        // todo 規則引擎拋例外處理
        out.collect(ruleEngine.evaluate(fact));

        finishState.update(true);
    }

    private void registerNextMonthTimer(ReadOnlyContext ctx, String yearMonth) {
        ctx.timerService().registerEventTimeTimer(nextMonthStartMillis(yearMonth));
    }

    private long nextMonthStartMillis(String yearMonth) {
        return YearMonth.parse(yearMonth, Demo2Job.YEAR_MONTH_FORMATTER)
                .plusMonths(1)
                .atDay(1)
                .atStartOfDay(Demo2Job.ZONE)
                .toInstant()
                .toEpochMilli();
    }

    @Data
    @AllArgsConstructor
    public static class LateTransactionRecord {
        private String cardNumber;
        private String txnYearMonth;
        private CreditCardTransaction transaction;
        private String reason;
    }


}
