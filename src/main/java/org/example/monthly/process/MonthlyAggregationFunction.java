package org.example.monthly.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.monthly.model.MonthlyConsumptionFact;
import org.example.monthly.repository.CardLimitRepository;
import org.example.monthly.rule.CardLevelRuleEngine;
import org.example.model.CreditCardTransaction;

import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 依卡號（keyBy cardNumber）即時累加當月消費總金額。
 *
 * - 每筆交易到達時累加進「目前追蹤月份」的總額，並記錄 log（不輸出）。
 * - 針對目前追蹤月份註冊一個處理時間 timer，時間點為「次月第一天 00:00」；
 *   當 timer 觸發（代表該月份已經過去／月結），才將累積結果交由規則引擎
 *   計算 status / description，輸出 fact，並重置狀態、追蹤下一個月份。
 */
@Slf4j
public class MonthlyAggregationFunction
        extends KeyedProcessFunction<String, CreditCardTransaction, MonthlyConsumptionFact> {
    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter YEAR_MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyyMM");
    private static final ZoneId ZONE = ZoneId.systemDefault();

    private final CardLimitRepository cardLimitRepository;
    private final CardLevelRuleEngine ruleEngine;

    private transient ValueState<String> currentYearMonthState;
    private transient ValueState<Double> currentMonthTotalState;

    public MonthlyAggregationFunction(CardLimitRepository cardLimitRepository, CardLevelRuleEngine ruleEngine) {
        this.cardLimitRepository = cardLimitRepository;
        this.ruleEngine = ruleEngine;
    }

    @Override
    public void open(Configuration parameters) {
        currentYearMonthState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("currentYearMonth", String.class));
        currentMonthTotalState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("currentMonthTotal", Double.class));
    }

    @Override
    public void processElement(CreditCardTransaction txn, Context ctx, Collector<MonthlyConsumptionFact> out)
            throws Exception {
        String cardNumber = ctx.getCurrentKey();
        String txnYearMonth = resolveYearMonth(txn);

        String trackingYearMonth = currentYearMonthState.value();
        if (trackingYearMonth == null) {
            // 該卡第一筆交易：開始追蹤這個月份，並註冊月結 timer
            trackingYearMonth = txnYearMonth;
            currentYearMonthState.update(trackingYearMonth);
            currentMonthTotalState.update(0.0);
            registerNextMonthTimer(ctx, trackingYearMonth);
        }

        if (!txnYearMonth.equals(trackingYearMonth)) {
            // 交易月份與目前追蹤月份不同（例如遲到的舊資料），僅記錄 log，不納入計算
            log.warn("交易月份與目前追蹤月份不符，忽略此筆累加: cardNumber={}, txnYearMonth={}, trackingYearMonth={}",
                    cardNumber, txnYearMonth, trackingYearMonth);
            return;
        }

        double amount = txn.getAmount() == null ? 0.0 : txn.getAmount();
        double newTotal = currentMonthTotalState.value() + amount;
        currentMonthTotalState.update(newTotal);

        log.info("累加當月消費金額: cardNumber={}, yearMonth={}, transactionId={}, amount={}, monthlyTotal={}",
                cardNumber, trackingYearMonth, txn.getTransactionId(), amount, newTotal);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<MonthlyConsumptionFact> out) throws Exception {
        String closedYearMonth = currentYearMonthState.value();
        if (closedYearMonth == null) {
            return; // 該卡從未累加過資料，無需月結
        }

        String cardNumber = ctx.getCurrentKey();
        double monthlyTotal = currentMonthTotalState.value();
        double cardLimit = cardLimitRepository.getCardLimit(cardNumber);

        log.info("月結觸發，交由規則引擎計算分級: cardNumber={}, yearMonth={}, monthlyTotal={}, cardLimit={}",
                cardNumber, closedYearMonth, monthlyTotal, cardLimit);

        MonthlyConsumptionFact fact = new MonthlyConsumptionFact(
                cardNumber, closedYearMonth, monthlyTotal, cardLimit, null, null, System.currentTimeMillis());
        MonthlyConsumptionFact result = ruleEngine.evaluate(fact);
        out.collect(result);

        // 重置狀態，開始追蹤下一個月份
        String nextYearMonth = YearMonth.parse(closedYearMonth, YEAR_MONTH_FORMATTER)
                .plusMonths(1)
                .format(YEAR_MONTH_FORMATTER);
        currentYearMonthState.update(nextYearMonth);
        currentMonthTotalState.update(0.0);
        registerNextMonthTimer(ctx, nextYearMonth);
    }

    /** 針對指定月份，於下個月第一天 00:00（本地時區）註冊處理時間 timer，代表該月份的月結時間點。 */
    private void registerNextMonthTimer(Context ctx, String yearMonth) {
        long nextMonthStartMillis = YearMonth.parse(yearMonth, YEAR_MONTH_FORMATTER)
                .plusMonths(1)
                .atDay(1)
                .atStartOfDay(ZONE)
                .toInstant()
                .toEpochMilli();
        ctx.timerService().registerProcessingTimeTimer(nextMonthStartMillis);
    }

    /** 依交易的 timestamp 解析所屬月份（yyyyMM），若無 timestamp 則退回目前處理時間。 */
    private String resolveYearMonth(CreditCardTransaction txn) {
        long epochMillis = txn.getTimestamp() != null ? txn.getTimestamp() : System.currentTimeMillis();
        return YearMonth.from(Instant.ofEpochMilli(epochMillis).atZone(ZONE)).format(YEAR_MONTH_FORMATTER);
    }
}
