package org.example.monthly.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.CreditCardTransaction;
import org.example.monthly.model.CardLimitUpdate;
import org.example.monthly.model.MonthlyConsumptionFact;
import org.example.monthly.rule.CardLevelRuleEngine;

import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 依卡號（keyBy cardNumber）即時累加當月消費總金額，並透過 broadcast state
 * 取得卡片額度上限（由 Flink CDC 監聽 PostgreSQL card_limit 表的 WAL 邏輯複製
 * 產生，經 {@link org.example.monthly.cdc.CardLimitDebeziumDeserializer} 轉換後
 * broadcast 而來），概念上對應 ksqlDB 的 stream + table join：交易明細是主要的
 * keyed stream，card_limit 是被 materialize 成本地 broadcast state 的「表」，
 * 兩者在此 join 後交由規則引擎判斷 —— 取代逐筆交易觸發一次 JDBC 查詢的作法。
 *
 * - 每筆交易到達時累加進「目前追蹤月份」的總額，並記錄 log（不輸出）。
 * - 針對目前追蹤月份註冊一個處理時間 timer，時間點為「次月第一天 00:00」；
 *   當 timer 觸發（代表該月份已經過去／月結），才從 broadcast state 讀取目前
 *   額度上限，交由規則引擎計算 status / description，輸出 fact，並重置狀態、
 *   追蹤下一個月份。
 * - broadcast 端收到 {@link CardLimitUpdate}：若為 upsert（新增/更新/CDC 初始
 *   snapshot），覆蓋 broadcast state 中對應卡號的額度上限；若為 delete（該卡
 *   額度上限資料已從來源表刪除），則從 broadcast state 移除。
 */
@Slf4j
public class MonthlyAggregationFunction
        extends KeyedBroadcastProcessFunction<String, CreditCardTransaction, CardLimitUpdate, MonthlyConsumptionFact> {
    private static final long serialVersionUID = 1L;

    /** Broadcast state descriptor：cardNumber -> 額度上限，由 card_limit CDC 廣播寫入。 */
    public static final MapStateDescriptor<String, Double> CARD_LIMIT_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("cardLimitBroadcastState", Types.STRING, Types.DOUBLE);

    /** broadcast state 尚無該卡資料（例如新卡尚未被 CDC 同步到）時使用的預設額度上限 */
    private static final double DEFAULT_CARD_LIMIT = 100000.0;

    private static final DateTimeFormatter YEAR_MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyyMM");
    private static final ZoneId ZONE = ZoneId.systemDefault();

    private final CardLevelRuleEngine ruleEngine;

    private transient ValueState<String> currentYearMonthState;
    private transient ValueState<Double> currentMonthTotalState;

    public MonthlyAggregationFunction(CardLevelRuleEngine ruleEngine) {
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
    public void processElement(CreditCardTransaction txn, ReadOnlyContext ctx, Collector<MonthlyConsumptionFact> out)
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
    public void processBroadcastElement(CardLimitUpdate update, Context ctx, Collector<MonthlyConsumptionFact> out)
            throws Exception {
        if (update.isDeleted()) {
            ctx.getBroadcastState(CARD_LIMIT_STATE_DESCRIPTOR).remove(update.getCardNumber());
            log.info("card_limit CDC 刪除事件，自 broadcast state 移除: cardNumber={}", update.getCardNumber());
            return;
        }
        ctx.getBroadcastState(CARD_LIMIT_STATE_DESCRIPTOR).put(update.getCardNumber(), update.getCreditLimit());
        log.debug("更新卡片額度上限 broadcast state: cardNumber={}, creditLimit={}",
                update.getCardNumber(), update.getCreditLimit());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<MonthlyConsumptionFact> out) throws Exception {
        String closedYearMonth = currentYearMonthState.value();
        if (closedYearMonth == null) {
            return; // 該卡從未累加過資料，無需月結
        }

        String cardNumber = ctx.getCurrentKey();
        double monthlyTotal = currentMonthTotalState.value();
        Double cardLimit = ctx.getBroadcastState(CARD_LIMIT_STATE_DESCRIPTOR).get(cardNumber);
        if (cardLimit == null) {
            log.warn("broadcast state 查無卡片額度上限資料，使用預設額度上限: cardNumber={}, 預設={}",
                    cardNumber, DEFAULT_CARD_LIMIT);
            cardLimit = DEFAULT_CARD_LIMIT;
        }

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
    private void registerNextMonthTimer(ReadOnlyContext ctx, String yearMonth) {
        ctx.timerService().registerProcessingTimeTimer(nextMonthStartMillis(yearMonth));
    }

    private long nextMonthStartMillis(String yearMonth) {
        return YearMonth.parse(yearMonth, YEAR_MONTH_FORMATTER)
                .plusMonths(1)
                .atDay(1)
                .atStartOfDay(ZONE)
                .toInstant()
                .toEpochMilli();
    }

    /** 依交易的 timestamp 解析所屬月份（yyyyMM），若無 timestamp 則退回目前處理時間。 */
    private String resolveYearMonth(CreditCardTransaction txn) {
        long epochMillis = txn.getTimestamp() != null ? txn.getTimestamp() : System.currentTimeMillis();
        return YearMonth.from(Instant.ofEpochMilli(epochMillis).atZone(ZONE)).format(YEAR_MONTH_FORMATTER);
    }
}
