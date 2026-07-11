package org.example.monthly.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 卡片額度上限變更事件。
 *
 * 由 Flink CDC（{@code flink-connector-postgres-cdc}）監聽 PostgreSQL
 * card_limit 表的 WAL 邏輯複製產生，透過 broadcast stream 廣播給所有
 * {@code MonthlyAggregationFunction} 的 keyed 分區，在本地 broadcast state
 * 中 materialize 成一張「額度上限表」，取代逐筆交易觸發 JDBC 查詢的作法
 * ——概念上對應 ksqlDB 的 stream + table join：card_limit 表的變更被轉成
 * 一條 changelog stream，在 Flink 端即時同步成 table。
 *
 * {@code deleted} 為 true 時代表該卡的額度上限資料已從來源表刪除
 * （Debezium op = 'd'），下游應將該卡從 broadcast state 移除；
 * 其餘情況（新增 op='c' 或更新 op='u'，以及 snapshot 階段 op='r'）
 * 一律視為 upsert，直接覆蓋 broadcast state 中對應卡號的額度上限。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CardLimitUpdate implements Serializable {
    private static final long serialVersionUID = 1L;

    private String cardNumber;
    private Double creditLimit;
    private boolean deleted;

    /** 建立一筆 upsert 事件（新增/更新/初始 snapshot）。 */
    public static CardLimitUpdate upsert(String cardNumber, double creditLimit) {
        return new CardLimitUpdate(cardNumber, creditLimit, false);
    }

    /** 建立一筆 delete 事件，creditLimit 無意義（設為 null）。 */
    public static CardLimitUpdate delete(String cardNumber) {
        return new CardLimitUpdate(cardNumber, null, true);
    }
}
