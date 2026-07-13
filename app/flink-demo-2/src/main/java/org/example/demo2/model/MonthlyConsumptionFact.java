package org.example.demo2.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 每卡每月消費統計 fact 物件。
 * <p>
 * 規則引擎 input / output 共用同一個物件：
 * - input 欄位：monthlyAmount（當月消費總金額）、cardLimit（卡片當月額度上限）
 * - output 欄位：status、description（規則引擎計算結果）
 * <p>
 * 額外附帶 cardNumber / yearMonth 供輸出結果識別對應的卡片與月份，
 * 不參與規則判斷。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MonthlyConsumptionFact implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 卡號 */
    private String cardNumber;

    /** 消費月份，格式 yyyyMM */
    private String yearMonth;

    /** 當月消費總金額（規則引擎 input） */
    private Double monthlyAmount;

    /** 卡片當月額度上限（規則引擎 input） */
    private Double cardLimit;

    /** 規則引擎計算結果：分級狀態（規則引擎 output） */
    private Integer status;

    /** 規則引擎計算結果：分級說明（規則引擎 output） */
    private String description;

    /** 規則引擎計算時間（毫秒） */
    private Long calculatedTime;
}
