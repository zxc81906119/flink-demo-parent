package org.example.model;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * 欺诈告警事件 POJO
 * 用于记录检测到的异常交易行为
 */
public class FraudAlert implements Serializable {
    private static final long serialVersionUID = 1L;

    private String alertId;              // 告警ID（唯一标识，用于去重）
    private String userId;               // 用户ID
    private String alertType;            // 告警类型（如: RAPID_TRANSACTIONS）
    private List<String> transactionIds; // 涉及的交易ID列表
    private Double totalAmount;          // 涉及交易的总金额
    private Long windowStart;            // 时间窗口开始时间（毫秒）
    private Long windowEnd;              // 时间窗口结束时间（毫秒）
    private Long detectedTime;           // 告警检测时间（毫秒）
    private String message;              // 告警描述信息

    // 无参构造函数（Jackson 序列化需要）
    public FraudAlert() {
    }

    // 全参构造函数
    public FraudAlert(String alertId, String userId, String alertType,
                      List<String> transactionIds, Double totalAmount,
                      Long windowStart, Long windowEnd, Long detectedTime,
                      String message) {
        this.alertId = alertId;
        this.userId = userId;
        this.alertType = alertType;
        this.transactionIds = transactionIds;
        this.totalAmount = totalAmount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.detectedTime = detectedTime;
        this.message = message;
    }

    // Getters and Setters
    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    public List<String> getTransactionIds() {
        return transactionIds;
    }

    public void setTransactionIds(List<String> transactionIds) {
        this.transactionIds = transactionIds;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getDetectedTime() {
        return detectedTime;
    }

    public void setDetectedTime(Long detectedTime) {
        this.detectedTime = detectedTime;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FraudAlert that = (FraudAlert) o;
        return Objects.equals(alertId, that.alertId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alertId);
    }

    @Override
    public String toString() {
        return "FraudAlert{" +
                "alertId='" + alertId + '\'' +
                ", userId='" + userId + '\'' +
                ", alertType='" + alertType + '\'' +
                ", transactionIds=" + transactionIds +
                ", totalAmount=" + totalAmount +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", detectedTime=" + detectedTime +
                ", message='" + message + '\'' +
                '}';
    }
}

