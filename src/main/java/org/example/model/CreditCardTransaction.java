package org.example.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 信用卡交易事件 POJO
 * 包含信用卡交易的核心必要字段
 */
public class CreditCardTransaction implements Serializable {
    private static final long serialVersionUID = 1L;

    private String transactionId;     // 交易ID（唯一标识）
    private String userId;            // 用户ID
    private Double amount;            // 交易金额
    private String currency;          // 币种（如: USD, CNY, EUR）
    private String merchantId;        // 商户ID
    private Long timestamp;           // 交易时间戳（毫秒）
    private String cardNumber;        // 卡号（脱敏后，如后4位）

    // 无参构造函数（Jackson 反序列化需要）
    public CreditCardTransaction() {
    }

    // 全参构造函数
    public CreditCardTransaction(String transactionId, String userId, Double amount,
                                  String currency, String merchantId, Long timestamp,
                                  String cardNumber) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.currency = currency;
        this.merchantId = merchantId;
        this.timestamp = timestamp;
        this.cardNumber = cardNumber;
    }

    // Getters and Setters
    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getCardNumber() {
        return cardNumber;
    }

    public void setCardNumber(String cardNumber) {
        this.cardNumber = cardNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreditCardTransaction that = (CreditCardTransaction) o;
        return Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }

    @Override
    public String toString() {
        return "CreditCardTransaction{" +
                "transactionId='" + transactionId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", merchantId='" + merchantId + '\'' +
                ", timestamp=" + timestamp +
                ", cardNumber='" + cardNumber + '\'' +
                '}';
    }
}

