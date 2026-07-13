package org.example.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 信用卡交易事件 POJO
 * 包含信用卡交易的核心必要字段
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreditCardTransaction implements Serializable {
    private static final long serialVersionUID = 1L;

    private String transactionId;     // 交易ID（唯一标识）
    private String userId;            // 用户ID
    private Double amount;            // 交易金额
    private String currency;          // 币种（如: USD, CNY, EUR）
    private String merchantId;        // 商户ID
    private Long timestamp;           // 交易时间戳（毫秒）
    private String cardNumber;        // 卡号（脱敏后，如后4位）

}

