package org.example.demo1.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 欺诈告警事件 POJO
 * 用于记录检测到的异常交易行为
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
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


}

