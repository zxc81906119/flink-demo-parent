package org.example.demo2.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CardLimitUpdate implements Serializable {
    private static final long serialVersionUID = 1L;

    private String cardNumber;
    private Double creditLimit;
    private boolean deleted;

    /**
     * 建立一筆 upsert 事件（新增/更新/初始 snapshot）。
     */
    public static CardLimitUpdate upsert(String cardNumber, double creditLimit) {
        return new CardLimitUpdate(cardNumber, creditLimit, false);
    }

    /**
     * 建立一筆 delete 事件，creditLimit 無意義（設為 null）。
     */
    public static CardLimitUpdate delete(String cardNumber) {
        return new CardLimitUpdate(cardNumber, null, true);
    }
}
