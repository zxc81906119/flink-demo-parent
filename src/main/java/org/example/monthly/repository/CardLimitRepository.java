package org.example.monthly.repository;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 信用卡當月額度上限查詢 repository。
 *
 * 透過 JDBC 連線 PostgreSQL 的 card_limit 表（card_number 為主鍵），
 * 並在記憶體中快取查詢結果，避免每筆交易都觸發一次 DB 查詢。
 *
 * 非 Flink 狀態，僅存活於單一 TaskManager slot 的 process function 實例內，
 * 因此每個 subtask 各自維護一份快取即可。
 */
@Slf4j
public class CardLimitRepository implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String QUERY_SQL =
            "SELECT credit_limit FROM card_limit WHERE card_number = ?";

    /** 查無卡號資料時使用的預設額度上限 */
    private static final double DEFAULT_CARD_LIMIT = 100000.0;

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private transient Map<String, Double> cache;

    public CardLimitRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    /** 依卡號查詢當月額度上限，若快取存在則直接返回，避免重複查詢 DB。 */
    public double getCardLimit(String cardNumber) {
        if (cache == null) {
            cache = new ConcurrentHashMap<>();
        }
        return cache.computeIfAbsent(cardNumber, this::queryFromDatabase);
    }

    /** 清除快取，供需要重新整理額度資料時使用（例如每月月結後）。 */
    public void invalidate(String cardNumber) {
        if (cache != null) {
            cache.remove(cardNumber);
        }
    }

    private double queryFromDatabase(String cardNumber) {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(QUERY_SQL)) {
            ps.setString(1, cardNumber);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    double limit = rs.getDouble("credit_limit");
                    log.info("查詢卡片額度上限成功: cardNumber={}, cardLimit={}", cardNumber, limit);
                    return limit;
                }
            }
        } catch (SQLException e) {
            log.error("查詢卡片額度上限失敗: cardNumber={}, 使用預設額度上限={}", cardNumber, DEFAULT_CARD_LIMIT, e);
            return DEFAULT_CARD_LIMIT;
        }
        log.warn("查無卡片額度上限資料: cardNumber={}, 使用預設額度上限={}", cardNumber, DEFAULT_CARD_LIMIT);
        return DEFAULT_CARD_LIMIT;
    }
}
