-- ============================================================================
-- 信用卡當月額度上限資料表 DDL
-- 以卡號 (card_number) 為主鍵，對應一個當月額度上限金額 (credit_limit)
-- ============================================================================

CREATE TABLE IF NOT EXISTS card_limit (
    card_number   VARCHAR(32)     NOT NULL,
    credit_limit  NUMERIC(15, 2)  NOT NULL,
    updated_at    TIMESTAMP       NOT NULL DEFAULT now(),
    CONSTRAINT pk_card_limit PRIMARY KEY (card_number)
);

COMMENT ON TABLE card_limit IS '信用卡當月額度上限';
COMMENT ON COLUMN card_limit.card_number IS '卡號（主鍵）';
COMMENT ON COLUMN card_limit.credit_limit IS '當月額度上限金額';

-- ============================================================================
-- Flink CDC 所需設定：
-- 1. REPLICA IDENTITY FULL：確保 UPDATE/DELETE 事件的 WAL 紀錄攜帶完整欄位值
--    （預設 REPLICA IDENTITY DEFAULT 只會在 old row 帶主鍵欄位，對於需要
--    比對 credit_limit 舊值的情境不夠用）。
-- 2. 賦予目前資料庫使用者 REPLICATION 權限，供 flink-connector-postgres-cdc
--    建立邏輯複製連線 / replication slot 使用。
-- ============================================================================
ALTER TABLE card_limit REPLICA IDENTITY FULL;

ALTER ROLE CURRENT_USER WITH REPLICATION;
