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
