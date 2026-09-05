-- Weekly volatility grade for current-month futures (SPAN/notional leverage).
ALTER TABLE arbitrage_master
    ADD COLUMN IF NOT EXISTS volatility_grade TEXT,
    ADD COLUMN IF NOT EXISTS volatility_score NUMERIC,
    ADD COLUMN IF NOT EXISTS volatility_grade_at TIMESTAMPTZ;
