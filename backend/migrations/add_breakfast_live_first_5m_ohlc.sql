-- 09:15 first-5m OHLC on locked breakfast_live_signals rows (top-3 picks at 9:20:05 freeze).
ALTER TABLE breakfast_live_signals
    ADD COLUMN IF NOT EXISTS first_5m_open DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS first_5m_high DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS first_5m_low DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS first_5m_close DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS first_5m_ts TIMESTAMPTZ;
