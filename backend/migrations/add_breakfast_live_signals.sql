CREATE TABLE IF NOT EXISTS breakfast_live_signals (
    id BIGSERIAL PRIMARY KEY,
    session_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
    UNIQUE (session_date, symbol, direction),
    sector TEXT NOT NULL,
    sector_rank SMALLINT NOT NULL CHECK (sector_rank BETWEEN 1 AND 15),
    rank_at_lock SMALLINT NOT NULL CHECK (rank_at_lock BETWEEN 1 AND 3),
    nifty_bias_pct DOUBLE PRECISION,
    sector_move_pct DOUBLE PRECISION NOT NULL,
    stock_move_pct_at_lock DOUBLE PRECISION NOT NULL,
    ltp_at_lock DOUBLE PRECISION,
    anchor_price DOUBLE PRECISION NOT NULL,
    tp_price DOUBLE PRECISION NOT NULL,
    sl_price DOUBLE PRECISION NOT NULL,
    lot_size INTEGER NOT NULL CHECK (lot_size > 0),
    locked_at_timestamp TIMESTAMPTZ NOT NULL,
    websocket_rest_cross_check_status TEXT NOT NULL DEFAULT 'matched'
        CHECK (websocket_rest_cross_check_status IN ('matched', 'mismatched')),
    instrument_key TEXT,
    trade_taken BOOLEAN NOT NULL DEFAULT FALSE,
    manual_entry_price DOUBLE PRECISION,
    manual_entry_time TIMESTAMPTZ,
    manual_exit_price DOUBLE PRECISION,
    manual_exit_time TIMESTAMPTZ,
    manual_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_breakfast_live_signals_session ON breakfast_live_signals (session_date DESC);
CREATE INDEX IF NOT EXISTS ix_breakfast_live_signals_session_rank ON breakfast_live_signals (session_date, sector_rank, rank_at_lock);
