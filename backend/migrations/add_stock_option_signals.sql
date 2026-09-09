CREATE TABLE IF NOT EXISTS stock_option_webhook_log (
    id BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    source_ip TEXT,
    scan_name TEXT,
    alert_name TEXT,
    triggered_at_raw TEXT,
    raw_payload JSONB NOT NULL,
    parse_status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_stock_option_webhook_log_received
    ON stock_option_webhook_log (received_at DESC);

CREATE TABLE IF NOT EXISTS stock_option_signals (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    williamsr DOUBLE PRECISION,
    instrument_key TEXT,
    status TEXT NOT NULL,
    side TEXT,
    trigger_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    triggered_at_raw TEXT,
    scan_name TEXT,
    alert_name TEXT,
    raw_payload JSONB,
    ema9 DOUBLE PRECISION,
    ema30 DOUBLE PRECISION,
    ema100 DOUBLE PRECISION,
    ema_updated_at TIMESTAMP WITHOUT TIME ZONE,
    armed_at TIMESTAMP WITHOUT TIME ZONE,
    sell_strike DOUBLE PRECISION,
    sell_delta DOUBLE PRECISION,
    sell_instrument_key TEXT,
    buy_strike DOUBLE PRECISION,
    buy_delta DOUBLE PRECISION,
    buy_instrument_key TEXT,
    date_traded DATE,
    sell_cost DOUBLE PRECISION,
    buy_cost DOUBLE PRECISION,
    user_sell_strike DOUBLE PRECISION,
    user_buy_strike DOUBLE PRECISION,
    hard_stop_placed BOOLEAN NOT NULL DEFAULT FALSE,
    remarks TEXT,
    sell_ltp DOUBLE PRECISION,
    buy_ltp DOUBLE PRECISION,
    ltp_updated_at TIMESTAMP WITHOUT TIME ZONE,
    exit_date DATE,
    sell_exit_price DOUBLE PRECISION,
    buy_exit_price DOUBLE PRECISION,
    realized_pnl DOUBLE PRECISION,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_stock_option_signals_symbol
    ON stock_option_signals (symbol, status);
CREATE INDEX IF NOT EXISTS ix_stock_option_signals_status
    ON stock_option_signals (status, id DESC);

ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS exit_date DATE;
ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS sell_exit_price DOUBLE PRECISION;
ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS buy_exit_price DOUBLE PRECISION;
ALTER TABLE stock_option_signals ADD COLUMN IF NOT EXISTS realized_pnl DOUBLE PRECISION;
