CREATE TABLE IF NOT EXISTS premium_futures_tv_webhook_log (
    id BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    source_ip TEXT,
    raw_body TEXT,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    parse_status TEXT NOT NULL CHECK (parse_status IN ('success', 'unmatched', 'failed')),
    parse_note TEXT,
    ticker_raw TEXT,
    symbol TEXT,
    side TEXT CHECK (side IS NULL OR side IN ('bullish', 'bearish')),
    resolved_fut_symbol TEXT,
    resolved_fut_instrument_key TEXT,
    underlying TEXT,
    promoted_at TIMESTAMP WITHOUT TIME ZONE
);
CREATE INDEX IF NOT EXISTS ix_premium_futures_tv_webhook_log_received
    ON premium_futures_tv_webhook_log (received_at DESC);
CREATE INDEX IF NOT EXISTS ix_premium_futures_tv_webhook_log_received_date
    ON premium_futures_tv_webhook_log ((CAST(received_at AS date)));
CREATE INDEX IF NOT EXISTS ix_premium_futures_tv_webhook_log_session_side
    ON premium_futures_tv_webhook_log ((CAST(received_at AS date)), side, parse_status);
