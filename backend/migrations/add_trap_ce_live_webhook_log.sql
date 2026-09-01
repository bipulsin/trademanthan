CREATE TABLE IF NOT EXISTS trap_ce_live_webhook_log (
    id BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    source_ip TEXT,
    symbol TEXT,
    trigger_price DOUBLE PRECISION,
    triggered_at_raw TEXT,
    scan_name TEXT,
    alert_name TEXT,
    raw_payload JSONB NOT NULL,
    parse_status TEXT NOT NULL CHECK (parse_status IN ('success', 'partial', 'failed'))
);
CREATE INDEX IF NOT EXISTS ix_trap_ce_live_webhook_log_received
    ON trap_ce_live_webhook_log (received_at DESC);
CREATE INDEX IF NOT EXISTS ix_trap_ce_live_webhook_log_received_date
    ON trap_ce_live_webhook_log ((CAST(received_at AS date)));
