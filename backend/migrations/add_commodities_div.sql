-- Commodities Div — isolated tables (do not reuse Kavach/Breakfast/Trap-CE).

CREATE TABLE IF NOT EXISTS commodities_div_symbol_map (
    id BIGSERIAL PRIMARY KEY,
    tv_ticker TEXT NOT NULL,
    upstox_symbol TEXT NOT NULL,
    exchange TEXT NOT NULL DEFAULT 'MCX',
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tv_ticker)
);

CREATE TABLE IF NOT EXISTS commodities_div_webhook_log (
    id BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    source_ip TEXT,
    flag TEXT,
    symbol_raw TEXT,
    symbol_mapped TEXT,
    direction TEXT,
    signal_kind TEXT,
    tv_time_ms BIGINT,
    tv_time_ist TIMESTAMP WITHOUT TIME ZONE,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    parse_status TEXT NOT NULL
        CHECK (parse_status IN ('success', 'partial', 'failed', 'unmatched')),
    disposition TEXT
        CHECK (disposition IS NULL OR disposition IN (
            'applied', 'replaced_prior', 'ignored_in_trade_block',
            'unmatched', 'parse_failed'
        )),
    active_signal_id BIGINT
);

CREATE INDEX IF NOT EXISTS ix_commodities_div_webhook_log_received
    ON commodities_div_webhook_log (received_at DESC);
CREATE INDEX IF NOT EXISTS ix_commodities_div_webhook_log_received_date
    ON commodities_div_webhook_log ((CAST(received_at AS date)));

CREATE TABLE IF NOT EXISTS commodities_div_signals (
    id BIGSERIAL PRIMARY KEY,
    symbol_raw TEXT NOT NULL,
    symbol_mapped TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('BULL', 'BEAR')),
    status TEXT NOT NULL CHECK (status IN (
        'Divergence', 'Activated', 'In-Trade', 'Exit Trade', 'History'
    )),
    div_received_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    div_tv_time_ist TIMESTAMP WITHOUT TIME ZONE,
    div_webhook_log_id BIGINT,
    go_received_at TIMESTAMP WITHOUT TIME ZONE,
    go_tv_time_ist TIMESTAMP WITHOUT TIME ZONE,
    go_webhook_log_id BIGINT,
    trade_taken_at TIMESTAMP WITHOUT TIME ZONE,
    entry_price DOUBLE PRECISION,
    ltp DOUBLE PRECISION,
    ltp_updated_at TIMESTAMP WITHOUT TIME ZONE,
    instrument_key TEXT,
    contract TEXT,
    lot_size INTEGER,
    exit_signal_received_at TIMESTAMP WITHOUT TIME ZONE,
    exit_tv_time_ist TIMESTAMP WITHOUT TIME ZONE,
    exit_webhook_log_id BIGINT,
    exit_submitted_at TIMESTAMP WITHOUT TIME ZONE,
    exit_price DOUBLE PRECISION,
    exit_at TIMESTAMP WITHOUT TIME ZONE,
    trade_log_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- At most one non-History (active cycle) row.
CREATE UNIQUE INDEX IF NOT EXISTS uq_commodities_div_one_active
    ON commodities_div_signals ((TRUE))
    WHERE status <> 'History';
