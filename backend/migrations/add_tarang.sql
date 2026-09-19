-- Kosmic Tarang — isolated tables (prefix tarang_*). Idempotent via ensure_tarang_tables().

CREATE TABLE IF NOT EXISTS tarang_settings (
    id BIGSERIAL PRIMARY KEY,
    key TEXT NOT NULL UNIQUE,
    value JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tarang_iv_snapshots (
    id BIGSERIAL PRIMARY KEY,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    profile_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    underlying_symbol TEXT NOT NULL,
    expiry_date DATE,
    futures_or_spot DOUBLE PRECISION,
    atm_strike DOUBLE PRECISION,
    atm_iv DOUBLE PRECISION,
    atm_iv_raw DOUBLE PRECISION,
    atm_iv_unit TEXT,
    skew_25d DOUBLE PRECISION,
    put_iv_25d DOUBLE PRECISION,
    call_iv_25d DOUBLE PRECISION,
    realized_vol_20d DOUBLE PRECISION,
    source TEXT,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_tarang_iv_snapshots_profile_time
    ON tarang_iv_snapshots (profile_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS ix_tarang_iv_snapshots_underlying_time
    ON tarang_iv_snapshots (underlying_symbol, captured_at DESC);

CREATE TABLE IF NOT EXISTS tarang_feed_health (
    id BIGSERIAL PRIMARY KEY,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    venue TEXT NOT NULL,
    status TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_tarang_feed_health_venue_time
    ON tarang_feed_health (venue, checked_at DESC);

CREATE TABLE IF NOT EXISTS tarang_event_calendar (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    label TEXT NOT NULL,
    starts_at TIMESTAMPTZ NOT NULL,
    applies_to JSONB NOT NULL DEFAULT '[]'::jsonb,
    blackout_hours_before DOUBLE PRECISION NOT NULL DEFAULT 2.5,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_tarang_event_calendar_starts
    ON tarang_event_calendar (starts_at);

-- Phase 2+ scaffolds (created early so ensure_* is complete)
CREATE TABLE IF NOT EXISTS tarang_candidates (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    profile_id TEXT NOT NULL,
    structure TEXT,
    status TEXT NOT NULL DEFAULT 'screened',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_tarang_candidates_profile_time
    ON tarang_candidates (profile_id, created_at DESC);

CREATE TABLE IF NOT EXISTS tarang_rejections (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    profile_id TEXT NOT NULL,
    gate_name TEXT NOT NULL,
    detail TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_tarang_rejections_profile_time
    ON tarang_rejections (profile_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_tarang_rejections_gate_time
    ON tarang_rejections (gate_name, created_at DESC);

CREATE TABLE IF NOT EXISTS tarang_trades (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    profile_id TEXT NOT NULL,
    risk_bucket TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'PAPER'
        CHECK (mode IN ('PAPER', 'LIVE')),
    holding_mode TEXT NOT NULL DEFAULT 'INTRADAY',
    status TEXT NOT NULL DEFAULT 'open',
    auto_managed BOOLEAN NOT NULL DEFAULT FALSE,
    entry_credit DOUBLE PRECISION,
    max_loss DOUBLE PRECISION,
    lots_or_contracts INTEGER,
    legs JSONB NOT NULL DEFAULT '[]'::jsonb,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS tarang_orders (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trade_id BIGINT REFERENCES tarang_trades(id) ON DELETE SET NULL,
    client_order_id TEXT,
    venue TEXT NOT NULL,
    side TEXT,
    status TEXT,
    raw JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_tarang_orders_client_order_id
    ON tarang_orders (client_order_id)
    WHERE client_order_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS tarang_alerts (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    level TEXT NOT NULL DEFAULT 'info',
    message TEXT NOT NULL,
    trade_id BIGINT,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Phase 3: append-only trade lifecycle events
CREATE TABLE IF NOT EXISTS tarang_trade_events (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trade_id BIGINT REFERENCES tarang_trades(id) ON DELETE SET NULL,
    candidate_id BIGINT,
    from_status TEXT,
    to_status TEXT NOT NULL,
    actor TEXT NOT NULL
        CHECK (actor IN ('USER', 'AUTO', 'SYSTEM')),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_tarang_trade_events_trade_time
    ON tarang_trade_events (trade_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_tarang_trade_events_type_time
    ON tarang_trade_events (event_type, created_at DESC);

CREATE TABLE IF NOT EXISTS tarang_fills (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trade_id BIGINT REFERENCES tarang_trades(id) ON DELETE CASCADE,
    order_id BIGINT REFERENCES tarang_orders(id) ON DELETE SET NULL,
    leg_index INTEGER,
    side TEXT,
    symbol TEXT,
    qty DOUBLE PRECISION,
    price DOUBLE PRECISION,
    fees DOUBLE PRECISION NOT NULL DEFAULT 0,
    phase TEXT NOT NULL DEFAULT 'entry'
        CHECK (phase IN ('entry', 'exit')),
    raw JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_tarang_fills_trade
    ON tarang_fills (trade_id, phase);

-- Phase 3 columns on tarang_trades (idempotent)
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS candidate_id BIGINT;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS venue TEXT;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS structure TEXT;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS exit_reason TEXT;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS entry_at TIMESTAMPTZ;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS exit_at TIMESTAMPTZ;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS gross_pnl DOUBLE PRECISION;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS net_pnl DOUBLE PRECISION;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS fees_total DOUBLE PRECISION;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS entry_iv_percentile DOUBLE PRECISION;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS exit_iv_percentile DOUBLE PRECISION;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS notes TEXT;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS entry_debit_to_close DOUBLE PRECISION;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS exit_debit_to_close DOUBLE PRECISION;
ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS currency TEXT DEFAULT 'INR';

CREATE INDEX IF NOT EXISTS ix_tarang_trades_status_time
    ON tarang_trades (status, updated_at DESC);
CREATE INDEX IF NOT EXISTS ix_tarang_trades_mode_status
    ON tarang_trades (mode, status);

-- Seed defaults (PAPER / AUTO off) if missing
INSERT INTO tarang_settings (key, value)
VALUES
    ('mode', '"PAPER"'::jsonb),
    ('auto', 'false'::jsonb),
    ('holding_mode', '"INTRADAY"'::jsonb)
ON CONFLICT (key) DO NOTHING;

-- Full-chain snapshots (gzip BYTEA). ATM IV rows remain in tarang_iv_snapshots.
CREATE TABLE IF NOT EXISTS tarang_chain_snapshots (
    id BIGSERIAL PRIMARY KEY,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    profile_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    underlying_symbol TEXT NOT NULL,
    expiry_date DATE,
    futures_or_spot DOUBLE PRECISION,
    atm_strike DOUBLE PRECISION,
    atm_iv DOUBLE PRECISION,
    ist_dow SMALLINT NOT NULL,
    ist_hour SMALLINT NOT NULL,
    ist_weekend_window BOOLEAN NOT NULL DEFAULT FALSE,
    n_quotes INTEGER NOT NULL DEFAULT 0,
    payload_gzip BYTEA NOT NULL,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_tarang_chain_snapshots_underlying_time
    ON tarang_chain_snapshots (underlying_symbol, captured_at DESC);
CREATE INDEX IF NOT EXISTS ix_tarang_chain_snapshots_profile_time
    ON tarang_chain_snapshots (profile_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS ix_tarang_chain_snapshots_venue_dow
    ON tarang_chain_snapshots (venue, ist_dow, ist_hour);

CREATE TABLE IF NOT EXISTS tarang_alert_dedupe (
    dedupe_key TEXT PRIMARY KEY,
    last_sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_message TEXT
);

-- Tag ATM±10 rows captured before the delta-window change.
UPDATE tarang_chain_snapshots
SET meta = COALESCE(meta, '{}'::jsonb) || '{"window_kind":"narrow_window"}'::jsonb
WHERE COALESCE(meta->>'window_kind', '') NOT IN ('delta_window', 'narrow_window');

CREATE TABLE IF NOT EXISTS tarang_data_gaps (
    id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    venue TEXT NOT NULL,
    profile_id TEXT,
    expected_at TIMESTAMPTZ,
    reason TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS ix_tarang_data_gaps_venue_time
    ON tarang_data_gaps (venue, expected_at DESC);

CREATE TABLE IF NOT EXISTS tarang_liquidity_probes (
    id BIGSERIAL PRIMARY KEY,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ist_hour SMALLINT,
    underlying_symbol TEXT NOT NULL,
    expiry_date DATE,
    payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_tarang_liq_probes_und_time
    ON tarang_liquidity_probes (underlying_symbol, captured_at DESC);

ALTER TABLE tarang_trades ADD COLUMN IF NOT EXISTS origin TEXT;

CREATE TABLE IF NOT EXISTS tarang_hist_imports (
    id BIGSERIAL PRIMARY KEY,
    filename TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    report JSONB
);

CREATE TABLE IF NOT EXISTS tarang_hist_eod (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    expiry_date DATE NOT NULL,
    option_type TEXT NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    trade_date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    prev_close DOUBLE PRECISION,
    volume_lots INTEGER,
    value_lacs DOUBLE PRECISION,
    oi_lots INTEGER,
    instrument_name TEXT,
    traded BOOLEAN NOT NULL DEFAULT FALSE,
    source_filename TEXT,
    import_id BIGINT REFERENCES tarang_hist_imports(id) ON DELETE SET NULL,
    iv DOUBLE PRECISION,
    delta DOUBLE PRECISION,
    greeks_source TEXT,
    underlying_price DOUBLE PRECISION,
    underlying_source TEXT,
    reconstruction_run_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_tarang_hist_eod_key
    ON tarang_hist_eod (symbol, expiry_date, option_type, strike, trade_date);
CREATE INDEX IF NOT EXISTS ix_tarang_hist_eod_symbol_date
    ON tarang_hist_eod (symbol, trade_date);
CREATE INDEX IF NOT EXISTS ix_tarang_hist_eod_recon
    ON tarang_hist_eod (reconstruction_run_id);

ALTER TABLE tarang_hist_eod ADD COLUMN IF NOT EXISTS iv DOUBLE PRECISION;
ALTER TABLE tarang_hist_eod ADD COLUMN IF NOT EXISTS delta DOUBLE PRECISION;
ALTER TABLE tarang_hist_eod ADD COLUMN IF NOT EXISTS greeks_source TEXT;
ALTER TABLE tarang_hist_eod ADD COLUMN IF NOT EXISTS underlying_price DOUBLE PRECISION;
ALTER TABLE tarang_hist_eod ADD COLUMN IF NOT EXISTS underlying_source TEXT;
ALTER TABLE tarang_hist_eod ADD COLUMN IF NOT EXISTS reconstruction_run_id TEXT;

