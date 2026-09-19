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

-- Seed defaults (PAPER / AUTO off) if missing
INSERT INTO tarang_settings (key, value)
VALUES
    ('mode', '"PAPER"'::jsonb),
    ('auto', 'false'::jsonb),
    ('holding_mode', '"INTRADAY"'::jsonb)
ON CONFLICT (key) DO NOTHING;
