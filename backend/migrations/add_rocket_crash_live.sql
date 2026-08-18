-- Rocket/Crash live rolling state + append-only threshold event log
-- Applied at process start via database._run_startup_schema_migrations
-- and backend.services.rocket_ws_live.ensure_rocket_live_tables.

CREATE TABLE IF NOT EXISTS rocket_live_state (
    symbol TEXT NOT NULL,
    timeframe INTEGER NOT NULL,
    candle_start TIMESTAMPTZ,
    candle_end TIMESTAMPTZ,
    rocket_score INTEGER NOT NULL DEFAULT 0,
    rocket_signals TEXT,
    rocket_label TEXT,
    crash_score INTEGER NOT NULL DEFAULT 0,
    crash_signals TEXT,
    crash_label TEXT,
    active_side TEXT,
    candle_delta DOUBLE PRECISION,
    session_cum_delta DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    ema5 DOUBLE PRECISION,
    atr10 DOUBLE PRECISION,
    lookback_used INTEGER,
    session_bar_number INTEGER,
    candle_status TEXT,
    data_quality_flag TEXT,
    last_update TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL DEFAULT 'upstox_websocket_live',
    PRIMARY KEY (symbol, timeframe)
);
CREATE INDEX IF NOT EXISTS ix_rocket_live_state_upd
    ON rocket_live_state (last_update DESC);
CREATE INDEX IF NOT EXISTS ix_rocket_live_state_rocket
    ON rocket_live_state (rocket_score DESC);
CREATE INDEX IF NOT EXISTS ix_rocket_live_state_crash
    ON rocket_live_state (crash_score DESC);

CREATE TABLE IF NOT EXISTS rocket_crash_event_log (
    event_id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    timeframe INTEGER NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    candle_timestamp TIMESTAMPTZ,
    side TEXT NOT NULL,
    score INTEGER NOT NULL,
    s1_flag BOOLEAN NOT NULL DEFAULT FALSE,
    s2_flag BOOLEAN NOT NULL DEFAULT FALSE,
    s3_flag BOOLEAN NOT NULL DEFAULT FALSE,
    s4_flag BOOLEAN NOT NULL DEFAULT FALSE,
    close DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    candle_delta DOUBLE PRECISION,
    cumulative_delta_session DOUBLE PRECISION,
    ema5 DOUBLE PRECISION,
    atr10 DOUBLE PRECISION,
    lookback_used INTEGER,
    session_bar_number INTEGER,
    candle_status TEXT,
    source TEXT NOT NULL DEFAULT 'upstox_websocket_live',
    data_quality_flag TEXT,
    notes TEXT
);
CREATE INDEX IF NOT EXISTS ix_rocket_crash_event_sym_ts
    ON rocket_crash_event_log (symbol, timeframe, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_rocket_crash_event_side_score
    ON rocket_crash_event_log (side, score, event_timestamp DESC);

ALTER TABLE rs_universe_score_snapshot ADD COLUMN IF NOT EXISTS crash_score INTEGER;
ALTER TABLE rs_universe_score_snapshot ADD COLUMN IF NOT EXISTS crash_signals TEXT;
ALTER TABLE rs_universe_score_snapshot ADD COLUMN IF NOT EXISTS crash_label TEXT;
