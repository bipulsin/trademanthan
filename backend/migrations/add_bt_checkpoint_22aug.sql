-- Kavach 22-Aug BT checkpoint research tables (do not alter trade_log semantics)
-- Applied also via backend.services.kavach_bt_checkpoint.db.ensure_bt_checkpoint_tables()

CREATE TABLE IF NOT EXISTS bt_checkpoint_trade_detail (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    trade_log_id BIGINT NOT NULL,
    session_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT,
    entry_time TIMESTAMPTZ,
    entry_price DOUBLE PRECISION,
    exit_time TIMESTAMPTZ,
    exit_price DOUBLE PRECISION,
    grade TEXT,
    r_realized DOUBLE PRECISION,
    mfe_r DOUBLE PRECISION,
    mae_r DOUBLE PRECISION,
    pnl DOUBLE PRECISION,
    pb_legacy INTEGER,
    pb_v2 INTEGER,
    pb_hard_blocked BOOLEAN NOT NULL DEFAULT FALSE,
    res_confluence BOOLEAN NOT NULL DEFAULT FALSE,
    nearest_pivot DOUBLE PRECISION,
    pivot_kind TEXT,
    pivot_zone_pct DOUBLE PRECISION,
    cluster_n INTEGER,
    exit_a_price DOUBLE PRECISION,
    exit_a_time TEXT,
    exit_a_r DOUBLE PRECISION,
    exit_a_reason TEXT,
    exit_b_price DOUBLE PRECISION,
    exit_b_time TEXT,
    exit_b_r DOUBLE PRECISION,
    exit_b_reason TEXT,
    exit_c_price DOUBLE PRECISION,
    exit_c_time TEXT,
    exit_c_r DOUBLE PRECISION,
    exit_c_reason TEXT,
    exit_c_trigger_type TEXT,
    best_exit_method TEXT,
    garuda_confluence TEXT,
    garuda_rank INTEGER,
    garuda_direction TEXT,
    components JSONB,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, trade_log_id)
);

CREATE INDEX IF NOT EXISTS idx_bt_cp_detail_run
ON bt_checkpoint_trade_detail (run_id, session_date, symbol);

CREATE TABLE IF NOT EXISTS bt_checkpoint_pullback_bars (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    session_date DATE NOT NULL,
    bar_end TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    pb_legacy INTEGER,
    pb_v2 INTEGER,
    touched_ema5 BOOLEAN,
    touched_ema10 BOOLEAN,
    touched_vwap BOOLEAN,
    dual_reset BOOLEAN,
    UNIQUE (run_id, session_date, bar_end, symbol)
);

CREATE TABLE IF NOT EXISTS bt_checkpoint_summary (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    cohort_type TEXT NOT NULL,
    cohort_key TEXT NOT NULL,
    n INTEGER NOT NULL DEFAULT 0,
    win_rate DOUBLE PRECISION,
    avg_r DOUBLE PRECISION,
    total_pnl DOUBLE PRECISION,
    avg_mfe DOUBLE PRECISION,
    avg_mae DOUBLE PRECISION,
    recommendation_text TEXT,
    extras JSONB,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, cohort_type, cohort_key)
);
