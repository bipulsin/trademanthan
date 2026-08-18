-- Research-only replay of live compute_rocket_crash() on REST 10m OHLCV.
-- Do not use for live Ready Now overlay. Does not replace rocket_backtest_* tables.
CREATE TABLE IF NOT EXISTS rocket_live_replay_events (
    event_id UUID PRIMARY KEY,
    symbol TEXT NOT NULL,
    session_date DATE NOT NULL,
    bar_timestamp TIMESTAMPTZ NOT NULL,
    sess_bar_number INTEGER NOT NULL,
    rocket_score INTEGER NOT NULL,
    crash_score INTEGER NOT NULL,
    side TEXT NOT NULL,
    close_at_signal DOUBLE PRECISION,
    volume_at_signal DOUBLE PRECISION,
    delta_at_signal DOUBLE PRECISION,
    fwd_ret_1bar DOUBLE PRECISION,
    fwd_ret_3bar DOUBLE PRECISION,
    fwd_ret_5bar DOUBLE PRECISION,
    fwd_mfe_5bar DOUBLE PRECISION,
    fwd_mae_5bar DOUBLE PRECISION,
    fwd_direction_correct_1bar BOOLEAN,
    fwd_direction_correct_3bar BOOLEAN,
    adx_at_signal DOUBLE PRECISION,
    session_phase TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_rocket_live_replay_sym_dt
    ON rocket_live_replay_events (symbol, session_date, bar_timestamp);

CREATE TABLE IF NOT EXISTS rocket_live_replay_summary (
    score_bucket INTEGER NOT NULL,
    side TEXT NOT NULL,
    session_phase TEXT NOT NULL,
    adx_bucket TEXT NOT NULL,
    signal_count INTEGER NOT NULL,
    win_rate_1bar DOUBLE PRECISION,
    win_rate_3bar DOUBLE PRECISION,
    win_rate_5bar DOUBLE PRECISION,
    avg_fwd_ret_1bar DOUBLE PRECISION,
    avg_fwd_ret_3bar DOUBLE PRECISION,
    avg_fwd_ret_5bar DOUBLE PRECISION,
    avg_mfe_5bar DOUBLE PRECISION,
    avg_mae_5bar DOUBLE PRECISION,
    pct_direction_correct_1bar DOUBLE PRECISION,
    pct_direction_correct_3bar DOUBLE PRECISION,
    PRIMARY KEY (score_bucket, side, session_phase, adx_bucket)
);
