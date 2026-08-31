-- Breakfast live session freeze metadata (9:20:30 IST lock)
CREATE TABLE IF NOT EXISTS breakfast_session_lock (
    session_date DATE PRIMARY KEY,
    locked_at TIMESTAMPTZ NOT NULL,
    locked_by TEXT NOT NULL DEFAULT 'auto',
    lock_status TEXT NOT NULL DEFAULT 'locked'
        CHECK (lock_status IN ('locked', 'failed')),
    failure_reason TEXT,
    signal_count SMALLINT NOT NULL DEFAULT 0,
    capture_source TEXT NOT NULL DEFAULT 'live_scheduler',
    payload_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_breakfast_session_lock_locked_at
    ON breakfast_session_lock (locked_at DESC);

ALTER TABLE breakfast_live_signals
    ADD COLUMN IF NOT EXISTS capture_source TEXT;
