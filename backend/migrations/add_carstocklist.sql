-- Migration: Create carstocklist table for CAR GPT
-- Run: python3 backend/migrations/add_carstocklist.py

CREATE TABLE IF NOT EXISTS carstocklist (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_carstocklist_symbol ON carstocklist(symbol);
CREATE INDEX IF NOT EXISTS ix_carstocklist_id ON carstocklist(id);
