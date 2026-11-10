-- Encrypted broker access tokens shared across workers (Dhan TOTP login).
CREATE TABLE IF NOT EXISTS broker_tokens (
    broker TEXT PRIMARY KEY,
    token_encrypted TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);
