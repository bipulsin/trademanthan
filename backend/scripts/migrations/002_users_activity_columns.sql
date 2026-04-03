-- Add user activity/governance columns used by admintwc User Activity section

ALTER TABLE users ADD COLUMN IF NOT EXISTS is_blocked BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_paid_user BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_ip VARCHAR(64);
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_page_visited VARCHAR(255);
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_page_visited_at TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_activity_ip VARCHAR(64);

UPDATE users SET is_blocked = FALSE WHERE is_blocked IS NULL;
UPDATE users SET is_paid_user = FALSE WHERE is_paid_user IS NULL;
