-- Tradentical users: admin flag and page permissions
-- Run against PostgreSQL database trademanthan (adjust schema if needed)

ALTER TABLE users ADD COLUMN IF NOT EXISTS "isAdmin" VARCHAR(10);
ALTER TABLE users ADD COLUMN IF NOT EXISTS page_permitted VARCHAR(255);

-- Admin rights for user ids 2 and 4 only ("Yes"); all others leave blank / NULL
UPDATE users SET "isAdmin" = 'Yes' WHERE id IN (2, 4);
