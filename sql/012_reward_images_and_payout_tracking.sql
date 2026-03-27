ALTER TABLE ca_rewards
  ADD COLUMN IF NOT EXISTS reward_image_url VARCHAR(512) NULL AFTER reward_quantity,
  ADD COLUMN IF NOT EXISTS payout_status VARCHAR(16) NOT NULL DEFAULT 'unpaid' AFTER used_at,
  ADD COLUMN IF NOT EXISTS payout_marked_at DATETIME NULL AFTER payout_status,
  ADD COLUMN IF NOT EXISTS payout_marked_by VARCHAR(255) NULL AFTER payout_marked_at,
  ADD COLUMN IF NOT EXISTS payout_notes TEXT NULL AFTER payout_marked_by;
