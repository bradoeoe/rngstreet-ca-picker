ALTER TABLE ca_rewards
  ADD COLUMN IF NOT EXISTS reward_kind VARCHAR(16) NULL AFTER reward_tier,
  ADD COLUMN IF NOT EXISTS reward_label VARCHAR(255) NULL AFTER reward_kind,
  ADD COLUMN IF NOT EXISTS reward_quantity INT NULL AFTER reward_amount;
