CREATE TABLE IF NOT EXISTS ca_rewards (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  reward_key VARCHAR(32) NOT NULL,
  discord_user_id VARCHAR(32) NOT NULL,
  rsn VARCHAR(32) NOT NULL,
  task_id INT NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'pending_verification',
  reward_tier VARCHAR(16) NULL,
  reward_kind VARCHAR(16) NULL,
  reward_label VARCHAR(255) NULL,
  reward_amount BIGINT NULL,
  reward_quantity INT NULL,
  reward_image_url VARCHAR(512) NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  verified_at DATETIME NULL,
  used_at DATETIME NULL,
  payout_status VARCHAR(16) NOT NULL DEFAULT 'unpaid',
  payout_marked_at DATETIME NULL,
  payout_marked_by VARCHAR(255) NULL,
  payout_notes TEXT NULL,
  UNIQUE KEY uq_ca_rewards_key (reward_key),
  UNIQUE KEY uq_ca_rewards_claim (discord_user_id, rsn, task_id),
  KEY idx_ca_rewards_status (status, created_at),
  KEY idx_ca_rewards_rsn (rsn, status),
  KEY idx_ca_rewards_payout (payout_status, used_at),
  CONSTRAINT fk_ca_rewards_task
    FOREIGN KEY (task_id) REFERENCES ca_task_catalog(task_id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
