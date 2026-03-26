CREATE TABLE IF NOT EXISTS ca_boss_completion_reroll_rewards (
  discord_user_id VARCHAR(32) NOT NULL,
  rsn VARCHAR(32) NOT NULL,
  npc VARCHAR(255) NOT NULL,
  rewarded_task_id INT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (discord_user_id, rsn, npc),
  KEY idx_boss_reward_rsn (rsn),
  CONSTRAINT fk_boss_reward_task
    FOREIGN KEY (rewarded_task_id) REFERENCES ca_task_catalog(task_id)
    ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
