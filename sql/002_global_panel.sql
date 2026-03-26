CREATE TABLE IF NOT EXISTS bot_panels (
  panel_key VARCHAR(64) NOT NULL PRIMARY KEY,
  guild_id VARCHAR(32) NULL,
  channel_id VARCHAR(32) NOT NULL,
  message_id VARCHAR(32) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS user_active_tasks (
  discord_user_id VARCHAR(32) NOT NULL PRIMARY KEY,
  rsn VARCHAR(32) NOT NULL,
  task_id INT NOT NULL,
  assigned_scan_run_id BIGINT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_active_rsn (rsn),
  KEY idx_active_task (task_id),
  KEY idx_active_scan (assigned_scan_run_id),
  CONSTRAINT fk_active_task_catalog
    FOREIGN KEY (task_id) REFERENCES ca_task_catalog(task_id)
    ON DELETE CASCADE,
  CONSTRAINT fk_active_scan_run
    FOREIGN KEY (assigned_scan_run_id) REFERENCES scan_runs(id)
    ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
