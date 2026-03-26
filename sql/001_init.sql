CREATE TABLE IF NOT EXISTS ca_schema_migrations (
  version VARCHAR(64) NOT NULL PRIMARY KEY,
  applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ca_scan_runs (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  trigger_source VARCHAR(32) NOT NULL,
  status VARCHAR(24) NOT NULL,
  started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  completed_at DATETIME NULL,
  total_users INT NOT NULL DEFAULT 0,
  success_users INT NOT NULL DEFAULT 0,
  failed_users INT NOT NULL DEFAULT 0,
  error_text TEXT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ca_player_snapshots (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  scan_run_id BIGINT NOT NULL,
  rsn VARCHAR(32) NOT NULL,
  source_timestamp DATETIME NULL,
  fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  payload_json LONGTEXT NOT NULL,
  UNIQUE KEY uq_snapshot_run_rsn (scan_run_id, rsn),
  KEY idx_snapshot_rsn_fetched (rsn, fetched_at),
  CONSTRAINT fk_snapshot_scan_run
    FOREIGN KEY (scan_run_id) REFERENCES ca_scan_runs(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ca_task_catalog (
  task_id INT NOT NULL PRIMARY KEY,
  task_name VARCHAR(255) NOT NULL,
  monster VARCHAR(255) NULL,
  task_type VARCHAR(64) NULL,
  tier_label VARCHAR(64) NULL,
  points INT NULL,
  source_url VARCHAR(255) NOT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ca_progress (
  rsn VARCHAR(32) NOT NULL,
  task_id INT NOT NULL,
  is_complete TINYINT(1) NOT NULL,
  source VARCHAR(24) NOT NULL,
  source_scan_run_id BIGINT NULL,
  last_changed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (rsn, task_id),
  KEY idx_progress_rsn_complete (rsn, is_complete),
  KEY idx_progress_scan_run (source_scan_run_id),
  CONSTRAINT fk_progress_task
    FOREIGN KEY (task_id) REFERENCES ca_task_catalog(task_id)
    ON DELETE CASCADE,
  CONSTRAINT fk_progress_scan_run
    FOREIGN KEY (source_scan_run_id) REFERENCES ca_scan_runs(id)
    ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ca_task_claims (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  discord_user_id VARCHAR(32) NOT NULL,
  rsn VARCHAR(32) NOT NULL,
  task_id INT NOT NULL,
  status VARCHAR(32) NOT NULL,
  claim_scan_run_id BIGINT NULL,
  verified_scan_run_id BIGINT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  last_verified_at DATETIME NULL,
  guild_id VARCHAR(32) NULL,
  channel_id VARCHAR(32) NULL,
  message_id VARCHAR(32) NULL,
  UNIQUE KEY uq_claim_user_rsn_task (discord_user_id, rsn, task_id),
  KEY idx_claim_rsn_status (rsn, status),
  KEY idx_claim_scan (claim_scan_run_id, discord_user_id, rsn),
  CONSTRAINT fk_claim_task
    FOREIGN KEY (task_id) REFERENCES ca_task_catalog(task_id)
    ON DELETE CASCADE,
  CONSTRAINT fk_claim_scan_run
    FOREIGN KEY (claim_scan_run_id) REFERENCES ca_scan_runs(id)
    ON DELETE SET NULL,
  CONSTRAINT fk_claim_verify_scan_run
    FOREIGN KEY (verified_scan_run_id) REFERENCES ca_scan_runs(id)
    ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
