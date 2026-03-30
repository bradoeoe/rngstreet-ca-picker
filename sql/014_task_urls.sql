ALTER TABLE ca_task_catalog
  ADD COLUMN IF NOT EXISTS task_url VARCHAR(255) NULL AFTER task_name;
