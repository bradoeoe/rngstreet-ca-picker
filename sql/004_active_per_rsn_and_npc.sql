ALTER TABLE ca_task_catalog
  CHANGE COLUMN monster npc VARCHAR(255) NULL;

ALTER TABLE ca_user_active_tasks
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (discord_user_id, rsn),
  ADD KEY idx_active_user (discord_user_id);
