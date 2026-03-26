ALTER TABLE ca_task_catalog
  ADD COLUMN npc_url VARCHAR(255) NULL AFTER npc,
  ADD COLUMN npc_image_url VARCHAR(255) NULL AFTER npc_url;
