from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import mysql.connector
from mysql.connector import MySQLConnection

from .config import Settings

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class TaskCatalogEntry:
    task_id: int
    task_name: str
    description: str | None
    npc: str | None
    npc_url: str | None
    npc_image_url: str | None
    task_type: str | None
    tier_label: str | None
    points: int | None
    source_url: str


@dataclass(slots=True)
class ScanRunResult:
    run_id: int
    status: str
    total_users: int
    success_users: int
    failed_users: int


class Database:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    @contextmanager
    def connection(self) -> Iterator[MySQLConnection]:
        conn = mysql.connector.connect(**self._settings.db_settings)
        try:
            yield conn
        finally:
            conn.close()

    def run_migrations(self, migrations_dir: Path) -> None:
        with self.connection() as conn:
            conn.start_transaction()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version VARCHAR(64) NOT NULL PRIMARY KEY,
                        applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )

                cursor.execute("SELECT version FROM schema_migrations")
                applied = {row[0] for row in cursor.fetchall()}

                sql_files = sorted(migrations_dir.glob("*.sql"))
                for path in sql_files:
                    version = path.stem
                    if version in applied:
                        continue

                    LOGGER.info("Applying migration %s", version)
                    sql_text = path.read_text(encoding="utf-8")
                    for statement in _split_sql_statements(sql_text):
                        cursor.execute(statement)

                    cursor.execute(
                        "INSERT INTO schema_migrations (version) VALUES (%s)",
                        (version,),
                    )

                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def fetch_rsns(self, conn: MySQLConnection) -> list[str]:
        if not self._settings.user_source_sql.strip():
            return []

        cursor = conn.cursor()
        cursor.execute(self._settings.user_source_sql)
        rows = cursor.fetchall()
        rsns: list[str] = []
        for row in rows:
            if not row:
                continue
            value = str(row[0]).strip()
            if value:
                rsns.append(value)

        seen = set()
        deduped: list[str] = []
        for rsn in rsns:
            key = rsn.casefold()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(rsn)
        return deduped

    def get_rsns_for_discord_user(self, conn: MySQLConnection, discord_user_id: str) -> list[str]:
        cursor = conn.cursor()
        rsns: list[str] = []
        seen: set[str] = set()

        def _append_rsn(value) -> None:
            if value is None:
                return
            rsn = str(value).strip()
            if not rsn:
                return
            key = rsn.casefold()
            if key in seen:
                return
            seen.add(key)
            rsns.append(rsn)

        # Prefer direct mappings on members (current source of truth in this environment).
        try:
            cursor.execute(
                """
                SELECT RSN
                FROM members
                WHERE DISCORD_ID = %s
                  AND TRIM(COALESCE(RSN, '')) <> ''
                ORDER BY CASE WHEN COALESCE(MAIN_WOM_ID, 0) = 0 THEN 0 ELSE 1 END, WOM_ID
                """,
                (discord_user_id,),
            )
            for row in cursor.fetchall():
                if row:
                    _append_rsn(row[0])
        except mysql.connector.Error:
            LOGGER.warning("Could not resolve RSN via members table for discord user %s", discord_user_id)

        if rsns:
            return rsns

        # Fallback to map table when direct member mapping is not available.
        try:
            cursor.execute(
                """
                SELECT m.RSN
                FROM main_rsn_map map
                JOIN members m
                  ON m.WOM_ID = map.WOM_ID
                WHERE map.DISCORD_ID = %s
                  AND TRIM(COALESCE(m.RSN, '')) <> ''
                ORDER BY m.WOM_ID
                """,
                (discord_user_id,),
            )
            for row in cursor.fetchall():
                if row:
                    _append_rsn(row[0])
        except mysql.connector.Error:
            LOGGER.warning("Could not resolve RSN via main_rsn_map for discord user %s", discord_user_id)

        return rsns

    def get_rsn_for_discord_user(self, conn: MySQLConnection, discord_user_id: str) -> str | None:
        rsns = self.get_rsns_for_discord_user(conn, discord_user_id)
        return rsns[0] if rsns else None

    def create_scan_run(self, conn: MySQLConnection, trigger_source: str, total_users: int) -> int:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO scan_runs (trigger_source, status, total_users)
            VALUES (%s, 'running', %s)
            """,
            (trigger_source, total_users),
        )
        return int(cursor.lastrowid)

    def finish_scan_run(
        self,
        conn: MySQLConnection,
        run_id: int,
        status: str,
        success_users: int,
        failed_users: int,
        error_text: str | None = None,
    ) -> None:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE scan_runs
            SET status = %s,
                completed_at = CURRENT_TIMESTAMP,
                success_users = %s,
                failed_users = %s,
                error_text = %s
            WHERE id = %s
            """,
            (status, success_users, failed_users, error_text, run_id),
        )

    def insert_player_snapshot(
        self,
        conn: MySQLConnection,
        run_id: int,
        rsn: str,
        source_timestamp,
        payload_json: str,
    ) -> None:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO player_snapshots (scan_run_id, rsn, source_timestamp, payload_json)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                source_timestamp = VALUES(source_timestamp),
                payload_json = VALUES(payload_json),
                fetched_at = CURRENT_TIMESTAMP
            """,
            (run_id, rsn, source_timestamp, payload_json),
        )

    def upsert_task_catalog(self, conn: MySQLConnection, entries: Sequence[TaskCatalogEntry]) -> None:
        if not entries:
            return

        rows = [
            (
                entry.task_id,
                entry.task_name,
                entry.description,
                entry.npc,
                entry.npc_url,
                entry.npc_image_url,
                entry.task_type,
                entry.tier_label,
                entry.points,
                entry.source_url,
            )
            for entry in entries
        ]

        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT INTO ca_task_catalog (
                task_id,
                task_name,
                description,
                npc,
                npc_url,
                npc_image_url,
                task_type,
                tier_label,
                points,
                source_url
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                task_name = VALUES(task_name),
                description = VALUES(description),
                npc = VALUES(npc),
                npc_url = VALUES(npc_url),
                npc_image_url = VALUES(npc_image_url),
                task_type = VALUES(task_type),
                tier_label = VALUES(tier_label),
                points = VALUES(points),
                source_url = VALUES(source_url),
                updated_at = CURRENT_TIMESTAMP
            """,
            rows,
        )

    def get_catalog_task_ids(self, conn: MySQLConnection) -> list[int]:
        cursor = conn.cursor()
        cursor.execute("SELECT task_id FROM ca_task_catalog ORDER BY task_id")
        return [int(row[0]) for row in cursor.fetchall()]

    def get_task_metadata(self, conn: MySQLConnection, task_id: int) -> dict | None:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT
                task_id,
                task_name,
                description,
                npc,
                npc_url,
                npc_image_url,
                task_type,
                tier_label,
                points
            FROM ca_task_catalog
            WHERE task_id = %s
            """,
            (task_id,),
        )
        return cursor.fetchone()

    def get_task_metadata_for_ids(self, conn: MySQLConnection, task_ids: Sequence[int]) -> dict[int, dict]:
        if not task_ids:
            return {}
        placeholders = ", ".join(["%s"] * len(task_ids))
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"""
            SELECT
                task_id,
                task_name,
                description,
                npc,
                npc_url,
                npc_image_url,
                task_type,
                tier_label,
                points
            FROM ca_task_catalog
            WHERE task_id IN ({placeholders})
            """,
            tuple(int(task_id) for task_id in task_ids),
        )
        rows = cursor.fetchall()
        return {int(row["task_id"]): row for row in rows}

    def get_task_completion_state(self, conn: MySQLConnection, rsn: str, task_id: int) -> bool | None:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT is_complete
            FROM ca_progress
            WHERE rsn = %s
              AND task_id = %s
            """,
            (rsn, task_id),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return bool(row[0])

    def upsert_progress_rows(
        self,
        conn: MySQLConnection,
        rsn: str,
        completed_task_ids: set[int],
        universe_task_ids: Iterable[int],
        source: str,
        source_scan_run_id: int | None,
    ) -> None:
        rows = []
        for task_id in universe_task_ids:
            rows.append(
                (
                    rsn,
                    int(task_id),
                    1 if task_id in completed_task_ids else 0,
                    source,
                    source_scan_run_id,
                )
            )

        if not rows:
            return

        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT INTO ca_progress (rsn, task_id, is_complete, source, source_scan_run_id)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                is_complete = VALUES(is_complete),
                source = VALUES(source),
                source_scan_run_id = VALUES(source_scan_run_id),
                last_changed_at = CURRENT_TIMESTAMP
            """,
            rows,
        )

    def upsert_single_progress(
        self,
        conn: MySQLConnection,
        rsn: str,
        task_id: int,
        is_complete: bool,
        source: str,
        source_scan_run_id: int | None,
    ) -> None:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO ca_progress (rsn, task_id, is_complete, source, source_scan_run_id)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                is_complete = VALUES(is_complete),
                source = VALUES(source),
                source_scan_run_id = VALUES(source_scan_run_id),
                last_changed_at = CURRENT_TIMESTAMP
            """,
            (rsn, task_id, 1 if is_complete else 0, source, source_scan_run_id),
        )

    def get_latest_completed_scan_run_id(self, conn: MySQLConnection) -> int | None:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(id) FROM scan_runs WHERE status = 'completed'")
        row = cursor.fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0])

    def get_incomplete_task_ids(self, conn: MySQLConnection, rsn: str) -> set[int]:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT task_id FROM ca_progress WHERE rsn = %s AND is_complete = 0",
            (rsn,),
        )
        return {int(row[0]) for row in cursor.fetchall()}

    def get_claimed_task_ids_for_scan(
        self,
        conn: MySQLConnection,
        discord_user_id: str,
        rsn: str,
        scan_run_id: int | None,
    ) -> set[int]:
        if scan_run_id is None:
            return set()

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT task_id
            FROM task_claims
            WHERE discord_user_id = %s
              AND rsn = %s
              AND claim_scan_run_id = %s
              AND status IN ('claimed_complete', 'verified_complete')
            """,
            (discord_user_id, rsn, scan_run_id),
        )
        return {int(row[0]) for row in cursor.fetchall()}

    def mark_task_claimed_complete(
        self,
        conn: MySQLConnection,
        discord_user_id: str,
        rsn: str,
        task_id: int,
        claim_scan_run_id: int | None,
        guild_id: str | None,
        channel_id: str | None,
        message_id: str | None,
    ) -> None:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO task_claims (
                discord_user_id,
                rsn,
                task_id,
                status,
                claim_scan_run_id,
                guild_id,
                channel_id,
                message_id
            )
            VALUES (%s, %s, %s, 'claimed_complete', %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                status = 'claimed_complete',
                claim_scan_run_id = VALUES(claim_scan_run_id),
                guild_id = VALUES(guild_id),
                channel_id = VALUES(channel_id),
                message_id = VALUES(message_id),
                updated_at = CURRENT_TIMESTAMP
            """,
            (discord_user_id, rsn, task_id, claim_scan_run_id, guild_id, channel_id, message_id),
        )

    def mark_task_verified(
        self,
        conn: MySQLConnection,
        discord_user_id: str,
        rsn: str,
        task_id: int,
        is_complete: bool,
        verified_scan_run_id: int | None,
    ) -> str:
        new_status = "verified_complete" if is_complete else "corrected_incomplete"
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO task_claims (
                discord_user_id,
                rsn,
                task_id,
                status,
                claim_scan_run_id,
                verified_scan_run_id,
                last_verified_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                verified_scan_run_id = VALUES(verified_scan_run_id),
                last_verified_at = VALUES(last_verified_at),
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                discord_user_id,
                rsn,
                task_id,
                new_status,
                verified_scan_run_id,
                verified_scan_run_id,
            ),
        )
        return new_status

    def reconcile_claims_with_scan(
        self,
        conn: MySQLConnection,
        rsn: str,
        completed_task_ids: set[int],
        scan_run_id: int,
    ) -> tuple[int, int]:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, task_id, status
            FROM task_claims
            WHERE rsn = %s
            """,
            (rsn,),
        )

        verified = 0
        corrected = 0
        for claim_id, task_id, _status in cursor.fetchall():
            task_id = int(task_id)
            is_complete = task_id in completed_task_ids
            next_status = "verified_complete" if is_complete else "corrected_incomplete"
            if is_complete:
                verified += 1
            else:
                corrected += 1

            cursor.execute(
                """
                UPDATE task_claims
                SET status = %s,
                    verified_scan_run_id = %s,
                    last_verified_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (next_status, scan_run_id, claim_id),
            )

        return verified, corrected

    def ensure_task_stub(self, conn: MySQLConnection, task_id: int, source_url: str) -> None:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT IGNORE INTO ca_task_catalog (task_id, task_name, source_url)
            VALUES (%s, %s, %s)
            """,
            (task_id, f"Task {task_id}", source_url),
        )

    def get_latest_snapshot(self, conn: MySQLConnection, rsn: str) -> dict | None:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT id, scan_run_id, rsn, source_timestamp, fetched_at, payload_json
            FROM player_snapshots
            WHERE rsn = %s
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (rsn,),
        )
        return cursor.fetchone()

    def get_progress_summary(self, conn: MySQLConnection, rsn: str) -> dict[str, int]:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                SUM(CASE WHEN is_complete = 1 THEN 1 ELSE 0 END) AS completed_count,
                SUM(CASE WHEN is_complete = 0 THEN 1 ELSE 0 END) AS incomplete_count
            FROM ca_progress
            WHERE rsn = %s
            """,
            (rsn,),
        )
        row = cursor.fetchone()
        if not row:
            return {"completed_count": 0, "incomplete_count": 0}
        return {
            "completed_count": int(row[0] or 0),
            "incomplete_count": int(row[1] or 0),
        }

    def get_bot_panel(self, conn: MySQLConnection, panel_key: str) -> dict | None:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT panel_key, guild_id, channel_id, message_id, created_at, updated_at
            FROM bot_panels
            WHERE panel_key = %s
            """,
            (panel_key,),
        )
        return cursor.fetchone()

    def upsert_bot_panel(
        self,
        conn: MySQLConnection,
        panel_key: str,
        guild_id: str | None,
        channel_id: str,
        message_id: str,
    ) -> None:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO bot_panels (panel_key, guild_id, channel_id, message_id)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                guild_id = VALUES(guild_id),
                channel_id = VALUES(channel_id),
                message_id = VALUES(message_id),
                updated_at = CURRENT_TIMESTAMP
            """,
            (panel_key, guild_id, channel_id, message_id),
        )

    def get_active_task(self, conn: MySQLConnection, discord_user_id: str, rsn: str) -> dict | None:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT discord_user_id, rsn, task_id, assigned_scan_run_id, created_at, updated_at
            FROM user_active_tasks
            WHERE discord_user_id = %s
              AND rsn = %s
            """,
            (discord_user_id, rsn),
        )
        return cursor.fetchone()

    def get_active_tasks(self, conn: MySQLConnection, discord_user_id: str) -> list[dict]:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT discord_user_id, rsn, task_id, assigned_scan_run_id, created_at, updated_at
            FROM user_active_tasks
            WHERE discord_user_id = %s
            ORDER BY updated_at DESC
            """,
            (discord_user_id,),
        )
        return list(cursor.fetchall())

    def upsert_active_task(
        self,
        conn: MySQLConnection,
        discord_user_id: str,
        rsn: str,
        task_id: int,
        assigned_scan_run_id: int | None,
    ) -> None:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO user_active_tasks (discord_user_id, rsn, task_id, assigned_scan_run_id)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                rsn = VALUES(rsn),
                task_id = VALUES(task_id),
                assigned_scan_run_id = VALUES(assigned_scan_run_id),
                updated_at = CURRENT_TIMESTAMP
            """,
            (discord_user_id, rsn, task_id, assigned_scan_run_id),
        )

    def clear_active_task(self, conn: MySQLConnection, discord_user_id: str, rsn: str) -> None:
        cursor = conn.cursor()
        cursor.execute(
            """
            DELETE FROM user_active_tasks
            WHERE discord_user_id = %s
              AND rsn = %s
            """,
            (discord_user_id, rsn),
        )


def completed_scan_status(success_users: int, failed_users: int) -> str:
    if success_users == 0 and failed_users > 0:
        return "failed"
    return "completed"


def _split_sql_statements(sql_text: str) -> list[str]:
    # Migration files in this project are simple CREATE/ALTER/INSERT statements.
    # Splitting on semicolon is sufficient and keeps compatibility with cext cursor.
    statements = [part.strip() for part in sql_text.split(";")]
    return [stmt for stmt in statements if stmt]
