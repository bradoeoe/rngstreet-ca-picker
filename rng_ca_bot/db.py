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

SCHEMA_MIGRATIONS_TABLE = "ca_schema_migrations"
LEGACY_SCHEMA_MIGRATIONS_TABLE = "schema_migrations"
SCAN_RUNS_TABLE = "ca_scan_runs"
PLAYER_SNAPSHOTS_TABLE = "ca_player_snapshots"
TASK_CATALOG_TABLE = "ca_task_catalog"
PROGRESS_TABLE = "ca_progress"
TASK_CLAIMS_TABLE = "ca_task_claims"
BOT_PANELS_TABLE = "ca_bot_panels"
ACTIVE_TASKS_TABLE = "ca_user_active_tasks"
USER_TASK_PROFILES_TABLE = "ca_user_task_profiles"
BOSS_COMPLETION_REWARDS_TABLE = "ca_boss_completion_reroll_rewards"
RSN_TIER_REWARDS_TABLE = "ca_rsn_tier_rewards"

PREFIX_RENAMES: tuple[tuple[str, str], ...] = (
    ("scan_runs", SCAN_RUNS_TABLE),
    ("player_snapshots", PLAYER_SNAPSHOTS_TABLE),
    ("task_claims", TASK_CLAIMS_TABLE),
    ("bot_panels", BOT_PANELS_TABLE),
    ("user_active_tasks", ACTIVE_TASKS_TABLE),
    ("user_task_profiles", USER_TASK_PROFILES_TABLE),
    ("boss_completion_reroll_rewards", BOSS_COMPLETION_REWARDS_TABLE),
)


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
                self._ensure_schema_migrations_table(cursor)
                cursor.execute(f"SELECT version FROM {SCHEMA_MIGRATIONS_TABLE}")
                applied = {row[0] for row in cursor.fetchall()}

                sql_files = sorted(migrations_dir.glob("*.sql"))
                for path in sql_files:
                    version = path.stem
                    if version in applied:
                        continue

                    LOGGER.info("Applying migration %s", version)
                    if version == "008_prefix_bot_tables":
                        self._apply_prefix_table_migration(cursor)
                    else:
                        sql_text = path.read_text(encoding="utf-8")
                        for statement in _split_sql_statements(sql_text):
                            cursor.execute(statement)

                    cursor.execute(
                        f"INSERT INTO {SCHEMA_MIGRATIONS_TABLE} (version) VALUES (%s)",
                        (version,),
                    )

                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _ensure_schema_migrations_table(self, cursor) -> None:
        if self._table_exists(cursor, SCHEMA_MIGRATIONS_TABLE):
            return

        if self._table_exists(cursor, LEGACY_SCHEMA_MIGRATIONS_TABLE):
            cursor.execute(
                f"RENAME TABLE {LEGACY_SCHEMA_MIGRATIONS_TABLE} TO {SCHEMA_MIGRATIONS_TABLE}"
            )
            return

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_MIGRATIONS_TABLE} (
                version VARCHAR(64) NOT NULL PRIMARY KEY,
                applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

    def _apply_prefix_table_migration(self, cursor) -> None:
        for old_name, new_name in PREFIX_RENAMES:
            old_exists = self._table_exists(cursor, old_name)
            new_exists = self._table_exists(cursor, new_name)
            if old_exists and not new_exists:
                LOGGER.info("Renaming table %s to %s", old_name, new_name)
                cursor.execute(f"RENAME TABLE {old_name} TO {new_name}")
            elif old_exists and new_exists:
                LOGGER.warning(
                    "Legacy table %s and new table %s both exist; leaving both in place",
                    old_name,
                    new_name,
                )

    def _table_exists(self, cursor, table_name: str) -> bool:
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name = %s
            LIMIT 1
            """,
            (table_name,),
        )
        return cursor.fetchone() is not None

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

    def get_primary_discord_user_id_for_rsn(self, conn: MySQLConnection, rsn: str) -> str | None:
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT DISCORD_ID
                FROM members
                WHERE RSN = %s
                  AND TRIM(COALESCE(DISCORD_ID, '')) <> ''
                ORDER BY CASE WHEN COALESCE(MAIN_WOM_ID, 0) = 0 THEN 0 ELSE 1 END, WOM_ID
                LIMIT 1
                """,
                (rsn,),
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                discord_user_id = str(row[0]).strip()
                if discord_user_id:
                    return discord_user_id
        except mysql.connector.Error:
            LOGGER.warning("Could not resolve Discord ID via members table for rsn %s", rsn)

        try:
            cursor.execute(
                """
                SELECT map.DISCORD_ID
                FROM members m
                JOIN main_rsn_map map
                  ON map.WOM_ID = m.WOM_ID
                WHERE m.RSN = %s
                  AND TRIM(COALESCE(map.DISCORD_ID, '')) <> ''
                ORDER BY m.WOM_ID
                LIMIT 1
                """,
                (rsn,),
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                discord_user_id = str(row[0]).strip()
                if discord_user_id:
                    return discord_user_id
        except mysql.connector.Error:
            LOGGER.warning("Could not resolve Discord ID via main_rsn_map for rsn %s", rsn)

        return None

    def create_scan_run(self, conn: MySQLConnection, trigger_source: str, total_users: int) -> int:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO {SCAN_RUNS_TABLE} (trigger_source, status, total_users)
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
            f"""
            UPDATE {SCAN_RUNS_TABLE}
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
            f"""
            INSERT INTO {PLAYER_SNAPSHOTS_TABLE} (scan_run_id, rsn, source_timestamp, payload_json)
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
            f"""
            INSERT INTO {TASK_CATALOG_TABLE} (
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
        cursor.execute(f"SELECT task_id FROM {TASK_CATALOG_TABLE} ORDER BY task_id")
        return [int(row[0]) for row in cursor.fetchall()]

    def get_task_metadata(self, conn: MySQLConnection, task_id: int) -> dict | None:
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
            FROM {TASK_CATALOG_TABLE}
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
            FROM {TASK_CATALOG_TABLE}
            WHERE task_id IN ({placeholders})
            """,
            tuple(int(task_id) for task_id in task_ids),
        )
        rows = cursor.fetchall()
        return {int(row["task_id"]): row for row in rows}

    def get_task_completion_state(self, conn: MySQLConnection, rsn: str, task_id: int) -> bool | None:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT is_complete
            FROM {PROGRESS_TABLE}
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
            f"""
            INSERT INTO {PROGRESS_TABLE} (rsn, task_id, is_complete, source, source_scan_run_id)
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
            f"""
            INSERT INTO {PROGRESS_TABLE} (rsn, task_id, is_complete, source, source_scan_run_id)
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
        cursor.execute(f"SELECT MAX(id) FROM {SCAN_RUNS_TABLE} WHERE status = 'completed'")
        row = cursor.fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0])

    def get_incomplete_task_ids(self, conn: MySQLConnection, rsn: str) -> set[int]:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT task_id FROM {PROGRESS_TABLE} WHERE rsn = %s AND is_complete = 0",
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
            f"""
            SELECT task_id
            FROM {TASK_CLAIMS_TABLE}
            WHERE discord_user_id = %s
              AND rsn = %s
              AND claim_scan_run_id = %s
              AND status IN ('claimed_complete', 'verified_complete')
            """,
            (discord_user_id, rsn, scan_run_id),
        )
        return {int(row[0]) for row in cursor.fetchall()}

    def ensure_user_task_profile(self, conn: MySQLConnection, discord_user_id: str) -> None:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT IGNORE INTO {USER_TASK_PROFILES_TABLE} (discord_user_id)
            VALUES (%s)
            """,
            (discord_user_id,),
        )

    def get_user_task_profile(
        self,
        conn: MySQLConnection,
        discord_user_id: str,
        *,
        for_update: bool = False,
    ) -> dict:
        self.ensure_user_task_profile(conn, discord_user_id)
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT discord_user_id, rerolls_available, completion_reward_batches, created_at, updated_at
            FROM {USER_TASK_PROFILES_TABLE}
            WHERE discord_user_id = %s
        """
        query = query.format(USER_TASK_PROFILES_TABLE=USER_TASK_PROFILES_TABLE)
        if for_update:
            query += " FOR UPDATE"
        cursor.execute(query, (discord_user_id,))
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError(f"Could not load task profile for Discord user {discord_user_id}")
        return row

    def update_user_task_profile(
        self,
        conn: MySQLConnection,
        discord_user_id: str,
        *,
        rerolls_available: int | None = None,
        completion_reward_batches: int | None = None,
    ) -> None:
        assignments: list[str] = []
        values: list[int | str] = []

        if rerolls_available is not None:
            assignments.append("rerolls_available = %s")
            values.append(int(rerolls_available))
        if completion_reward_batches is not None:
            assignments.append("completion_reward_batches = %s")
            values.append(int(completion_reward_batches))

        if not assignments:
            return

        assignments.append("updated_at = CURRENT_TIMESTAMP")
        values.append(discord_user_id)

        cursor = conn.cursor()
        cursor.execute(
            f"""
            UPDATE {USER_TASK_PROFILES_TABLE}
            SET {", ".join(assignments)}
            WHERE discord_user_id = %s
            """,
            tuple(values),
        )

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
            f"""
            INSERT INTO {TASK_CLAIMS_TABLE} (
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
    ) -> tuple[str | None, str]:
        new_status = "verified_complete" if is_complete else "corrected_incomplete"
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT status
            FROM {TASK_CLAIMS_TABLE}
            WHERE discord_user_id = %s
              AND rsn = %s
              AND task_id = %s
            """,
            (discord_user_id, rsn, task_id),
        )
        row = cursor.fetchone()
        previous_status = str(row[0]) if row and row[0] is not None else None

        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO {TASK_CLAIMS_TABLE} (
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
        return previous_status, new_status

    def reconcile_claims_with_scan(
        self,
        conn: MySQLConnection,
        rsn: str,
        completed_task_ids: set[int],
        scan_run_id: int,
    ) -> tuple[int, int, list[tuple[str, int]]]:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT id, discord_user_id, task_id, status
            FROM {TASK_CLAIMS_TABLE}
            WHERE rsn = %s
            """,
            (rsn,),
        )

        verified = 0
        corrected = 0
        newly_verified_claims: list[tuple[str, int]] = []
        for claim_id, discord_user_id, task_id, current_status in cursor.fetchall():
            task_id = int(task_id)
            is_complete = task_id in completed_task_ids
            next_status = "verified_complete" if is_complete else "corrected_incomplete"
            if is_complete:
                verified += 1
            else:
                corrected += 1

            cursor.execute(
                f"""
                UPDATE {TASK_CLAIMS_TABLE}
                SET status = %s,
                    verified_scan_run_id = %s,
                    last_verified_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (next_status, scan_run_id, claim_id),
            )

            if next_status == "verified_complete" and current_status != "verified_complete":
                newly_verified_claims.append((str(discord_user_id), task_id))

        return verified, corrected, newly_verified_claims

    def count_verified_task_claims(self, conn: MySQLConnection, discord_user_id: str) -> int:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {TASK_CLAIMS_TABLE}
            WHERE discord_user_id = %s
              AND status = 'verified_complete'
            """,
            (discord_user_id,),
        )
        row = cursor.fetchone()
        return int(row[0] or 0) if row else 0

    def get_task_claim_counts_for_user(self, conn: MySQLConnection, discord_user_id: str) -> dict[str, int]:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT status, COUNT(*)
            FROM {TASK_CLAIMS_TABLE}
            WHERE discord_user_id = %s
            GROUP BY status
            """,
            (discord_user_id,),
        )
        counts = {
            "verified_complete": 0,
            "claimed_complete": 0,
            "corrected_incomplete": 0,
        }
        for status, count in cursor.fetchall():
            key = str(status or "").strip()
            counts[key] = int(count or 0)
        return counts

    def get_verified_task_claim_counts_by_rsn(self, conn: MySQLConnection, discord_user_id: str) -> dict[str, int]:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT rsn, COUNT(*)
            FROM {TASK_CLAIMS_TABLE}
            WHERE discord_user_id = %s
              AND status = 'verified_complete'
            GROUP BY rsn
            ORDER BY rsn
            """,
            (discord_user_id,),
        )
        counts: dict[str, int] = {}
        for rsn, count in cursor.fetchall():
            rsn_value = str(rsn or "").strip()
            if not rsn_value:
                continue
            counts[rsn_value] = int(count or 0)
        return counts

    def record_boss_completion_reroll_reward(
        self,
        conn: MySQLConnection,
        discord_user_id: str,
        rsn: str,
        npc: str,
        rewarded_task_id: int,
    ) -> bool:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT IGNORE INTO {BOSS_COMPLETION_REWARDS_TABLE} (
                discord_user_id,
                rsn,
                npc,
                rewarded_task_id
            )
            VALUES (%s, %s, %s, %s)
            """,
            (discord_user_id, rsn, npc, rewarded_task_id),
        )
        return cursor.rowcount > 0

    def ensure_rsn_tier_reward_state(self, conn: MySQLConnection, rsn: str) -> None:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT IGNORE INTO {RSN_TIER_REWARDS_TABLE} (rsn)
            VALUES (%s)
            """,
            (rsn,),
        )

    def get_rsn_tier_reward_state(
        self,
        conn: MySQLConnection,
        rsn: str,
        *,
        for_update: bool = False,
    ) -> dict:
        self.ensure_rsn_tier_reward_state(conn, rsn)
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT rsn, highest_rewarded_tier_rank, highest_rewarded_tier_label, updated_at
            FROM {RSN_TIER_REWARDS_TABLE}
            WHERE rsn = %s
        """
        query = query.format(RSN_TIER_REWARDS_TABLE=RSN_TIER_REWARDS_TABLE)
        if for_update:
            query += " FOR UPDATE"
        cursor.execute(query, (rsn,))
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError(f"Could not load rsn tier reward state for {rsn}")
        return row

    def update_rsn_tier_reward_state(
        self,
        conn: MySQLConnection,
        rsn: str,
        *,
        highest_rewarded_tier_rank: int,
        highest_rewarded_tier_label: str | None,
    ) -> None:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            UPDATE {RSN_TIER_REWARDS_TABLE}
            SET highest_rewarded_tier_rank = %s,
                highest_rewarded_tier_label = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE rsn = %s
            """,
            (int(highest_rewarded_tier_rank), highest_rewarded_tier_label, rsn),
        )

    def get_catalog_point_totals_by_tier(self, conn: MySQLConnection) -> dict[str, int]:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT tier_label, SUM(COALESCE(points, 0)) AS tier_points
            FROM {TASK_CATALOG_TABLE}
            WHERE TRIM(COALESCE(tier_label, '')) <> ''
            GROUP BY tier_label
            """,
        )
        totals: dict[str, int] = {}
        for tier_label, tier_points in cursor.fetchall():
            label = str(tier_label or "").strip()
            if not label:
                continue
            totals[label] = int(tier_points or 0)
        return totals

    def ensure_task_stub(self, conn: MySQLConnection, task_id: int, source_url: str) -> None:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT IGNORE INTO {TASK_CATALOG_TABLE} (task_id, task_name, source_url)
            VALUES (%s, %s, %s)
            """,
            (task_id, f"Task {task_id}", source_url),
        )

    def get_latest_snapshot(self, conn: MySQLConnection, rsn: str) -> dict | None:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"""
            SELECT id, scan_run_id, rsn, source_timestamp, fetched_at, payload_json
            FROM {PLAYER_SNAPSHOTS_TABLE}
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
            f"""
            SELECT
                SUM(CASE WHEN progress.is_complete = 1 THEN 1 ELSE 0 END) AS completed_count,
                SUM(CASE WHEN progress.is_complete = 0 THEN 1 ELSE 0 END) AS incomplete_count,
                SUM(CASE WHEN progress.is_complete = 1 THEN COALESCE(catalog.points, 0) ELSE 0 END) AS total_points
            FROM {PROGRESS_TABLE} AS progress
            LEFT JOIN {TASK_CATALOG_TABLE} AS catalog
              ON catalog.task_id = progress.task_id
            WHERE rsn = %s
            """,
            (rsn,),
        )
        row = cursor.fetchone()
        if not row:
            return {"completed_count": 0, "incomplete_count": 0, "total_points": 0}
        return {
            "completed_count": int(row[0] or 0),
            "incomplete_count": int(row[1] or 0),
            "total_points": int(row[2] or 0),
        }

    def get_bot_panel(self, conn: MySQLConnection, panel_key: str) -> dict | None:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"""
            SELECT panel_key, guild_id, channel_id, message_id, created_at, updated_at
            FROM {BOT_PANELS_TABLE}
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
            f"""
            INSERT INTO {BOT_PANELS_TABLE} (panel_key, guild_id, channel_id, message_id)
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
            f"""
            SELECT discord_user_id, rsn, task_id, assigned_scan_run_id, created_at, updated_at
            FROM {ACTIVE_TASKS_TABLE}
            WHERE discord_user_id = %s
              AND rsn = %s
            """,
            (discord_user_id, rsn),
        )
        return cursor.fetchone()

    def get_active_tasks(self, conn: MySQLConnection, discord_user_id: str) -> list[dict]:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"""
            SELECT discord_user_id, rsn, task_id, assigned_scan_run_id, created_at, updated_at
            FROM {ACTIVE_TASKS_TABLE}
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
            f"""
            INSERT INTO {ACTIVE_TASKS_TABLE} (discord_user_id, rsn, task_id, assigned_scan_run_id)
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
            f"""
            DELETE FROM {ACTIVE_TASKS_TABLE}
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
