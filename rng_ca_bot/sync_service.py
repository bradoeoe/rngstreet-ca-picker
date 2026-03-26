from __future__ import annotations

import json
import logging
import random
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from html import unescape
from typing import Iterable
from urllib.parse import quote

import requests

from .config import Settings
from .db import Database, ScanRunResult, TaskCatalogEntry, completed_scan_status

LOGGER = logging.getLogger(__name__)

RUNELITE_SYNC_URL = "https://sync.runescape.wiki/runelite/player/{rsn}/{account_type}"
CA_TASKS_URL = "https://oldschool.runescape.wiki/w/Combat_Achievements/All_tasks"

_ROW_PATTERN = re.compile(r'<tr[^>]*data-ca-task-id="(\d+)"[^>]*>(.*?)</tr>', re.DOTALL | re.IGNORECASE)
_TD_PATTERN = re.compile(r"<td[^>]*>(.*?)</td>", re.DOTALL | re.IGNORECASE)
_TAG_PATTERN = re.compile(r"<[^>]+>")
_HREF_PATTERN = re.compile(r'<a[^>]*href="([^"]+)"', re.IGNORECASE)
_POINTS_PATTERN = re.compile(r"\((\d+)\s*pt")


@dataclass(slots=True)
class RandomTaskResult:
    task_id: int
    task_name: str
    task_description: str | None
    npc: str | None
    npc_url: str | None
    npc_image_url: str | None
    task_type: str | None
    tier_label: str | None
    points: int | None
    eligible_count: int


@dataclass(slots=True)
class ActiveTaskAssignment:
    rsn: str
    task: RandomTaskResult
    reused_existing: bool


@dataclass(slots=True)
class TooHardResult:
    rsn: str
    previous_task: RandomTaskResult
    replacement_task: RandomTaskResult | None


class RuneLiteSyncError(Exception):
    def __init__(self, rsn: str, status_code: int, code: str | None, message: str) -> None:
        self.rsn = rsn
        self.status_code = status_code
        self.code = code
        self.message = message
        super().__init__(f"{status_code} {code or 'UNKNOWN'}: {message}")


class NoUserDataError(RuneLiteSyncError):
    pass


class UnsupportedWorldTypeError(RuneLiteSyncError):
    pass


def _clean_html_text(value: str) -> str:
    text = _TAG_PATTERN.sub(" ", value)
    text = unescape(text)
    return " ".join(text.split())


def _absolute_wiki_url(href: str | None) -> str | None:
    if not href:
        return None
    cleaned = href.strip()
    if not cleaned:
        return None
    if cleaned.startswith("http://") or cleaned.startswith("https://"):
        return cleaned
    if cleaned.startswith("/"):
        return f"https://oldschool.runescape.wiki{cleaned}"
    return f"https://oldschool.runescape.wiki/{cleaned.lstrip('/')}"


def _extract_first_href(cell_html: str) -> str | None:
    match = _HREF_PATTERN.search(cell_html)
    if not match:
        return None
    href = unescape(match.group(1))
    return _absolute_wiki_url(href)


def _infer_npc_image_url(npc_url: str | None, npc_name: str | None) -> str | None:
    slug: str | None = None
    if npc_url and "/w/" in npc_url:
        slug = npc_url.split("/w/", 1)[1]
        slug = slug.split("#", 1)[0].split("?", 1)[0].strip("/")
    if not slug and npc_name:
        slug = npc_name.strip().replace(" ", "_")
    if not slug:
        return None
    return f"https://oldschool.runescape.wiki/w/Special:FilePath/{slug}.png"


def parse_ca_task_catalog(html: str) -> list[TaskCatalogEntry]:
    entries: list[TaskCatalogEntry] = []

    for task_id_raw, row_html in _ROW_PATTERN.findall(html):
        cells = _TD_PATTERN.findall(row_html)
        if len(cells) < 5:
            continue

        npc = _clean_html_text(cells[0]) or None
        npc_url = _extract_first_href(cells[0])
        npc_image_url = _infer_npc_image_url(npc_url, npc)
        task_name = _clean_html_text(cells[1]) or f"Task {task_id_raw}"
        task_description = _clean_html_text(cells[2]) or None
        task_type = _clean_html_text(cells[3]) or None
        tier_text = _clean_html_text(cells[4])
        points_match = _POINTS_PATTERN.search(tier_text)
        points = int(points_match.group(1)) if points_match else None

        tier_label = tier_text
        if "(" in tier_text:
            tier_label = tier_text.split("(", 1)[0].strip()
        tier_label = tier_label or None

        entries.append(
            TaskCatalogEntry(
                task_id=int(task_id_raw),
                task_name=task_name,
                description=task_description,
                npc=npc,
                npc_url=npc_url,
                npc_image_url=npc_image_url,
                task_type=task_type,
                tier_label=tier_label,
                points=points,
                source_url=CA_TASKS_URL,
            )
        )

    # De-duplicate by task_id in case wiki HTML contains repeated rows.
    deduped: dict[int, TaskCatalogEntry] = {}
    for entry in entries:
        deduped[entry.task_id] = entry
    return list(sorted(deduped.values(), key=lambda e: e.task_id))


def parse_source_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None

    cleaned = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(tzinfo=None)


def extract_completed_ca_task_ids(payload: dict) -> set[int]:
    raw_ids = payload.get("combat_achievements", [])
    if not isinstance(raw_ids, list):
        return set()
    completed: set[int] = set()
    for value in raw_ids:
        try:
            completed.add(int(value))
        except (TypeError, ValueError):
            continue
    return completed


def compute_eligible_task_ids(incomplete_ids: Iterable[int], claimed_ids_for_latest_scan: Iterable[int]) -> list[int]:
    incomplete = {int(x) for x in incomplete_ids}
    claimed = {int(x) for x in claimed_ids_for_latest_scan}
    eligible = sorted(incomplete - claimed)
    return eligible


class SyncService:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db
        self._rand = random.SystemRandom()

    def fetch_runelite_payload(self, rsn: str) -> dict:
        encoded_rsn = quote(rsn, safe="")
        url = RUNELITE_SYNC_URL.format(rsn=encoded_rsn, account_type=self.settings.runelite_account_type)
        response = requests.get(
            url,
            timeout=30,
            headers={
                "User-Agent": self.settings.http_user_agent,
                "Accept": "application/json",
            },
        )
        if response.status_code >= 400:
            code: str | None = None
            message = response.text.strip()
            try:
                err_json = response.json()
                if isinstance(err_json, dict):
                    code = str(err_json.get("code") or "").strip() or None
                    msg = str(err_json.get("error") or "").strip()
                    if msg:
                        message = msg
            except Exception:
                pass

            if response.status_code == 400 and code == "NO_USER_DATA":
                raise NoUserDataError(rsn, response.status_code, code, message)
            if response.status_code == 400 and "world type" in message.lower():
                raise UnsupportedWorldTypeError(rsn, response.status_code, code, message)
            raise RuneLiteSyncError(rsn, response.status_code, code, message)

        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected payload type for {rsn}: {type(payload)!r}")
        return payload

    def fetch_ca_catalog_entries(self) -> list[TaskCatalogEntry]:
        response = requests.get(
            CA_TASKS_URL,
            timeout=30,
            headers={"User-Agent": self.settings.http_user_agent},
        )
        response.raise_for_status()
        entries = parse_ca_task_catalog(response.text)
        if not entries:
            raise ValueError("No combat achievement tasks parsed from wiki page")
        return entries

    def refresh_catalog(self) -> int:
        entries = self.fetch_ca_catalog_entries()
        with self.db.connection() as conn:
            try:
                self.db.upsert_task_catalog(conn, entries)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return len(entries)

    def _ensure_catalog_has_scan_ids(
        self,
        conn,
        completed_ids: set[int],
        catalog_ids: list[int],
    ) -> list[int]:
        known = set(catalog_ids)
        missing = sorted(completed_ids - known)
        for task_id in missing:
            self.db.ensure_task_stub(conn, task_id=task_id, source_url=CA_TASKS_URL)
        if missing:
            catalog_ids = sorted(known | set(missing))
        return catalog_ids

    def run_daily_scan(self, trigger_source: str = "scheduled") -> ScanRunResult:
        # Keep catalog fresh so all IDs are represented before progress reconciliation.
        try:
            updated = self.refresh_catalog()
            LOGGER.info("Catalog refreshed: %s tasks", updated)
        except Exception:
            LOGGER.exception("Catalog refresh failed; continuing with existing catalog data")

        with self.db.connection() as conn:
            rsns = self.db.fetch_rsns(conn)
            try:
                run_id = self.db.create_scan_run(conn, trigger_source=trigger_source, total_users=len(rsns))
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            success_users = 0
            failed_users = 0
            errors: list[str] = []
            no_user_data_users = 0
            wrong_world_type_users = 0

            for rsn in rsns:
                try:
                    payload = self.fetch_runelite_payload(rsn)
                    completed_ids = extract_completed_ca_task_ids(payload)
                    source_timestamp = parse_source_timestamp(payload.get("timestamp"))
                    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

                    try:
                        self.db.insert_player_snapshot(
                            conn,
                            run_id=run_id,
                            rsn=rsn,
                            source_timestamp=source_timestamp,
                            payload_json=payload_json,
                        )

                        catalog_ids = self.db.get_catalog_task_ids(conn)
                        catalog_ids = self._ensure_catalog_has_scan_ids(conn, completed_ids, catalog_ids)
                        universe_ids = catalog_ids if catalog_ids else sorted(completed_ids)

                        self.db.upsert_progress_rows(
                            conn,
                            rsn=rsn,
                            completed_task_ids=completed_ids,
                            universe_task_ids=universe_ids,
                            source="scan",
                            source_scan_run_id=run_id,
                        )

                        self.db.reconcile_claims_with_scan(
                            conn,
                            rsn=rsn,
                            completed_task_ids=completed_ids,
                            scan_run_id=run_id,
                        )
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise

                    success_users += 1
                except NoUserDataError as exc:
                    failed_users += 1
                    no_user_data_users += 1
                    err = f"{rsn}: {exc.code or 'NO_USER_DATA'} - {exc.message}"
                    errors.append(err)
                    LOGGER.warning("Player scan skipped (no RuneLite sync data) for %s", rsn)
                except UnsupportedWorldTypeError as exc:
                    failed_users += 1
                    wrong_world_type_users += 1
                    err = f"{rsn}: {exc.code or 'WRONG_WORLD_TYPE'} - {exc.message}"
                    errors.append(err)
                    LOGGER.warning(
                        "Player scan skipped (wrong world type) for %s using account type %s",
                        rsn,
                        self.settings.runelite_account_type,
                    )
                except RuneLiteSyncError as exc:
                    failed_users += 1
                    err = f"{rsn}: {exc.code or 'API_ERROR'} - {exc.message}"
                    errors.append(err)
                    LOGGER.warning("Player scan API error for %s: %s", rsn, exc)
                except Exception as exc:
                    failed_users += 1
                    err = f"{rsn}: {exc}"
                    errors.append(err)
                    LOGGER.exception("Player scan failed for %s", rsn)

            status = completed_scan_status(success_users=success_users, failed_users=failed_users)
            error_text = "\n".join(errors[:25]) if errors else None
            LOGGER.info(
                "Scan rollup | total=%s success=%s failed=%s no_user_data=%s wrong_world_type=%s",
                len(rsns),
                success_users,
                failed_users,
                no_user_data_users,
                wrong_world_type_users,
            )

            try:
                self.db.finish_scan_run(
                    conn,
                    run_id=run_id,
                    status=status,
                    success_users=success_users,
                    failed_users=failed_users,
                    error_text=error_text,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            return ScanRunResult(
                run_id=run_id,
                status=status,
                total_users=len(rsns),
                success_users=success_users,
                failed_users=failed_users,
            )

    def pick_random_task(self, discord_user_id: str, rsn: str) -> RandomTaskResult | None:
        with self.db.connection() as conn:
            latest_scan_id = self.db.get_latest_completed_scan_run_id(conn)
            incomplete_ids = self.db.get_incomplete_task_ids(conn, rsn)
            claimed_ids = self.db.get_claimed_task_ids_for_scan(
                conn,
                discord_user_id=discord_user_id,
                rsn=rsn,
                scan_run_id=latest_scan_id,
            )

            eligible_ids = compute_eligible_task_ids(incomplete_ids, claimed_ids)
            if not eligible_ids:
                return None

            task_id = self._rand.choice(eligible_ids)
            metadata = self.db.get_task_metadata(conn, task_id)

            if metadata is None:
                task_name = f"Task {task_id}"
                task_description = None
                npc = None
                npc_url = None
                npc_image_url = None
                task_type = None
                tier_label = None
                points = None
            else:
                task_name = metadata.get("task_name") or f"Task {task_id}"
                task_description = metadata.get("description")
                npc = metadata.get("npc")
                npc_url = metadata.get("npc_url")
                npc_image_url = metadata.get("npc_image_url")
                task_type = metadata.get("task_type")
                tier_label = metadata.get("tier_label")
                points = metadata.get("points")

            return RandomTaskResult(
                task_id=task_id,
                task_name=task_name,
                task_description=task_description,
                npc=npc,
                npc_url=npc_url,
                npc_image_url=npc_image_url,
                task_type=task_type,
                tier_label=tier_label,
                points=points,
                eligible_count=len(eligible_ids),
            )

    def _build_task_result(self, conn, task_id: int, eligible_count: int) -> RandomTaskResult:
        metadata = self.db.get_task_metadata(conn, task_id)
        if metadata is None:
            task_name = f"Task {task_id}"
            task_description = None
            npc = None
            npc_url = None
            npc_image_url = None
            task_type = None
            tier_label = None
            points = None
        else:
            task_name = metadata.get("task_name") or f"Task {task_id}"
            task_description = metadata.get("description")
            npc = metadata.get("npc")
            npc_url = metadata.get("npc_url")
            npc_image_url = metadata.get("npc_image_url")
            task_type = metadata.get("task_type")
            tier_label = metadata.get("tier_label")
            points = metadata.get("points")

        return RandomTaskResult(
            task_id=task_id,
            task_name=task_name,
            task_description=task_description,
            npc=npc,
            npc_url=npc_url,
            npc_image_url=npc_image_url,
            task_type=task_type,
            tier_label=tier_label,
            points=points,
            eligible_count=eligible_count,
        )

    def get_or_assign_active_task(self, discord_user_id: str, rsn_override: str | None = None) -> ActiveTaskAssignment | None:
        with self.db.connection() as conn:
            rsn = (rsn_override or "").strip()
            if not rsn:
                rsn = self.db.get_rsn_for_discord_user(conn, discord_user_id) or ""
            if not rsn:
                return None

            latest_scan_id = self.db.get_latest_completed_scan_run_id(conn)
            active = self.db.get_active_task(conn, discord_user_id, rsn)
            if active is not None:
                active_rsn = str(active["rsn"]).strip()
                active_task_id = int(active["task_id"])
                completion_state = self.db.get_task_completion_state(conn, active_rsn, active_task_id)
                claimed_ids = self.db.get_claimed_task_ids_for_scan(
                    conn,
                    discord_user_id=discord_user_id,
                    rsn=active_rsn,
                    scan_run_id=latest_scan_id,
                )
                eligible_ids = compute_eligible_task_ids(
                    self.db.get_incomplete_task_ids(conn, active_rsn),
                    claimed_ids,
                )
                if (
                    active_rsn.casefold() == rsn.casefold()
                    and completion_state is False
                    and active_task_id in set(eligible_ids)
                ):
                    return ActiveTaskAssignment(
                        rsn=active_rsn,
                        task=self._build_task_result(conn, active_task_id, len(eligible_ids)),
                        reused_existing=True,
                    )
                self.db.clear_active_task(conn, discord_user_id, rsn)
                conn.commit()

            incomplete_ids = self.db.get_incomplete_task_ids(conn, rsn)
            claimed_ids = self.db.get_claimed_task_ids_for_scan(
                conn,
                discord_user_id=discord_user_id,
                rsn=rsn,
                scan_run_id=latest_scan_id,
            )
            eligible_ids = compute_eligible_task_ids(incomplete_ids, claimed_ids)
            if not eligible_ids:
                return None

            task_id = self._rand.choice(eligible_ids)
            self.db.upsert_active_task(
                conn,
                discord_user_id=discord_user_id,
                rsn=rsn,
                task_id=task_id,
                assigned_scan_run_id=latest_scan_id,
            )
            conn.commit()
            return ActiveTaskAssignment(
                rsn=rsn,
                task=self._build_task_result(conn, task_id, len(eligible_ids)),
                reused_existing=False,
            )

    def resolve_rsn_for_discord_user(self, discord_user_id: str) -> str | None:
        with self.db.connection() as conn:
            return self.db.get_rsn_for_discord_user(conn, discord_user_id)

    def resolve_rsns_for_discord_user(self, discord_user_id: str) -> list[str]:
        with self.db.connection() as conn:
            return self.db.get_rsns_for_discord_user(conn, discord_user_id)

    def mark_claim_complete(
        self,
        discord_user_id: str,
        rsn: str,
        task_id: int,
        guild_id: str | None,
        channel_id: str | None,
        message_id: str | None,
    ) -> None:
        with self.db.connection() as conn:
            try:
                latest_scan_id = self.db.get_latest_completed_scan_run_id(conn)
                self.db.upsert_single_progress(
                    conn,
                    rsn=rsn,
                    task_id=task_id,
                    is_complete=True,
                    source="claim",
                    source_scan_run_id=latest_scan_id,
                )
                self.db.mark_task_claimed_complete(
                    conn,
                    discord_user_id=discord_user_id,
                    rsn=rsn,
                    task_id=task_id,
                    claim_scan_run_id=latest_scan_id,
                    guild_id=guild_id,
                    channel_id=channel_id,
                    message_id=message_id,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def complete_active_task(
        self,
        discord_user_id: str,
        rsn: str,
        guild_id: str | None,
        channel_id: str | None,
        message_id: str | None,
    ) -> tuple[str, RandomTaskResult] | None:
        with self.db.connection() as conn:
            active = self.db.get_active_task(conn, discord_user_id, rsn)
            if active is None:
                return None

            active_rsn = str(active["rsn"]).strip()
            task_id = int(active["task_id"])

            try:
                latest_scan_id = self.db.get_latest_completed_scan_run_id(conn)
                self.db.upsert_single_progress(
                    conn,
                    rsn=active_rsn,
                    task_id=task_id,
                    is_complete=True,
                    source="claim",
                    source_scan_run_id=latest_scan_id,
                )
                self.db.mark_task_claimed_complete(
                    conn,
                    discord_user_id=discord_user_id,
                    rsn=active_rsn,
                    task_id=task_id,
                    claim_scan_run_id=latest_scan_id,
                    guild_id=guild_id,
                    channel_id=channel_id,
                    message_id=message_id,
                )
                self.db.clear_active_task(conn, discord_user_id, active_rsn)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            return active_rsn, self._build_task_result(conn, task_id, eligible_count=0)

    def reroll_active_task_too_hard(self, discord_user_id: str, rsn: str) -> TooHardResult | None:
        with self.db.connection() as conn:
            active = self.db.get_active_task(conn, discord_user_id, rsn)
            if active is None:
                return None

            task_id = int(active["task_id"])
            completion_state = self.db.get_task_completion_state(conn, rsn, task_id)
            if completion_state is not False:
                self.db.clear_active_task(conn, discord_user_id, rsn)
                conn.commit()
                return None

            latest_scan_id = self.db.get_latest_completed_scan_run_id(conn)
            incomplete_ids = self.db.get_incomplete_task_ids(conn, rsn)
            claimed_ids = self.db.get_claimed_task_ids_for_scan(
                conn,
                discord_user_id=discord_user_id,
                rsn=rsn,
                scan_run_id=latest_scan_id,
            )
            eligible_ids = compute_eligible_task_ids(incomplete_ids, claimed_ids)
            current_task = self._build_task_result(conn, task_id, len(eligible_ids))
            candidates = [candidate_id for candidate_id in eligible_ids if candidate_id != task_id]

            if current_task.points is not None and candidates:
                metadata_by_id = self.db.get_task_metadata_for_ids(conn, candidates)
                candidates = [
                    candidate_id
                    for candidate_id in candidates
                    if metadata_by_id.get(candidate_id) is not None
                    and metadata_by_id[candidate_id].get("points") is not None
                    and int(metadata_by_id[candidate_id]["points"]) < int(current_task.points)
                ]

            if not candidates:
                return TooHardResult(rsn=rsn, previous_task=current_task, replacement_task=None)

            next_task_id = self._rand.choice(candidates)
            self.db.upsert_active_task(
                conn,
                discord_user_id=discord_user_id,
                rsn=rsn,
                task_id=next_task_id,
                assigned_scan_run_id=latest_scan_id,
            )
            conn.commit()

            replacement = self._build_task_result(conn, next_task_id, len(candidates))
            return TooHardResult(rsn=rsn, previous_task=current_task, replacement_task=replacement)

    def verify_task_live(
        self,
        discord_user_id: str,
        rsn: str,
        task_id: int,
    ) -> tuple[bool, str]:
        payload = self.fetch_runelite_payload(rsn)
        completed_ids = extract_completed_ca_task_ids(payload)
        is_complete = task_id in completed_ids

        with self.db.connection() as conn:
            try:
                latest_scan_id = self.db.get_latest_completed_scan_run_id(conn)
                source = "verify" if is_complete else "correction"
                self.db.upsert_single_progress(
                    conn,
                    rsn=rsn,
                    task_id=task_id,
                    is_complete=is_complete,
                    source=source,
                    source_scan_run_id=latest_scan_id,
                )
                claim_status = self.db.mark_task_verified(
                    conn,
                    discord_user_id=discord_user_id,
                    rsn=rsn,
                    task_id=task_id,
                    is_complete=is_complete,
                    verified_scan_run_id=latest_scan_id,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return is_complete, claim_status

    def get_player_debug_summary(self, rsn: str) -> dict | None:
        with self.db.connection() as conn:
            snapshot = self.db.get_latest_snapshot(conn, rsn)
            if snapshot is None:
                return None
            progress = self.db.get_progress_summary(conn, rsn)

        payload = json.loads(snapshot["payload_json"])
        completed_ids = extract_completed_ca_task_ids(payload)
        return {
            "rsn": rsn,
            "scan_run_id": snapshot["scan_run_id"],
            "fetched_at": str(snapshot["fetched_at"]),
            "source_timestamp": str(snapshot["source_timestamp"]) if snapshot["source_timestamp"] else None,
            "payload_keys": sorted(payload.keys()),
            "combat_ids_count": len(completed_ids),
            "completed_count": progress["completed_count"],
            "incomplete_count": progress["incomplete_count"],
            "sample_combat_ids": sorted(list(completed_ids))[:25],
        }
