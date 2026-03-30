from __future__ import annotations

import json
import logging
import random
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from html import unescape
from typing import Iterable, Mapping
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

from .config import Settings
from .db import Database, ScanRunResult, TaskCatalogEntry, completed_scan_status
from .rewards import format_reward_display

LOGGER = logging.getLogger(__name__)

RUNELITE_SYNC_URL = "https://sync.runescape.wiki/runelite/player/{rsn}/{account_type}"
CA_TASKS_URL = "https://oldschool.runescape.wiki/w/Combat_Achievements/All_tasks"

_ROW_PATTERN = re.compile(r'<tr[^>]*data-ca-task-id="(\d+)"[^>]*>(.*?)</tr>', re.DOTALL | re.IGNORECASE)
_TD_PATTERN = re.compile(r"<td[^>]*>(.*?)</td>", re.DOTALL | re.IGNORECASE)
_TAG_PATTERN = re.compile(r"<[^>]+>")
_HREF_PATTERN = re.compile(r'<a[^>]*href="([^"]+)"', re.IGNORECASE)
_POINTS_PATTERN = re.compile(r"\((\d+)\s*pt")
_TIER_ORDER: tuple[tuple[str, str], ...] = (
    ("easy", "Easy"),
    ("medium", "Medium"),
    ("hard", "Hard"),
    ("elite", "Elite"),
    ("master", "Master"),
    ("grandmaster", "Grandmaster"),
)
_TIER_RANKS: dict[str, int] = {tier_key: rank for rank, (tier_key, _label) in enumerate(_TIER_ORDER, start=1)}
_BOSS_IMAGE_OVERRIDES: dict[str, str] = {
    "aberrant spectre": "https://oldschool.runescape.wiki/images/Aberrant_spectre.png?65d6f",
    "aberrant spectres": "https://oldschool.runescape.wiki/images/Aberrant_spectre.png?65d6f",
    "alchemical hydra": "https://oldschool.runescape.wiki/images/thumb/Alchemical_Hydra_%28serpentine%29.png/543px-Alchemical_Hydra_%28serpentine%29.png?925dd",
    "barrows": "https://oldschool.runescape.wiki/images/thumb/Dharok_the_Wretched.png/228px-Dharok_the_Wretched.png?33092",
    "black dragon": "https://oldschool.runescape.wiki/images/thumb/Black_dragon.png/580px-Black_dragon.png?b8574",
    "black dragons": "https://oldschool.runescape.wiki/images/thumb/Black_dragon.png/580px-Black_dragon.png?b8574",
    "brutal black dragon": "https://oldschool.runescape.wiki/images/thumb/Brutal_black_dragon.png/580px-Brutal_black_dragon.png?24f54",
    "brutal black dragons": "https://oldschool.runescape.wiki/images/thumb/Brutal_black_dragon.png/580px-Brutal_black_dragon.png?24f54",
    "chambers of xeric": "https://oldschool.runescape.wiki/w/Special:FilePath/Great_Olm.png",
    "chambers of xeric: challenge mode": "https://oldschool.runescape.wiki/w/Special:FilePath/Great_Olm.png",
    "crazy archaeologist": "https://oldschool.runescape.wiki/images/Crazy_archaeologist.png?3ecc9",
    "demonic gorilla": "https://oldschool.runescape.wiki/images/Demonic_gorilla.png?26536",
    "demonic gorillas": "https://oldschool.runescape.wiki/images/Demonic_gorilla.png?26536",
    "deranged archaeologist": "https://oldschool.runescape.wiki/images/Deranged_archaeologist.png?32c7e",
    "fire giant": "https://oldschool.runescape.wiki/images/Fire_giant_%285%29.png?870b3",
    "fire giants": "https://oldschool.runescape.wiki/images/Fire_giant_%285%29.png?870b3",
    "fortis colosseum": "https://oldschool.runescape.wiki/w/Special:FilePath/Sol_Heredit.png",
    "greater demon": "https://oldschool.runescape.wiki/images/Greater_demon.png?f293e",
    "greater demons": "https://oldschool.runescape.wiki/images/Greater_demon.png?f293e",
    "grotesque guardians": "https://oldschool.runescape.wiki/images/Dawn.png?8b8ea",
    "leviathan": "https://oldschool.runescape.wiki/images/The_Leviathan.png?d588a",
    "lizardman shaman": "https://oldschool.runescape.wiki/images/thumb/Lizardman_shaman_%281%29.png/400px-Lizardman_shaman_%281%29.png?f127d",
    "lizardman shamans": "https://oldschool.runescape.wiki/images/thumb/Lizardman_shaman_%281%29.png/400px-Lizardman_shaman_%281%29.png?f127d",
    "n/a": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSct86lyd3Z1LnvnSvfzZvQc776_JbpT4KC7Q&s",
    "theatre of blood": "https://oldschool.runescape.wiki/w/Special:FilePath/Verzik_Vitur_(final_form).png",
    "theatre of blood: entry mode": "https://oldschool.runescape.wiki/w/Special:FilePath/Verzik_Vitur_(final_form).png",
    "theatre of blood: hard mode": "https://oldschool.runescape.wiki/w/Special:FilePath/Verzik_Vitur_(final_form).png",
    "tombs of amascut": "https://oldschool.runescape.wiki/w/Special:FilePath/Zebak.png",
    "tombs of amascut: entry mode": "https://oldschool.runescape.wiki/w/Special:FilePath/Zebak.png",
    "tombs of amascut: expert mode": "https://oldschool.runescape.wiki/w/Special:FilePath/Zebak.png",
    "thermonuclear smoke devil": "https://oldschool.runescape.wiki/w/Special:FilePath/Thermonuclear_smoke_devil.png",
    "tormented demon": "https://oldschool.runescape.wiki/w/Special:FilePath/Tormented_Demon_(1).png",
    "tormented demons": "https://oldschool.runescape.wiki/w/Special:FilePath/Tormented_Demon_(1).png",
    "tzhaar-ket-rak's challenges": "https://oldschool.runescape.wiki/w/Special:FilePath/JalTok-Jad.png",
    "phosani's nightmare": "https://oldschool.runescape.wiki/w/Special:FilePath/The_Nightmare.png",
    "phantom muspah": "https://oldschool.runescape.wiki/w/Special:FilePath/Phantom_Muspah_(ranged).png",
    "the whisperer": "https://oldschool.runescape.wiki/w/Special:FilePath/The_Whisperer.png",
    "whisperer": "https://oldschool.runescape.wiki/w/Special:FilePath/The_Whisperer.png",
    "wintertodt": "https://oldschool.runescape.wiki/images/Howling_Snow_Storm.gif?ec549",
    "moons of peril": "https://oldschool.runescape.wiki/images/Eclipse_Moon.png?c3e72",
    "royal titans": "https://oldschool.runescape.wiki/images/Branda_the_Fire_Queen.png?0687c",
    "zulrah": "https://oldschool.runescape.wiki/w/Special:FilePath/Zulrah_(serpentine).png",
}


@dataclass(slots=True)
class RandomTaskResult:
    task_id: int
    task_name: str
    task_url: str | None
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
    rerolls_remaining: int


@dataclass(slots=True)
class RerollResult:
    rsn: str
    previous_task: RandomTaskResult
    replacement_task: RandomTaskResult | None
    rerolls_remaining: int


@dataclass(slots=True)
class CompletionResult:
    rsn: str
    task: RandomTaskResult
    reward_key: str | None
    reward_status: str | None
    rerolls_remaining: int
    awarded_rerolls: int
    verified_assigned_completions: int
    live_verification_attempted: bool
    live_verified: bool


@dataclass(slots=True)
class ActiveTaskSummary:
    rsn: str
    task: RandomTaskResult


@dataclass(slots=True)
class AccountCompletedTasksSummary:
    rsn: str
    rank_label: str
    completed_tasks: int
    completed_ca_tasks: int
    total_ca_tasks: int
    total_points: int
    total_points_available: int


@dataclass(slots=True)
class UserTaskProfileSummary:
    discord_user_id: str
    rerolls_available: int
    completed_tasks_by_account: list[AccountCompletedTasksSummary]
    active_tasks: list[ActiveTaskSummary]


@dataclass(slots=True)
class TierThreshold:
    rank: int
    key: str
    label: str
    required_points: int


@dataclass(slots=True)
class BotHighscoreEntry:
    rsn: str
    verified_tasks: int
    verified_points: int
    pending_tasks: int = 0
    pending_points: int = 0


@dataclass(slots=True)
class TierLeaderEntry:
    rsn: str
    completed_tasks: int
    total_points: int
    tier_label: str


@dataclass(slots=True)
class HighscoresSummary:
    title: str
    description: str
    entries: list["HighscoresEntry"]
    empty_text: str
    reset_text: str | None = None
    total_verified_tasks: int = 0
    total_verified_points: int = 0
    total_pending_tasks: int = 0
    total_pending_points: int = 0
    includes_pending: bool = False


@dataclass(slots=True)
class HighscoresEntry:
    rank: int
    rsn: str
    headline: str
    detail: str


@dataclass(slots=True)
class BossImageMapping:
    npc: str
    npc_url: str | None
    npc_image_url: str | None
    task_count: int


@dataclass(slots=True)
class RewardPayoutEntry:
    reward_key: str
    discord_user_id: str | None
    rsn: str
    reward_label: str
    reward_display_value: str
    reward_tier: str
    payout_status: str
    redeemed_at: datetime | None
    payout_marked_at: datetime | None
    payout_marked_by: str | None
    payout_notes: str | None


@dataclass(slots=True)
class RewardPayoutSummary:
    unpaid_count: int
    paid_count: int
    unpaid_entries: list[RewardPayoutEntry]
    recent_paid_entries: list[RewardPayoutEntry]


@dataclass(slots=True)
class TaskRollKeyIssue:
    roll_key: str
    discord_user_id: str
    rsn: str
    roll_mode: str
    status: str
    created_at: datetime | None


@dataclass(slots=True)
class TaskRollRedeemResult:
    status: str
    roll_key: str
    discord_user_id: str
    rsn: str
    roll_mode: str
    task: RandomTaskResult | None
    rerolls_remaining: int | None
    used_at: datetime | None
    message: str


@dataclass(slots=True)
class RewardKeyStatusEntry:
    reward_key: str
    rsn: str
    task_id: int | None
    reward_display_value: str | None
    created_at: datetime | None
    verified_at: datetime | None
    used_at: datetime | None
    payout_marked_at: datetime | None


@dataclass(slots=True)
class UserRewardKeySummary:
    unused_count: int
    pending_count: int
    unpaid_count: int
    unused_entries: list[RewardKeyStatusEntry]
    pending_entries: list[RewardKeyStatusEntry]
    unpaid_entries: list[RewardKeyStatusEntry]
    limit_per_bucket: int


@dataclass(slots=True)
class AdminRerollUpdateResult:
    discord_user_id: str
    previous_rerolls: int
    current_rerolls: int


@dataclass(slots=True)
class AdminPendingResetResult:
    discord_user_id: str | None
    rsn: str | None
    reset_claims: int
    touched_rsns: list[str]
    ready_rewards_synced: int
    cancelled_rewards_synced: int


@dataclass(slots=True)
class AdminActiveTaskClearResult:
    discord_user_id: str
    rsn: str | None
    cleared_tasks: int
    touched_rsns: list[str]


@dataclass(slots=True)
class AdminRewardKeyIssueResult:
    discord_user_id: str
    rsn: str
    task_id: int
    reward_key: str
    reward_status: str
    used_active_task: bool
    created_new: bool


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
    if npc_name:
        override = _BOSS_IMAGE_OVERRIDES.get(" ".join(npc_name.split()).casefold())
        if override:
            return override

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
        task_url = _extract_first_href(cells[1])
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
                task_url=task_url,
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


def _normalize_task_type(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def _npc_group_key(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split()).casefold()


def _normalize_tier_label(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split()).casefold()


def _tier_rank(value: str | None) -> int | None:
    tier_key = _normalize_tier_label(value)
    if not tier_key:
        return None
    return _TIER_RANKS.get(tier_key)


def filter_reroll_candidate_ids(
    current_task_id: int,
    candidate_ids: Iterable[int],
    metadata_by_id: Mapping[int, Mapping[str, object] | dict],
) -> list[int]:
    current_task_id = int(current_task_id)
    candidates = [int(task_id) for task_id in candidate_ids if int(task_id) != current_task_id]
    if not candidates:
        return []

    current_metadata = metadata_by_id.get(current_task_id) or {}
    current_rank = _tier_rank(current_metadata.get("tier_label"))
    if current_rank is not None:
        lower_tier_candidates = [
            candidate_id
            for candidate_id in candidates
            if (
                (candidate_metadata := metadata_by_id.get(candidate_id) or {})
                and (candidate_rank := _tier_rank(candidate_metadata.get("tier_label"))) is not None
                and candidate_rank < current_rank
            )
        ]
        if lower_tier_candidates:
            return lower_tier_candidates

    current_npc_key = _npc_group_key(current_metadata.get("npc"))
    if current_npc_key:
        different_boss_candidates = [
            candidate_id
            for candidate_id in candidates
            if _npc_group_key((metadata_by_id.get(candidate_id) or {}).get("npc")) != current_npc_key
        ]
        if different_boss_candidates:
            return different_boss_candidates

    return candidates


def filter_task_ids_by_tier_cap(
    candidate_ids: Iterable[int],
    metadata_by_id: Mapping[int, Mapping[str, object] | dict],
    *,
    current_tier_rank: int,
    max_tiers_above: int = 3,
) -> list[int]:
    current_rank = max(int(current_tier_rank), 0)
    max_allowed_rank = min(len(_TIER_ORDER), current_rank + max(0, int(max_tiers_above)))

    filtered: list[int] = []
    for candidate_id in candidate_ids:
        task_id = int(candidate_id)
        task_rank = _tier_rank((metadata_by_id.get(task_id) or {}).get("tier_label"))
        if task_rank is not None and task_rank > max_allowed_rank:
            continue
        filtered.append(task_id)
    return filtered


def get_task_selection_weight(
    task_id: int,
    metadata_by_id: Mapping[int, Mapping[str, object] | dict],
    tier_weights: Mapping[str, int],
) -> int:
    metadata = metadata_by_id.get(int(task_id)) or {}
    tier_key = _normalize_tier_label(metadata.get("tier_label"))
    if not tier_key:
        return 1
    return max(int(tier_weights.get(tier_key, 1)), 0)


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

    def _pick_weighted_task_id(
        self,
        conn,
        candidate_ids: Iterable[int],
    ) -> int:
        candidates = [int(task_id) for task_id in candidate_ids]
        if not candidates:
            raise ValueError("Cannot pick a weighted task from an empty candidate list")

        metadata_by_id = self.db.get_task_metadata_for_ids(conn, candidates)
        weights = [
            get_task_selection_weight(
                task_id,
                metadata_by_id,
                self.settings.tier_assignment_weights,
            )
            for task_id in candidates
        ]
        total_weight = sum(weights)
        if total_weight <= 0:
            return self._rand.choice(candidates)

        target = self._rand.randint(1, total_weight)
        running_total = 0
        for task_id, weight in zip(candidates, weights):
            running_total += weight
            if target <= running_total:
                return task_id
        return candidates[-1]

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

                        _verified, _corrected, newly_verified_claims = self.db.reconcile_claims_with_scan(
                            conn,
                            rsn=rsn,
                            completed_task_ids=completed_ids,
                            scan_run_id=run_id,
                        )
                        self.db.sync_reward_statuses_for_rsn(conn, rsn)
                        mapped_discord_user_id = self.db.get_primary_discord_user_id_for_rsn(conn, rsn)
                        if mapped_discord_user_id:
                            self._award_tier_promotion_rerolls_if_due(
                                conn,
                                discord_user_id=mapped_discord_user_id,
                                rsn=rsn,
                            )
                        rewarded_users: set[str] = set()
                        for discord_user_id, task_id in newly_verified_claims:
                            if discord_user_id not in rewarded_users:
                                self._award_due_rerolls(conn, discord_user_id)
                                rewarded_users.add(discord_user_id)
                            self._award_boss_completion_reroll_if_due(
                                conn,
                                discord_user_id=discord_user_id,
                                rsn=rsn,
                                task_id=task_id,
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
            current_tier = self._get_current_tier_threshold(conn, rsn)
            current_tier_rank = current_tier.rank if current_tier is not None else 0
            incomplete_ids = self.db.get_incomplete_task_ids(conn, rsn)
            remaining_count = len(incomplete_ids)
            claimed_ids = self.db.get_claimed_task_ids_for_scan(
                conn,
                discord_user_id=discord_user_id,
                rsn=rsn,
                scan_run_id=latest_scan_id,
            )

            eligible_ids = self._filter_assignable_task_ids(
                conn,
                compute_eligible_task_ids(incomplete_ids, claimed_ids),
                incomplete_ids=incomplete_ids,
                current_tier_rank=current_tier_rank,
            )
            if not eligible_ids:
                return None

            task_id = self._pick_weighted_task_id(conn, eligible_ids)
            metadata = self.db.get_task_metadata(conn, task_id)

            if metadata is None:
                task_name = f"Task {task_id}"
                task_url = None
                task_description = None
                npc = None
                npc_url = None
                npc_image_url = None
                task_type = None
                tier_label = None
                points = None
            else:
                task_name = metadata.get("task_name") or f"Task {task_id}"
                task_url = metadata.get("task_url")
                task_description = metadata.get("description")
                npc = metadata.get("npc")
                npc_url = metadata.get("npc_url")
                npc_image_url = _infer_npc_image_url(npc_url, npc) or metadata.get("npc_image_url")
                task_type = metadata.get("task_type")
                tier_label = metadata.get("tier_label")
                points = metadata.get("points")

            return RandomTaskResult(
                task_id=task_id,
                task_name=task_name,
                task_url=task_url,
                task_description=task_description,
                npc=npc,
                npc_url=npc_url,
                npc_image_url=npc_image_url,
                task_type=task_type,
                tier_label=tier_label,
                points=points,
                eligible_count=remaining_count,
            )

    def _build_task_result(self, conn, task_id: int, eligible_count: int) -> RandomTaskResult:
        metadata = self.db.get_task_metadata(conn, task_id)
        if metadata is None:
            task_name = f"Task {task_id}"
            task_url = None
            task_description = None
            npc = None
            npc_url = None
            npc_image_url = None
            task_type = None
            tier_label = None
            points = None
        else:
            task_name = metadata.get("task_name") or f"Task {task_id}"
            task_url = metadata.get("task_url")
            task_description = metadata.get("description")
            npc = metadata.get("npc")
            npc_url = metadata.get("npc_url")
            npc_image_url = _infer_npc_image_url(npc_url, npc) or metadata.get("npc_image_url")
            task_type = metadata.get("task_type")
            tier_label = metadata.get("tier_label")
            points = metadata.get("points")

        return RandomTaskResult(
            task_id=task_id,
            task_name=task_name,
            task_url=task_url,
            task_description=task_description,
            npc=npc,
            npc_url=npc_url,
            npc_image_url=npc_image_url,
            task_type=task_type,
            tier_label=tier_label,
            points=points,
            eligible_count=eligible_count,
        )

    def _filter_assignable_task_ids(
        self,
        conn,
        eligible_ids: Iterable[int],
        *,
        incomplete_ids: Iterable[int],
        current_tier_rank: int,
    ) -> list[int]:
        eligible = sorted({int(task_id) for task_id in eligible_ids})
        if not eligible:
            return []

        incomplete = {int(task_id) for task_id in incomplete_ids}
        metadata_by_id = self.db.get_task_metadata_for_ids(conn, sorted(set(eligible) | incomplete))
        eligible = filter_task_ids_by_tier_cap(
            eligible,
            metadata_by_id,
            current_tier_rank=current_tier_rank,
        )
        if not eligible:
            return []
        incomplete_by_npc: dict[str, set[int]] = {}

        for task_id in incomplete:
            metadata = metadata_by_id.get(task_id)
            npc_key = _npc_group_key(metadata.get("npc") if metadata else None)
            if not npc_key:
                continue
            incomplete_by_npc.setdefault(npc_key, set()).add(task_id)

        filtered: list[int] = []
        for task_id in eligible:
            metadata = metadata_by_id.get(task_id)
            task_type_key = _normalize_task_type(metadata.get("task_type") if metadata else None)
            if task_type_key != "killcount":
                filtered.append(task_id)
                continue

            npc_key = _npc_group_key(metadata.get("npc") if metadata else None)
            if not npc_key:
                filtered.append(task_id)
                continue

            remaining_for_boss = incomplete_by_npc.get(npc_key, set())
            if remaining_for_boss <= {task_id}:
                filtered.append(task_id)

        return filtered

    def _compute_assignable_task_ids_with_conn(
        self,
        conn,
        discord_user_id: str,
        rsn: str,
        *,
        include_task_id: int | None = None,
    ) -> list[int]:
        latest_scan_id = self.db.get_latest_completed_scan_run_id(conn)
        current_tier = self._get_current_tier_threshold(conn, rsn)
        current_tier_rank = current_tier.rank if current_tier is not None else 0
        incomplete_ids = self.db.get_incomplete_task_ids(conn, rsn)
        claimed_ids = self.db.get_claimed_task_ids_for_scan(
            conn,
            discord_user_id=discord_user_id,
            rsn=rsn,
            scan_run_id=latest_scan_id,
        )
        assignable_ids = self._filter_assignable_task_ids(
            conn,
            compute_eligible_task_ids(incomplete_ids, claimed_ids),
            incomplete_ids=incomplete_ids,
            current_tier_rank=current_tier_rank,
        )

        if include_task_id is not None:
            target_task_id = int(include_task_id)
            if target_task_id not in assignable_ids:
                metadata = self.db.get_task_metadata(conn, target_task_id)
                if metadata is not None:
                    assignable_ids.append(target_task_id)

        return sorted({int(task_id) for task_id in assignable_ids})

    def get_assignable_task_ids_for_user(
        self,
        discord_user_id: str,
        rsn: str,
        *,
        include_task_id: int | None = None,
    ) -> list[int]:
        normalized_user_id = str(discord_user_id).strip()
        normalized_rsn = str(rsn).strip()
        if not normalized_user_id or not normalized_rsn:
            return []
        with self.db.connection() as conn:
            return self._compute_assignable_task_ids_with_conn(
                conn,
                normalized_user_id,
                normalized_rsn,
                include_task_id=include_task_id,
            )

    def _award_due_rerolls(self, conn, discord_user_id: str) -> tuple[int, int, int]:
        profile = self.db.get_user_task_profile(conn, discord_user_id, for_update=True)
        rerolls_available = int(profile["rerolls_available"] or 0)
        current_batches = int(profile["completion_reward_batches"] or 0)
        verified_count = self.db.count_verified_task_claims(conn, discord_user_id)
        target_batches = verified_count // 3
        award_count = max(target_batches - current_batches, 0)
        if award_count > 0:
            rerolls_available += award_count
            self.db.update_user_task_profile(
                conn,
                discord_user_id,
                rerolls_available=rerolls_available,
                completion_reward_batches=target_batches,
            )
        return award_count, rerolls_available, verified_count

    def _award_boss_completion_reroll_if_due(
        self,
        conn,
        discord_user_id: str,
        rsn: str,
        task_id: int,
    ) -> int:
        metadata = self.db.get_task_metadata(conn, task_id)
        npc = str(metadata.get("npc") or "").strip() if metadata else ""
        npc_key = _npc_group_key(npc)
        if not npc_key:
            return 0

        incomplete_ids = self.db.get_incomplete_task_ids(conn, rsn)
        if incomplete_ids:
            incomplete_metadata = self.db.get_task_metadata_for_ids(conn, sorted(incomplete_ids))
            for incomplete_task_id in incomplete_ids:
                incomplete_npc_key = _npc_group_key(
                    incomplete_metadata.get(incomplete_task_id, {}).get("npc"),
                )
                if incomplete_npc_key == npc_key:
                    return 0

        inserted = self.db.record_boss_completion_reroll_reward(
            conn,
            discord_user_id=discord_user_id,
            rsn=rsn,
            npc=npc,
            rewarded_task_id=task_id,
        )
        if not inserted:
            return 0

        profile = self.db.get_user_task_profile(conn, discord_user_id, for_update=True)
        rerolls_available = int(profile["rerolls_available"] or 0) + 1
        self.db.update_user_task_profile(
            conn,
            discord_user_id,
            rerolls_available=rerolls_available,
        )
        return 1

    def _get_catalog_tier_thresholds(self, conn) -> list[TierThreshold]:
        tier_totals = self.db.get_catalog_point_totals_by_tier(conn)
        thresholds: list[TierThreshold] = []
        cumulative_points = 0

        for rank, (tier_key, tier_label) in enumerate(_TIER_ORDER, start=1):
            matched_points = 0
            for catalog_label, tier_points in tier_totals.items():
                if _normalize_tier_label(catalog_label) == tier_key:
                    matched_points = int(tier_points or 0)
                    break
            if matched_points <= 0:
                continue
            cumulative_points += matched_points
            thresholds.append(
                TierThreshold(
                    rank=rank,
                    key=tier_key,
                    label=tier_label,
                    required_points=cumulative_points,
                )
            )

        return thresholds

    def _get_tier_threshold_for_points(self, conn, total_points: int) -> TierThreshold | None:
        current_tier: TierThreshold | None = None
        for threshold in self._get_catalog_tier_thresholds(conn):
            if int(total_points) >= threshold.required_points:
                current_tier = threshold
            else:
                break
        return current_tier

    def _get_current_tier_threshold(self, conn, rsn: str) -> TierThreshold | None:
        total_points = self.db.get_progress_summary(conn, rsn)["total_points"]
        return self._get_tier_threshold_for_points(conn, total_points)

    def _get_local_month_window(self) -> tuple[datetime, datetime, str, str]:
        try:
            tz = ZoneInfo(self.settings.timezone)
        except Exception:
            LOGGER.warning("Invalid TIMEZONE '%s'; defaulting leaderboard window to UTC", self.settings.timezone)
            tz = ZoneInfo("UTC")

        now = datetime.now(tz)
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        days_remaining = max((end.date() - now.date()).days, 0)
        reset_text = "today" if days_remaining == 0 else f"in {days_remaining} day{'s' if days_remaining != 1 else ''}"
        return start.replace(tzinfo=None), end.replace(tzinfo=None), now.strftime("%B %Y"), reset_text

    def get_monthly_highscores_summary(self, limit: int | None = None) -> HighscoresSummary:
        start_at, end_at, month_label, reset_text = self._get_local_month_window()
        with self.db.connection() as conn:
            rows = self.db.get_verified_claim_leaderboard(
                conn,
                limit=limit if limit is None or limit > 0 else None,
                start_at=start_at,
                end_at=end_at,
            )

        ranked_entries = [
            BotHighscoreEntry(
                rsn=str(row.get("rsn") or "").strip(),
                verified_tasks=int(row.get("verified_tasks") or 0),
                verified_points=int(row.get("verified_points") or 0),
                pending_tasks=0,
                pending_points=0,
            )
            for row in rows
            if str(row.get("rsn") or "").strip()
        ]
        entries = [
            HighscoresEntry(
                rank=index,
                rsn=entry.rsn,
                headline=f"verified points : {entry.verified_points}",
                detail=f"verified tasks  : {entry.verified_tasks}",
            )
            for index, entry in enumerate(ranked_entries, start=1)
        ]
        return HighscoresSummary(
            title=f"Monthly Highscores - {month_label}",
            description="Bot-assigned progress this month (WikiSync verified only).",
            entries=entries,
            empty_text="No verified bot claims yet this month.",
            reset_text=reset_text,
            total_verified_tasks=sum(entry.verified_tasks for entry in ranked_entries),
            total_verified_points=sum(entry.verified_points for entry in ranked_entries),
            total_pending_tasks=0,
            total_pending_points=0,
            includes_pending=False,
        )

    def get_all_time_highscores_summary(self, limit: int | None = None) -> HighscoresSummary:
        with self.db.connection() as conn:
            rows = self.db.get_verified_claim_leaderboard(
                conn,
                limit=limit if limit is None or limit > 0 else None,
            )

        ranked_entries = [
            BotHighscoreEntry(
                rsn=str(row.get("rsn") or "").strip(),
                verified_tasks=int(row.get("verified_tasks") or 0),
                verified_points=int(row.get("verified_points") or 0),
                pending_tasks=0,
                pending_points=0,
            )
            for row in rows
            if str(row.get("rsn") or "").strip()
        ]
        entries = [
            HighscoresEntry(
                rank=index,
                rsn=entry.rsn,
                headline=f"verified points : {entry.verified_points}",
                detail=f"verified tasks  : {entry.verified_tasks}",
            )
            for index, entry in enumerate(ranked_entries, start=1)
        ]
        return HighscoresSummary(
            title="All-Time Bot Highscores",
            description="All-time bot-assigned progress (WikiSync verified only).",
            entries=entries,
            empty_text="No verified bot claims have been recorded yet.",
            total_verified_tasks=sum(entry.verified_tasks for entry in ranked_entries),
            total_verified_points=sum(entry.verified_points for entry in ranked_entries),
            total_pending_tasks=0,
            total_pending_points=0,
            includes_pending=False,
        )

    def get_overall_tier_leaders_summary(self, limit: int | None = None) -> HighscoresSummary:
        with self.db.connection() as conn:
            rows = self.db.get_progress_leaderboard(
                conn,
                limit=limit if limit is None or limit > 0 else None,
            )
            tier_entries = []
            for row in rows:
                rsn = str(row.get("rsn") or "").strip()
                if not rsn:
                    continue
                total_points = int(row.get("total_points") or 0)
                current_tier = self._get_tier_threshold_for_points(conn, total_points)
                tier_entries.append(
                    TierLeaderEntry(
                        rsn=rsn,
                        completed_tasks=int(row.get("completed_tasks") or 0),
                        total_points=total_points,
                        tier_label=current_tier.label if current_tier is not None else "Unranked",
                    )
                )

        entries = [
            HighscoresEntry(
                rank=index,
                rsn=entry.rsn,
                headline=f"rank tier       : {entry.tier_label}",
                detail=(
                    f"ca points       : {entry.total_points}\n"
                    f"completed tasks : {entry.completed_tasks}"
                ),
            )
            for index, entry in enumerate(tier_entries, start=1)
        ]
        return HighscoresSummary(
            title="Overall Tier Leaders",
            description="Current overall CA tier and total points by account.",
            entries=entries,
            empty_text="No overall progress data has been recorded yet.",
            includes_pending=False,
        )

    def get_boss_image_mappings(self) -> list[BossImageMapping]:
        with self.db.connection() as conn:
            rows = self.db.get_catalog_boss_image_mappings(conn)

        mappings: list[BossImageMapping] = []
        for row in rows:
            npc = str(row.get("npc") or "").strip()
            if not npc:
                continue
            npc_url = str(row.get("npc_url") or "").strip() or None
            stored_image_url = str(row.get("npc_image_url") or "").strip() or None
            mappings.append(
                BossImageMapping(
                    npc=npc,
                    npc_url=npc_url,
                    npc_image_url=_infer_npc_image_url(npc_url, npc) or stored_image_url,
                    task_count=int(row.get("task_count") or 0),
                )
            )
        return mappings

    def _build_reward_payout_entry(self, row: Mapping[str, object]) -> RewardPayoutEntry:
        reward_label = str(row.get("reward_label") or "").strip()
        reward_amount = int(row.get("reward_amount") or 0) or None
        reward_quantity = int(row.get("reward_quantity") or 0) or None
        reward_kind = str(row.get("reward_kind") or "").strip().casefold() or ("gp" if reward_amount else "item")
        reward_display_value = format_reward_display(
            kind=reward_kind,
            label=reward_label,
            amount=reward_amount,
            quantity=reward_quantity,
        )
        reward_tier = str(row.get("reward_tier") or "").strip().title() or "Unknown"
        payout_status = str(row.get("payout_status") or "").strip().casefold() or "unpaid"
        redeemed_at = row.get("used_at")
        payout_marked_at = row.get("payout_marked_at")
        payout_marked_by = str(row.get("payout_marked_by") or "").strip() or None
        payout_notes = str(row.get("payout_notes") or "").strip() or None
        return RewardPayoutEntry(
            reward_key=str(row.get("reward_key") or "").strip(),
            discord_user_id=str(row.get("discord_user_id") or "").strip() or None,
            rsn=str(row.get("rsn") or "").strip(),
            reward_label=reward_label or reward_display_value,
            reward_display_value=reward_display_value,
            reward_tier=reward_tier,
            payout_status=payout_status,
            redeemed_at=redeemed_at if isinstance(redeemed_at, datetime) else None,
            payout_marked_at=payout_marked_at if isinstance(payout_marked_at, datetime) else None,
            payout_marked_by=payout_marked_by,
            payout_notes=payout_notes,
        )

    def _build_reward_key_status_entry(self, row: Mapping[str, object]) -> RewardKeyStatusEntry:
        task_id_raw = row.get("task_id")
        reward_label = str(row.get("reward_label") or "").strip()
        reward_kind = str(row.get("reward_kind") or "").strip().casefold()
        amount_raw = row.get("reward_amount")
        quantity_raw = row.get("reward_quantity")
        reward_amount: int | None = None
        reward_quantity: int | None = None
        if amount_raw is not None:
            try:
                reward_amount = int(amount_raw)
            except (TypeError, ValueError):
                reward_amount = None
        if quantity_raw is not None:
            try:
                reward_quantity = int(quantity_raw)
            except (TypeError, ValueError):
                reward_quantity = None
        normalized_kind = reward_kind or ("gp" if reward_amount is not None else "item")
        reward_display_value: str | None = None
        if reward_label or reward_amount is not None or reward_quantity is not None:
            reward_display_value = format_reward_display(
                kind=normalized_kind,
                label=reward_label,
                amount=reward_amount,
                quantity=reward_quantity,
            )
        return RewardKeyStatusEntry(
            reward_key=str(row.get("reward_key") or "").strip(),
            rsn=str(row.get("rsn") or "").strip(),
            task_id=int(task_id_raw) if task_id_raw is not None else None,
            reward_display_value=reward_display_value,
            created_at=row.get("created_at") if isinstance(row.get("created_at"), datetime) else None,
            verified_at=row.get("verified_at") if isinstance(row.get("verified_at"), datetime) else None,
            used_at=row.get("used_at") if isinstance(row.get("used_at"), datetime) else None,
            payout_marked_at=(
                row.get("payout_marked_at")
                if isinstance(row.get("payout_marked_at"), datetime)
                else None
            ),
        )

    def get_reward_payout_summary(
        self,
        *,
        unpaid_limit: int = 12,
        paid_limit: int = 8,
    ) -> RewardPayoutSummary:
        with self.db.connection() as conn:
            counts = self.db.get_reward_payout_counts(conn)
            unpaid_rows = self.db.list_reward_payouts(conn, payout_status="unpaid", limit=unpaid_limit)
            paid_rows = self.db.list_reward_payouts(conn, payout_status="paid", limit=paid_limit)

        return RewardPayoutSummary(
            unpaid_count=int(counts.get("unpaid") or 0),
            paid_count=int(counts.get("paid") or 0),
            unpaid_entries=[self._build_reward_payout_entry(row) for row in unpaid_rows],
            recent_paid_entries=[self._build_reward_payout_entry(row) for row in paid_rows],
        )

    def get_user_reward_key_summary(
        self,
        discord_user_id: str,
        *,
        limit_per_bucket: int = 12,
    ) -> UserRewardKeySummary:
        with self.db.connection() as conn:
            raw = self.db.get_reward_key_status_summary_for_user(
                conn,
                discord_user_id=discord_user_id,
                limit_per_bucket=limit_per_bucket,
            )

        return UserRewardKeySummary(
            unused_count=int(raw.get("unused_count") or 0),
            pending_count=int(raw.get("pending_count") or 0),
            unpaid_count=int(raw.get("unpaid_count") or 0),
            unused_entries=[
                self._build_reward_key_status_entry(row)
                for row in raw.get("unused_entries", [])
            ],
            pending_entries=[
                self._build_reward_key_status_entry(row)
                for row in raw.get("pending_entries", [])
            ],
            unpaid_entries=[
                self._build_reward_key_status_entry(row)
                for row in raw.get("unpaid_entries", [])
            ],
            limit_per_bucket=int(raw.get("limit_per_bucket") or max(int(limit_per_bucket), 1)),
        )

    def admin_set_rerolls(self, discord_user_id: str, rerolls_available: int) -> AdminRerollUpdateResult:
        normalized_user_id = str(discord_user_id).strip()
        if not normalized_user_id:
            raise ValueError("Discord user ID is required")

        target_rerolls = max(int(rerolls_available), 0)
        with self.db.connection() as conn:
            try:
                profile = self.db.get_user_task_profile(conn, normalized_user_id, for_update=True)
                previous = int(profile.get("rerolls_available") or 0)
                self.db.update_user_task_profile(
                    conn,
                    normalized_user_id,
                    rerolls_available=target_rerolls,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return AdminRerollUpdateResult(
            discord_user_id=normalized_user_id,
            previous_rerolls=previous,
            current_rerolls=target_rerolls,
        )

    def admin_adjust_rerolls(self, discord_user_id: str, delta: int) -> AdminRerollUpdateResult:
        normalized_user_id = str(discord_user_id).strip()
        if not normalized_user_id:
            raise ValueError("Discord user ID is required")

        delta_value = int(delta)
        with self.db.connection() as conn:
            try:
                profile = self.db.get_user_task_profile(conn, normalized_user_id, for_update=True)
                previous = int(profile.get("rerolls_available") or 0)
                current = max(previous + delta_value, 0)
                self.db.update_user_task_profile(
                    conn,
                    normalized_user_id,
                    rerolls_available=current,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return AdminRerollUpdateResult(
            discord_user_id=normalized_user_id,
            previous_rerolls=previous,
            current_rerolls=current,
        )

    def admin_reset_pending_claims(
        self,
        discord_user_id: str | None = None,
        *,
        rsn: str | None = None,
    ) -> AdminPendingResetResult:
        normalized_user_id = (discord_user_id or "").strip() or None
        normalized_rsn = (rsn or "").strip() or None
        with self.db.connection() as conn:
            try:
                reset_claims, touched_rsns = self.db.reset_pending_claims(
                    conn,
                    discord_user_id=normalized_user_id,
                    rsn=normalized_rsn,
                )
                ready_synced = 0
                cancelled_synced = 0
                for touched_rsn in touched_rsns:
                    ready_count, cancelled_count = self.db.sync_reward_statuses_for_rsn(conn, touched_rsn)
                    ready_synced += int(ready_count or 0)
                    cancelled_synced += int(cancelled_count or 0)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return AdminPendingResetResult(
            discord_user_id=normalized_user_id,
            rsn=normalized_rsn,
            reset_claims=int(reset_claims or 0),
            touched_rsns=touched_rsns,
            ready_rewards_synced=ready_synced,
            cancelled_rewards_synced=cancelled_synced,
        )

    def admin_clear_active_tasks(
        self,
        discord_user_id: str,
        *,
        rsn: str | None = None,
    ) -> AdminActiveTaskClearResult:
        normalized_user_id = str(discord_user_id).strip()
        if not normalized_user_id:
            raise ValueError("Discord user ID is required")

        normalized_rsn = (rsn or "").strip() or None
        with self.db.connection() as conn:
            try:
                cleared_tasks, touched_rsns = self.db.clear_active_tasks(
                    conn,
                    discord_user_id=normalized_user_id,
                    rsn=normalized_rsn,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return AdminActiveTaskClearResult(
            discord_user_id=normalized_user_id,
            rsn=normalized_rsn,
            cleared_tasks=int(cleared_tasks or 0),
            touched_rsns=touched_rsns,
        )

    def admin_issue_reward_key(
        self,
        discord_user_id: str,
        *,
        rsn: str,
        task_id: int | None = None,
        quantity: int = 1,
    ) -> list[AdminRewardKeyIssueResult] | None:
        normalized_user_id = str(discord_user_id).strip()
        normalized_rsn = str(rsn or "").strip()
        if not normalized_user_id or not normalized_rsn:
            raise ValueError("Discord user ID and RSN are required")

        requested_quantity = int(quantity)
        if requested_quantity <= 0:
            raise ValueError("Quantity must be at least 1.")
        if requested_quantity > 25:
            raise ValueError("Quantity cannot exceed 25.")

        explicit_task_id = int(task_id) if task_id is not None else None
        if explicit_task_id is not None and explicit_task_id <= 0:
            raise ValueError("Task ID must be a positive integer")

        with self.db.connection() as conn:
            resolved_task_id: int | None = explicit_task_id
            used_active_task = False
            if resolved_task_id is None:
                active = self.db.get_active_task(conn, normalized_user_id, normalized_rsn)
                if active is None:
                    return None
                resolved_task_id = int(active["task_id"])
                used_active_task = True

            catalog_ids = self.db.get_catalog_task_ids(conn)
            if not catalog_ids:
                raise ValueError("Task catalog is empty; cannot issue reward keys.")
            catalog_id_set = {int(task_catalog_id) for task_catalog_id in catalog_ids}
            if int(resolved_task_id) not in catalog_id_set:
                raise ValueError(f"Task ID {resolved_task_id} was not found in the task catalog.")

            issued_task_ids: list[int] = [int(resolved_task_id)]
            if requested_quantity > 1:
                existing_task_ids = set(
                    self.db.list_reward_task_ids_for_user_rsn(
                        conn,
                        normalized_user_id,
                        normalized_rsn,
                        for_update=True,
                    )
                )
                existing_task_ids.add(int(resolved_task_id))
                for candidate_task_id in catalog_ids:
                    current_task_id = int(candidate_task_id)
                    if current_task_id in existing_task_ids:
                        continue
                    issued_task_ids.append(current_task_id)
                    existing_task_ids.add(current_task_id)
                    if len(issued_task_ids) >= requested_quantity:
                        break

                if len(issued_task_ids) < requested_quantity:
                    raise ValueError(
                        (
                            f"Only {len(issued_task_ids)} key(s) can be issued for `{normalized_rsn}` "
                            "before task IDs start duplicating existing rewards."
                        )
                    )

            issued_results: list[AdminRewardKeyIssueResult] = []
            for current_task_id in issued_task_ids:
                existing = self.db.get_reward_for_claim(
                    conn,
                    normalized_user_id,
                    normalized_rsn,
                    current_task_id,
                    for_update=True,
                )
                created_new = existing is None
                reward_row = self.db.ensure_reward_for_claim(
                    conn,
                    normalized_user_id,
                    normalized_rsn,
                    current_task_id,
                )
                reward_key = str(reward_row.get("reward_key") or "").strip()
                ready_row = self.db.set_reward_ready_by_key(conn, reward_key) or reward_row
                issued_results.append(
                    AdminRewardKeyIssueResult(
                        discord_user_id=normalized_user_id,
                        rsn=normalized_rsn,
                        task_id=int(current_task_id),
                        reward_key=reward_key,
                        reward_status=str(ready_row.get("status") or "").strip() or "ready",
                        used_active_task=bool(used_active_task and int(current_task_id) == int(resolved_task_id)),
                        created_new=created_new,
                    )
                )

            conn.commit()

        return issued_results

    def mark_reward_paid(
        self,
        reward_key: str,
        *,
        actor: str | None = None,
        notes: str | None = None,
    ) -> RewardPayoutEntry | None:
        normalized_key = reward_key.strip().upper()
        if not normalized_key:
            return None

        with self.db.connection() as conn:
            try:
                row = self.db.update_reward_payout_status(
                    conn,
                    normalized_key,
                    payout_status="paid",
                    marked_by=actor,
                    notes=notes,
                )
                if row is None:
                    conn.rollback()
                    return None
                if str(row.get("status") or "").strip() != "redeemed":
                    conn.rollback()
                    return None
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return self._build_reward_payout_entry(row)

    def mark_reward_unpaid(
        self,
        reward_key: str,
        *,
        actor: str | None = None,
        notes: str | None = None,
    ) -> RewardPayoutEntry | None:
        normalized_key = reward_key.strip().upper()
        if not normalized_key:
            return None

        with self.db.connection() as conn:
            try:
                row = self.db.update_reward_payout_status(
                    conn,
                    normalized_key,
                    payout_status="unpaid",
                    marked_by=actor,
                    notes=notes,
                )
                if row is None:
                    conn.rollback()
                    return None
                if str(row.get("status") or "").strip() != "redeemed":
                    conn.rollback()
                    return None
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return self._build_reward_payout_entry(row)

    def issue_task_roll_key(
        self,
        discord_user_id: str,
        *,
        rsn: str | None = None,
        roll_mode: str = "new",
    ) -> TaskRollKeyIssue | None:
        normalized_user_id = str(discord_user_id).strip()
        normalized_rsn = (rsn or "").strip()
        normalized_mode = (roll_mode or "new").strip().casefold()
        if normalized_mode not in {"new", "reroll"}:
            raise ValueError(f"Unsupported roll mode: {roll_mode}")

        with self.db.connection() as conn:
            target_rsn = normalized_rsn
            if not target_rsn:
                target_rsn = self.db.get_rsn_for_discord_user(conn, normalized_user_id) or ""
            if not target_rsn:
                return None
            created = self.db.create_task_roll_key(
                conn,
                discord_user_id=normalized_user_id,
                rsn=target_rsn,
                roll_mode=normalized_mode,
            )
            conn.commit()

        return TaskRollKeyIssue(
            roll_key=str(created.get("roll_key") or "").strip(),
            discord_user_id=str(created.get("discord_user_id") or "").strip(),
            rsn=str(created.get("rsn") or "").strip(),
            roll_mode=str(created.get("roll_mode") or "").strip().casefold() or "new",
            status=str(created.get("status") or "").strip().casefold() or "ready",
            created_at=created.get("created_at") if isinstance(created.get("created_at"), datetime) else None,
        )

    def _task_from_task_roll_row(self, row: Mapping[str, object], *, conn=None) -> RandomTaskResult | None:
        task_id_raw = row.get("result_task_id")
        if task_id_raw is None:
            return None
        task_name = str(row.get("result_task_name") or "").strip()
        if not task_name:
            return None
        task_id = int(task_id_raw)
        metadata = self.db.get_task_metadata(conn, task_id) if conn is not None else None
        npc_value = str(row.get("result_npc") or "").strip() or None
        task_url_value = str((metadata or {}).get("task_url") or "").strip() or None
        npc_url_value = str((metadata or {}).get("npc_url") or "").strip() or None
        task_type_value = str((metadata or {}).get("task_type") or "").strip() or None
        task_description_value = str((metadata or {}).get("description") or "").strip() or None
        return RandomTaskResult(
            task_id=task_id,
            task_name=task_name,
            task_url=task_url_value,
            task_description=task_description_value,
            npc=npc_value,
            npc_url=npc_url_value,
            npc_image_url=str(row.get("result_npc_image_url") or "").strip() or None,
            task_type=task_type_value,
            tier_label=str(row.get("result_tier_label") or "").strip() or None,
            points=int(row.get("result_points") or 0) if row.get("result_points") is not None else None,
            eligible_count=0,
        )

    def redeem_task_roll_key(self, roll_key: str) -> TaskRollRedeemResult:
        normalized_key = (roll_key or "").strip().upper()
        if not normalized_key:
            return TaskRollRedeemResult(
                status="invalid",
                roll_key="",
                discord_user_id="",
                rsn="",
                roll_mode="new",
                task=None,
                rerolls_remaining=None,
                used_at=None,
                message="Enter a task roll key first.",
            )

        with self.db.connection() as conn:
            row = self.db.get_task_roll_key(conn, normalized_key, for_update=True)
            if row is None:
                conn.rollback()
                return TaskRollRedeemResult(
                    status="invalid",
                    roll_key=normalized_key,
                    discord_user_id="",
                    rsn="",
                    roll_mode="new",
                    task=None,
                    rerolls_remaining=None,
                    used_at=None,
                    message="That task roll key does not exist.",
                )

            roll_mode = str(row.get("roll_mode") or "").strip().casefold() or "new"
            status = str(row.get("status") or "").strip().casefold() or "ready"
            discord_user_id = str(row.get("discord_user_id") or "").strip()
            rsn = str(row.get("rsn") or "").strip()

            if status == "used":
                task = self._task_from_task_roll_row(row, conn=conn)
                conn.rollback()
                return TaskRollRedeemResult(
                    status="used",
                    roll_key=normalized_key,
                    discord_user_id=discord_user_id,
                    rsn=rsn,
                    roll_mode=roll_mode,
                    task=task,
                    rerolls_remaining=(
                        int(row.get("result_rerolls_remaining") or 0)
                        if row.get("result_rerolls_remaining") is not None
                        else None
                    ),
                    used_at=row.get("used_at") if isinstance(row.get("used_at"), datetime) else None,
                    message="That task roll key has already been used.",
                )
            if status == "cancelled":
                conn.rollback()
                return TaskRollRedeemResult(
                    status="cancelled",
                    roll_key=normalized_key,
                    discord_user_id=discord_user_id,
                    rsn=rsn,
                    roll_mode=roll_mode,
                    task=None,
                    rerolls_remaining=None,
                    used_at=None,
                    message="That task roll key was cancelled.",
                )
            if status == "expired":
                conn.rollback()
                return TaskRollRedeemResult(
                    status="expired",
                    roll_key=normalized_key,
                    discord_user_id=discord_user_id,
                    rsn=rsn,
                    roll_mode=roll_mode,
                    task=None,
                    rerolls_remaining=None,
                    used_at=None,
                    message="That task roll key has expired.",
                )
            if status != "ready":
                conn.rollback()
                return TaskRollRedeemResult(
                    status="invalid",
                    roll_key=normalized_key,
                    discord_user_id=discord_user_id,
                    rsn=rsn,
                    roll_mode=roll_mode,
                    task=None,
                    rerolls_remaining=None,
                    used_at=None,
                    message="That task roll key is not in a usable state.",
                )

            expires_at = row.get("expires_at")
            if isinstance(expires_at, datetime) and expires_at <= datetime.now():
                self.db.cancel_task_roll_key(conn, normalized_key, status="expired")
                conn.commit()
                return TaskRollRedeemResult(
                    status="expired",
                    roll_key=normalized_key,
                    discord_user_id=discord_user_id,
                    rsn=rsn,
                    roll_mode=roll_mode,
                    task=None,
                    rerolls_remaining=None,
                    used_at=None,
                    message="That task roll key has expired.",
                )

            if roll_mode == "reroll":
                reroll = self._reroll_active_task_with_conn(
                    conn,
                    discord_user_id,
                    rsn,
                    consume_reroll=True,
                )
                if reroll is None:
                    conn.rollback()
                    return TaskRollRedeemResult(
                        status="no_active",
                        roll_key=normalized_key,
                        discord_user_id=discord_user_id,
                        rsn=rsn,
                        roll_mode=roll_mode,
                        task=None,
                        rerolls_remaining=None,
                        used_at=None,
                        message=f"No active task found for `{rsn}` to reroll.",
                    )
                if reroll.replacement_task is None:
                    conn.rollback()
                    if reroll.rerolls_remaining <= 0:
                        return TaskRollRedeemResult(
                            status="no_rerolls",
                            roll_key=normalized_key,
                            discord_user_id=discord_user_id,
                            rsn=rsn,
                            roll_mode=roll_mode,
                            task=reroll.previous_task,
                            rerolls_remaining=0,
                            used_at=None,
                            message=f"You have no rerolls left for `{rsn}`.",
                        )
                    return TaskRollRedeemResult(
                        status="no_alternative",
                        roll_key=normalized_key,
                        discord_user_id=discord_user_id,
                        rsn=rsn,
                        roll_mode=roll_mode,
                        task=None,
                        rerolls_remaining=reroll.rerolls_remaining,
                        used_at=None,
                        message=f"No alternative eligible reroll task found for `{rsn}`.",
                    )
                outcome_task = reroll.replacement_task
                rerolls_remaining = reroll.rerolls_remaining
            else:
                assignment = self._get_or_assign_active_task_with_conn(conn, discord_user_id, rsn)
                if assignment is None:
                    conn.rollback()
                    return TaskRollRedeemResult(
                        status="no_task",
                        roll_key=normalized_key,
                        discord_user_id=discord_user_id,
                        rsn=rsn,
                        roll_mode=roll_mode,
                        task=None,
                        rerolls_remaining=None,
                        used_at=None,
                        message=f"No eligible tasks found for `{rsn}`.",
                    )
                outcome_task = assignment.task
                rerolls_remaining = assignment.rerolls_remaining

            updated = self.db.mark_task_roll_key_used(
                conn,
                normalized_key,
                task_id=outcome_task.task_id,
                task_name=outcome_task.task_name,
                tier_label=outcome_task.tier_label,
                points=outcome_task.points,
                npc=outcome_task.npc,
                npc_image_url=outcome_task.npc_image_url,
                rerolls_remaining=rerolls_remaining,
            )
            if updated is None:
                latest = self.db.get_task_roll_key(conn, normalized_key, for_update=True)
                conn.rollback()
                if latest is not None and str(latest.get("status") or "").strip().casefold() == "used":
                    return TaskRollRedeemResult(
                        status="used",
                        roll_key=normalized_key,
                        discord_user_id=discord_user_id,
                        rsn=rsn,
                        roll_mode=roll_mode,
                        task=self._task_from_task_roll_row(latest, conn=conn),
                        rerolls_remaining=(
                            int(latest.get("result_rerolls_remaining") or 0)
                            if latest.get("result_rerolls_remaining") is not None
                            else None
                        ),
                        used_at=latest.get("used_at") if isinstance(latest.get("used_at"), datetime) else None,
                        message="That task roll key has already been used.",
                    )
                return TaskRollRedeemResult(
                    status="invalid",
                    roll_key=normalized_key,
                    discord_user_id=discord_user_id,
                    rsn=rsn,
                    roll_mode=roll_mode,
                    task=None,
                    rerolls_remaining=None,
                    used_at=None,
                    message="Could not finalize that task roll key.",
                )

            conn.commit()
            return TaskRollRedeemResult(
                status="ok",
                roll_key=normalized_key,
                discord_user_id=discord_user_id,
                rsn=rsn,
                roll_mode=roll_mode,
                task=outcome_task,
                rerolls_remaining=rerolls_remaining,
                used_at=updated.get("used_at") if isinstance(updated.get("used_at"), datetime) else None,
                message="Task rolled successfully.",
            )

    def _award_tier_promotion_rerolls_if_due(
        self,
        conn,
        discord_user_id: str,
        rsn: str,
    ) -> int:
        current_tier = self._get_current_tier_threshold(conn, rsn)
        if current_tier is None:
            return 0

        state = self.db.get_rsn_tier_reward_state(conn, rsn, for_update=True)
        previously_rewarded_rank = int(state["highest_rewarded_tier_rank"] or 0)
        if current_tier.rank <= previously_rewarded_rank:
            return 0

        tiers_crossed = current_tier.rank - previously_rewarded_rank
        rerolls_to_award = tiers_crossed * 3

        profile = self.db.get_user_task_profile(conn, discord_user_id, for_update=True)
        rerolls_available = int(profile["rerolls_available"] or 0) + rerolls_to_award
        self.db.update_user_task_profile(
            conn,
            discord_user_id,
            rerolls_available=rerolls_available,
        )
        self.db.update_rsn_tier_reward_state(
            conn,
            rsn,
            highest_rewarded_tier_rank=current_tier.rank,
            highest_rewarded_tier_label=current_tier.label,
        )
        return rerolls_to_award

    def _get_user_profile_snapshot(self, conn, discord_user_id: str) -> tuple[int, int]:
        profile = self.db.get_user_task_profile(conn, discord_user_id)
        rerolls_available = int(profile["rerolls_available"] or 0)
        reward_batches = int(profile["completion_reward_batches"] or 0)
        return rerolls_available, reward_batches

    def _get_or_assign_active_task_with_conn(
        self,
        conn,
        discord_user_id: str,
        rsn_override: str | None = None,
    ) -> ActiveTaskAssignment | None:
        awarded_rerolls, _, _ = self._award_due_rerolls(conn, discord_user_id)
        if awarded_rerolls > 0:
            conn.commit()
        rerolls_available, _reward_batches = self._get_user_profile_snapshot(conn, discord_user_id)

        rsn = (rsn_override or "").strip()
        if not rsn:
            rsn = self.db.get_rsn_for_discord_user(conn, discord_user_id) or ""
        if not rsn:
            return None

        latest_scan_id = self.db.get_latest_completed_scan_run_id(conn)
        current_tier = self._get_current_tier_threshold(conn, rsn)
        current_tier_rank = current_tier.rank if current_tier is not None else 0
        active = self.db.get_active_task(conn, discord_user_id, rsn)
        if active is not None:
            active_rsn = str(active["rsn"]).strip()
            active_task_id = int(active["task_id"])
            completion_state = self.db.get_task_completion_state(conn, active_rsn, active_task_id)
            active_incomplete_ids = self.db.get_incomplete_task_ids(conn, active_rsn)
            claimed_ids = self.db.get_claimed_task_ids_for_scan(
                conn,
                discord_user_id=discord_user_id,
                rsn=active_rsn,
                scan_run_id=latest_scan_id,
            )
            eligible_ids = compute_eligible_task_ids(
                active_incomplete_ids,
                claimed_ids,
            )
            eligible_ids = self._filter_assignable_task_ids(
                conn,
                eligible_ids,
                incomplete_ids=active_incomplete_ids,
                current_tier_rank=current_tier_rank,
            )
            if (
                active_rsn.casefold() == rsn.casefold()
                and completion_state is False
                and active_task_id in set(eligible_ids)
            ):
                return ActiveTaskAssignment(
                    rsn=active_rsn,
                    task=self._build_task_result(conn, active_task_id, len(active_incomplete_ids)),
                    reused_existing=True,
                    rerolls_remaining=rerolls_available,
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
        eligible_ids = self._filter_assignable_task_ids(
            conn,
            compute_eligible_task_ids(incomplete_ids, claimed_ids),
            incomplete_ids=incomplete_ids,
            current_tier_rank=current_tier_rank,
        )
        if not eligible_ids:
            return None

        task_id = self._pick_weighted_task_id(conn, eligible_ids)
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
            task=self._build_task_result(conn, task_id, len(incomplete_ids)),
            reused_existing=False,
            rerolls_remaining=rerolls_available,
        )

    def get_or_assign_active_task(self, discord_user_id: str, rsn_override: str | None = None) -> ActiveTaskAssignment | None:
        with self.db.connection() as conn:
            return self._get_or_assign_active_task_with_conn(conn, discord_user_id, rsn_override)

    def resolve_rsn_for_discord_user(self, discord_user_id: str) -> str | None:
        with self.db.connection() as conn:
            return self.db.get_rsn_for_discord_user(conn, discord_user_id)

    def resolve_rsns_for_discord_user(self, discord_user_id: str) -> list[str]:
        with self.db.connection() as conn:
            return self.db.get_rsns_for_discord_user(conn, discord_user_id)

    def has_active_incomplete_task(self, discord_user_id: str, rsn: str) -> bool:
        normalized_user_id = str(discord_user_id).strip()
        normalized_rsn = str(rsn).strip()
        if not normalized_user_id or not normalized_rsn:
            return False
        with self.db.connection() as conn:
            active = self.db.get_active_task(conn, normalized_user_id, normalized_rsn)
            if active is None:
                return False
            task_id = int(active["task_id"])
            completion_state = self.db.get_task_completion_state(conn, normalized_rsn, task_id)
            return completion_state is False

    def get_user_task_profile_summary(self, discord_user_id: str) -> UserTaskProfileSummary:
        with self.db.connection() as conn:
            awarded_rerolls, _, _ = self._award_due_rerolls(conn, discord_user_id)
            if awarded_rerolls > 0:
                conn.commit()
            rerolls_available, _reward_batches = self._get_user_profile_snapshot(conn, discord_user_id)
            active_tasks = [
                ActiveTaskSummary(
                    rsn=str(row["rsn"]).strip(),
                    task=self._build_task_result(
                        conn,
                        int(row["task_id"]),
                        eligible_count=len(self.db.get_incomplete_task_ids(conn, str(row["rsn"]).strip())),
                    ),
                )
                for row in self.db.get_active_tasks(conn, discord_user_id)
            ]
            verified_counts_by_rsn = self.db.get_verified_task_claim_counts_by_rsn(conn, discord_user_id)

            account_order: list[str] = []
            seen_rsns: set[str] = set()
            for rsn in self.db.get_rsns_for_discord_user(conn, discord_user_id):
                key = rsn.casefold()
                if key in seen_rsns:
                    continue
                seen_rsns.add(key)
                account_order.append(rsn)
            for rsn in verified_counts_by_rsn:
                key = rsn.casefold()
                if key in seen_rsns:
                    continue
                seen_rsns.add(key)
                account_order.append(rsn)
            for active in active_tasks:
                key = active.rsn.casefold()
                if key in seen_rsns:
                    continue
                seen_rsns.add(key)
                account_order.append(active.rsn)

            catalog_points_by_tier = self.db.get_catalog_point_totals_by_tier(conn)
            total_points_available = sum(int(points or 0) for points in catalog_points_by_tier.values())
            tier_thresholds = self._get_catalog_tier_thresholds(conn)

            completed_tasks_by_account: list[AccountCompletedTasksSummary] = []
            for rsn in account_order:
                progress = self.db.get_progress_summary(conn, rsn)
                completed_ca_tasks = int(progress["completed_count"] or 0)
                incomplete_ca_tasks = int(progress["incomplete_count"] or 0)
                total_ca_tasks = completed_ca_tasks + incomplete_ca_tasks
                total_points = int(progress["total_points"] or 0)
                rank_label = "Unranked"
                for threshold in tier_thresholds:
                    if total_points >= int(threshold.required_points):
                        rank_label = threshold.label
                    else:
                        break

                completed_tasks_by_account.append(
                    AccountCompletedTasksSummary(
                        rsn=rsn,
                        rank_label=rank_label,
                        completed_tasks=int(verified_counts_by_rsn.get(rsn, 0) or 0),
                        completed_ca_tasks=completed_ca_tasks,
                        total_ca_tasks=total_ca_tasks,
                        total_points=total_points,
                        total_points_available=total_points_available,
                    )
                )

            return UserTaskProfileSummary(
                discord_user_id=discord_user_id,
                rerolls_available=rerolls_available,
                completed_tasks_by_account=completed_tasks_by_account,
                active_tasks=active_tasks,
            )

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
    ) -> CompletionResult | None:
        active_rsn: str
        task_id: int
        reward_key: str | None = None
        reward_status: str | None = None
        awarded_rerolls = 0

        with self.db.connection() as conn:
            active = self.db.get_active_task(conn, discord_user_id, rsn)
            if active is None:
                return None
            active_rsn = str(active["rsn"]).strip()
            task_id = int(active["task_id"])

        live_verification_attempted = False
        live_verified = False
        try:
            payload = self.fetch_runelite_payload(active_rsn)
            live_verification_attempted = True
            completed_ids = extract_completed_ca_task_ids(payload)
            live_verified = task_id in completed_ids
        except Exception:
            LOGGER.exception(
                "Live verification failed for user %s rsn %s task %s",
                discord_user_id,
                active_rsn,
                task_id,
            )

        if live_verified:
            with self.db.connection() as conn:
                try:
                    latest_scan_id = self.db.get_latest_completed_scan_run_id(conn)
                    self.db.upsert_single_progress(
                        conn,
                        rsn=active_rsn,
                        task_id=task_id,
                        is_complete=True,
                        source="verify",
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
                    previous_status, claim_status = self.db.mark_task_verified(
                        conn,
                        discord_user_id=discord_user_id,
                        rsn=active_rsn,
                        task_id=task_id,
                        is_complete=True,
                        verified_scan_run_id=latest_scan_id,
                    )
                    reward = self.db.ensure_reward_for_claim(conn, discord_user_id, active_rsn, task_id)
                    reward = self.db.update_reward_status_for_claim(
                        conn,
                        discord_user_id,
                        active_rsn,
                        task_id,
                        status="ready",
                    ) or reward
                    reward_key = str(reward.get("reward_key") or "").strip() or None
                    reward_status = str(reward.get("status") or "").strip() or None
                    self.db.clear_active_task(conn, discord_user_id, active_rsn)

                    if claim_status == "verified_complete" and previous_status != "verified_complete":
                        completion_bonus, _, _ = self._award_due_rerolls(conn, discord_user_id)
                        boss_bonus = self._award_boss_completion_reroll_if_due(
                            conn,
                            discord_user_id=discord_user_id,
                            rsn=active_rsn,
                            task_id=task_id,
                        )
                        tier_bonus = self._award_tier_promotion_rerolls_if_due(
                            conn,
                            discord_user_id=discord_user_id,
                            rsn=active_rsn,
                        )
                        awarded_rerolls = completion_bonus + boss_bonus + tier_bonus

                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

        with self.db.connection() as conn:
            awarded_rerolls_snapshot, _, _ = self._award_due_rerolls(conn, discord_user_id)
            if awarded_rerolls_snapshot > 0:
                conn.commit()
            rerolls_available, _reward_batches = self._get_user_profile_snapshot(conn, discord_user_id)
            verified_assigned_completions = self.db.count_verified_task_claims(conn, discord_user_id)
            remaining_count = self.db.get_progress_summary(conn, active_rsn)["incomplete_count"]
            if not live_verified:
                active = self.db.get_active_task(conn, discord_user_id, active_rsn)
                if active is not None:
                    task_id = int(active["task_id"])
            task = self._build_task_result(conn, task_id, remaining_count)
            reward = self.db.get_reward_for_claim(conn, discord_user_id, active_rsn, task_id)
            if reward is not None:
                reward_key = str(reward.get("reward_key") or "").strip() or reward_key
                reward_status = str(reward.get("status") or "").strip() or reward_status

        return CompletionResult(
            rsn=active_rsn,
            task=task,
            reward_key=reward_key,
            reward_status=reward_status,
            rerolls_remaining=rerolls_available,
            awarded_rerolls=awarded_rerolls,
            verified_assigned_completions=verified_assigned_completions,
            live_verification_attempted=live_verification_attempted,
            live_verified=live_verified,
        )

    def _reroll_active_task_with_conn(
        self,
        conn,
        discord_user_id: str,
        rsn: str,
        *,
        consume_reroll: bool = True,
    ) -> RerollResult | None:
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
        current_tier = self._get_current_tier_threshold(conn, rsn)
        current_tier_rank = current_tier.rank if current_tier is not None else 0
        incomplete_ids = self.db.get_incomplete_task_ids(conn, rsn)
        claimed_ids = self.db.get_claimed_task_ids_for_scan(
            conn,
            discord_user_id=discord_user_id,
            rsn=rsn,
            scan_run_id=latest_scan_id,
        )
        eligible_ids = self._filter_assignable_task_ids(
            conn,
            compute_eligible_task_ids(incomplete_ids, claimed_ids),
            incomplete_ids=incomplete_ids,
            current_tier_rank=current_tier_rank,
        )
        current_task = self._build_task_result(conn, task_id, len(incomplete_ids))
        metadata_by_id = self.db.get_task_metadata_for_ids(conn, sorted(set(eligible_ids) | {task_id}))
        candidates = filter_reroll_candidate_ids(
            task_id,
            eligible_ids,
            metadata_by_id,
        )
        awarded_rerolls, _, _ = self._award_due_rerolls(conn, discord_user_id)
        if awarded_rerolls > 0:
            conn.commit()
        rerolls_available, _reward_batches = self._get_user_profile_snapshot(conn, discord_user_id)

        if not candidates:
            return RerollResult(
                rsn=rsn,
                previous_task=current_task,
                replacement_task=None,
                rerolls_remaining=rerolls_available,
            )

        if consume_reroll:
            profile = self.db.get_user_task_profile(conn, discord_user_id, for_update=True)
            rerolls_available = int(profile["rerolls_available"] or 0)
            if rerolls_available <= 0:
                return RerollResult(
                    rsn=rsn,
                    previous_task=current_task,
                    replacement_task=None,
                    rerolls_remaining=0,
                )

        next_task_id = self._pick_weighted_task_id(conn, candidates)
        self.db.upsert_active_task(
            conn,
            discord_user_id=discord_user_id,
            rsn=rsn,
            task_id=next_task_id,
            assigned_scan_run_id=latest_scan_id,
        )
        if consume_reroll:
            rerolls_available -= 1
            self.db.update_user_task_profile(
                conn,
                discord_user_id,
                rerolls_available=rerolls_available,
            )
        conn.commit()

        replacement = self._build_task_result(conn, next_task_id, len(incomplete_ids))
        return RerollResult(
            rsn=rsn,
            previous_task=current_task,
            replacement_task=replacement,
            rerolls_remaining=rerolls_available,
        )

    def reroll_active_task(self, discord_user_id: str, rsn: str) -> RerollResult | None:
        with self.db.connection() as conn:
            return self._reroll_active_task_with_conn(conn, discord_user_id, rsn, consume_reroll=True)

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
                previous_status, claim_status = self.db.mark_task_verified(
                    conn,
                    discord_user_id=discord_user_id,
                    rsn=rsn,
                    task_id=task_id,
                    is_complete=is_complete,
                    verified_scan_run_id=latest_scan_id,
                )
                if claim_status == "verified_complete":
                    self.db.update_reward_status_for_claim(
                        conn,
                        discord_user_id,
                        rsn,
                        task_id,
                        status="ready",
                    )
                elif claim_status == "corrected_incomplete":
                    self.db.update_reward_status_for_claim(
                        conn,
                        discord_user_id,
                        rsn,
                        task_id,
                        status="cancelled",
                    )
                if claim_status == "verified_complete" and previous_status != "verified_complete":
                    self._award_due_rerolls(conn, discord_user_id)
                    self._award_boss_completion_reroll_if_due(
                        conn,
                        discord_user_id=discord_user_id,
                        rsn=rsn,
                        task_id=task_id,
                    )
                    self._award_tier_promotion_rerolls_if_due(
                        conn,
                        discord_user_id=discord_user_id,
                        rsn=rsn,
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
