from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any, Mapping

from .sync_service import RandomTaskResult

if TYPE_CHECKING:
    from .sync_service import SyncService

_RAND = random.SystemRandom()
TASK_REEL_LENGTH = 115
TASK_REEL_STOP_INDEX = 96


def _tier_card_class(tier_label: str | None) -> str:
    tier = (tier_label or "").strip().casefold()
    if tier == "easy":
        return "white"
    if tier == "medium":
        return "blue"
    if tier == "hard":
        return "purple"
    if tier == "elite":
        return "red"
    if tier in {"master", "grandmaster"}:
        return "gold"
    return "white"


def task_payload(task: RandomTaskResult, *, rerolls_remaining: int | None = None) -> dict[str, Any]:
    points = int(task.points) if task.points is not None else None
    tier_label = (task.tier_label or "Unknown").strip() or "Unknown"
    subtitle = f"{tier_label} | {points if points is not None else '-'} pts"
    payload: dict[str, Any] = {
        "task_id": int(task.task_id),
        "tier": tier_label.casefold(),
        "tier_label": tier_label,
        "kind": "task",
        "label": task.task_name,
        "display_value": task.task_name,
        "display_amount": task.task_name,
        "card_class": _tier_card_class(task.tier_label),
        "accent": "#e87126",
        "image_url": task.npc_image_url,
        "points": points,
        "npc": task.npc,
        "task_type": task.task_type,
        "description": subtitle,
        "subtitle": subtitle,
        "npc_url": task.npc_url,
    }
    if rerolls_remaining is not None:
        payload["rerolls_remaining"] = max(int(rerolls_remaining), 0)
    return payload


def _task_from_metadata_row(row: Mapping[str, object]) -> RandomTaskResult:
    points_raw = row.get("points")
    return RandomTaskResult(
        task_id=int(row.get("task_id") or 0),
        task_name=str(row.get("task_name") or "").strip() or "Unknown Task",
        task_description=str(row.get("description") or "").strip() or None,
        npc=str(row.get("npc") or "").strip() or None,
        npc_url=str(row.get("npc_url") or "").strip() or None,
        npc_image_url=str(row.get("npc_image_url") or "").strip() or None,
        task_type=str(row.get("task_type") or "").strip() or None,
        tier_label=str(row.get("tier_label") or "").strip() or None,
        points=int(points_raw) if points_raw is not None else None,
        eligible_count=0,
    )


def build_task_reel(sync_service: "SyncService", final_task: RandomTaskResult) -> tuple[list[dict[str, Any]], int]:
    with sync_service.db.connection() as conn:
        catalog_ids = sync_service.db.get_catalog_task_ids(conn)
        if not catalog_ids:
            filler = [task_payload(final_task) for _ in range(TASK_REEL_LENGTH)]
            return filler, min(TASK_REEL_STOP_INDEX, len(filler) - 1)

        sampled_ids = [_RAND.choice(catalog_ids) for _ in range(TASK_REEL_LENGTH)]
        stop_index = min(TASK_REEL_STOP_INDEX, TASK_REEL_LENGTH - 1)
        sampled_ids[stop_index] = int(final_task.task_id)
        metadata = sync_service.db.get_task_metadata_for_ids(conn, sorted(set(sampled_ids)))

    reel: list[dict[str, Any]] = []
    for index, task_id in enumerate(sampled_ids):
        if index == stop_index:
            task = final_task
        else:
            row = metadata.get(int(task_id))
            task = _task_from_metadata_row(row) if row is not None else final_task
        reel.append(task_payload(task))
    return reel, stop_index
