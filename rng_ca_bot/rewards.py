from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from .db import Database

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RewardTier:
    key: str
    label: str
    weight: int
    card_class: str
    accent: str


@dataclass(frozen=True, slots=True)
class RewardEntry:
    kind: str
    label: str
    amount: int | None = None
    quantity: int | None = None
    image_url: str | None = None
    weight: int = 1


REWARD_TIERS: tuple[RewardTier, ...] = (
    RewardTier("white", "White", 650, "white", "#d9d9d9"),
    RewardTier("blue", "Blue", 220, "blue", "#4ea6ff"),
    RewardTier("purple", "Purple", 90, "purple", "#b16cff"),
    RewardTier("red", "Red", 30, "red", "#ff5f5f"),
    RewardTier("gold", "Gold", 10, "gold", "#ffcf5a"),
)
_RAND = random.SystemRandom()
REEL_LENGTH = 115
REEL_STOP_INDEX = 96

# Edit these pools whenever you want to swap in real item rewards.
# `kind` can be `gp` or `item`. Tier odds are controlled above; these are the rolls within each tier.
# `image_url` is optional and can be added per reward whenever you have a small icon ready.
REWARD_POOL_BY_TIER: dict[str, tuple[RewardEntry, ...]] = {
    "white": (
        RewardEntry(kind="gp", label="100k GP", amount=100_000, weight=30),
        RewardEntry(kind="gp", label="150k GP", amount=150_000, weight=22),
        RewardEntry(kind="gp", label="250k GP", amount=250_000, weight=12),
        RewardEntry(kind="item", label="Cannonballs", quantity=1500, weight=10),
        RewardEntry(kind="item", label="Dragon bones", quantity=100, weight=8),
        RewardEntry(kind="item", label="Rune platebody", weight=5),
        RewardEntry(kind="item", label="Dragon dagger(p++)", weight=5),
    ),
    "blue": (
        RewardEntry(kind="gp", label="1m GP", amount=1_000_000, weight=28),
        RewardEntry(kind="gp", label="2.5m GP", amount=2_500_000, weight=16),
        RewardEntry(kind="item", label="Abyssal whip", weight=10),
        RewardEntry(kind="item", label="Dragon boots", weight=9),
        RewardEntry(kind="item", label="Amulet of fury", weight=7),
        RewardEntry(kind="item", label="Cannonballs", quantity=5000, weight=6),
    ),
    "purple": (
        RewardEntry(kind="gp", label="10m GP", amount=10_000_000, weight=26),
        RewardEntry(kind="gp", label="15m GP", amount=15_000_000, weight=14),
        RewardEntry(kind="item", label="Toxic blowpipe", weight=10),
        RewardEntry(kind="item", label="Zenyte shard", weight=8),
        RewardEntry(kind="item", label="Amulet of torture", weight=6),
        RewardEntry(kind="item", label="Pegasian crystal", weight=5),
    ),
    "red": (
        RewardEntry(kind="gp", label="100m GP", amount=100_000_000, weight=24),
        RewardEntry(kind="gp", label="125m GP", amount=125_000_000, weight=10),
        RewardEntry(kind="item", label="Bow of faerdhinen", weight=8),
        RewardEntry(kind="item", label="Ancestral robe top", weight=6),
        RewardEntry(kind="item", label="Masori body", weight=5),
        RewardEntry(kind="item", label="Voidwaker", weight=4),
    ),
    "gold": (
        RewardEntry(kind="gp", label="500m GP", amount=500_000_000, weight=18),
        RewardEntry(kind="item", label="Twisted bow", weight=6),
        RewardEntry(kind="item", label="Tumeken's shadow", weight=5),
        RewardEntry(kind="item", label="Scythe of vitur", weight=4),
    ),
}


def format_gp(amount: int) -> str:
    if amount >= 1_000_000_000:
        return f"{amount / 1_000_000_000:.1f}b GP"
    if amount >= 1_000_000:
        value = amount / 1_000_000
        return f"{value:.0f}m GP" if amount % 1_000_000 == 0 else f"{value:.1f}m GP"
    if amount >= 1_000:
        value = amount / 1_000
        return f"{value:.0f}k GP" if amount % 1_000 == 0 else f"{value:.1f}k GP"
    return f"{amount} GP"


def format_reward_display(*, kind: str, label: str, amount: int | None, quantity: int | None) -> str:
    if kind == "gp" and amount:
        return format_gp(amount)

    clean_label = label.strip() or "Reward"
    if quantity is not None and quantity > 1:
        return f"{quantity} x {clean_label}"
    return clean_label


def pick_reward_tier() -> RewardTier:
    total_weight = sum(tier.weight for tier in REWARD_TIERS)
    target = _RAND.randint(1, total_weight)
    running_total = 0
    for tier in REWARD_TIERS:
        running_total += tier.weight
        if target <= running_total:
            return tier
    return REWARD_TIERS[-1]


def _pick_reward_entry(tier_key: str) -> RewardEntry:
    pool = REWARD_POOL_BY_TIER.get(tier_key, ())
    if not pool:
        raise ValueError(f"No reward pool configured for tier {tier_key!r}")

    total_weight = sum(max(int(entry.weight), 0) for entry in pool)
    if total_weight <= 0:
        return pool[0]

    target = _RAND.randint(1, total_weight)
    running_total = 0
    for entry in pool:
        running_total += max(int(entry.weight), 0)
        if target <= running_total:
            return entry
    return pool[-1]


def reward_payload(tier: RewardTier, entry: RewardEntry) -> dict[str, Any]:
    display_value = format_reward_display(
        kind=entry.kind,
        label=entry.label,
        amount=entry.amount,
        quantity=entry.quantity,
    )
    return {
        "tier": tier.key,
        "tier_label": tier.label,
        "kind": entry.kind,
        "label": entry.label,
        "amount": entry.amount,
        "quantity": entry.quantity,
        "image_url": entry.image_url,
        "display_value": display_value,
        "display_amount": display_value,
        "card_class": tier.card_class,
        "accent": tier.accent,
    }


def build_reel(final_tier: RewardTier, final_entry: RewardEntry) -> tuple[list[dict[str, Any]], int]:
    reel: list[dict[str, Any]] = []
    for index in range(REEL_LENGTH):
        if index == REEL_STOP_INDEX:
            tier = final_tier
            entry = final_entry
        else:
            tier = pick_reward_tier()
            entry = _pick_reward_entry(tier.key)
        reel.append(reward_payload(tier, entry))
    return reel, REEL_STOP_INDEX


def _reward_payload_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    tier_key = str(row.get("reward_tier") or "").strip().casefold()
    tier = next((item for item in REWARD_TIERS if item.key == tier_key), None)
    reward_label = str(row.get("reward_label") or "").strip()
    amount = int(row.get("reward_amount") or 0)
    quantity = row.get("reward_quantity")
    image_url = str(row.get("reward_image_url") or "").strip() or None
    kind = str(row.get("reward_kind") or "").strip().casefold() or ("gp" if amount else "item")
    display_value = format_reward_display(
        kind=kind,
        label=reward_label,
        amount=amount or None,
        quantity=int(quantity) if quantity is not None else None,
    )
    if tier is not None:
        return {
            "tier": tier.key,
            "tier_label": tier.label,
            "kind": kind,
            "label": reward_label or display_value,
            "amount": amount or None,
            "quantity": int(quantity) if quantity is not None else None,
            "image_url": image_url,
            "display_value": display_value,
            "display_amount": display_value,
            "card_class": tier.card_class,
            "accent": tier.accent,
        }

    return {
        "tier": tier_key or "used",
        "tier_label": tier_key.title() if tier_key else "Used",
        "kind": kind,
        "label": reward_label or display_value,
        "amount": amount or None,
        "quantity": int(quantity) if quantity is not None else None,
        "image_url": image_url,
        "display_value": display_value,
        "display_amount": display_value,
        "card_class": tier_key or "white",
        "accent": "#d9d9d9",
    }


def redeem_reward_key_payload(db: Database, reward_key: str) -> tuple[int, dict[str, Any]]:
    normalized_key = reward_key.strip().upper()
    if not normalized_key:
        return HTTPStatus.BAD_REQUEST, {"status": "invalid", "message": "Enter a reward key first."}

    with db.connection() as conn:
        try:
            reward = db.get_reward_by_key(conn, normalized_key, for_update=True)
            if reward is None:
                conn.rollback()
                return HTTPStatus.NOT_FOUND, {"status": "invalid", "message": "That reward key does not exist."}

            current_status = str(reward.get("status") or "").strip()
            if current_status == "pending_verification":
                conn.rollback()
                return (
                    HTTPStatus.CONFLICT,
                    {
                        "status": "pending",
                        "message": "That reward key exists, but the task has not been verified yet.",
                        "reward_key": normalized_key,
                    },
                )
            if current_status == "cancelled":
                conn.rollback()
                return (
                    HTTPStatus.GONE,
                    {
                        "status": "cancelled",
                        "message": "That reward key was cancelled because the linked task did not verify.",
                        "reward_key": normalized_key,
                    },
                )
            if current_status == "redeemed":
                conn.rollback()
                return (
                    HTTPStatus.CONFLICT,
                    {
                        "status": "used",
                        "message": "That reward key has already been redeemed.",
                        "reward_key": normalized_key,
                        "reward": _reward_payload_from_row(reward),
                    },
                )

            outcome_tier = pick_reward_tier()
            outcome_entry = _pick_reward_entry(outcome_tier.key)
            db.redeem_reward_key(
                conn,
                normalized_key,
                reward_tier=outcome_tier.key,
                reward_kind=outcome_entry.kind,
                reward_label=outcome_entry.label,
                reward_amount=outcome_entry.amount,
                reward_quantity=outcome_entry.quantity,
                reward_image_url=outcome_entry.image_url,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    reel, selected_index = build_reel(outcome_tier, outcome_entry)
    LOGGER.info(
        "Reward key redeemed key=%s tier=%s kind=%s label=%s amount=%s quantity=%s image_url=%s",
        normalized_key,
        outcome_tier.key,
        outcome_entry.kind,
        outcome_entry.label,
        outcome_entry.amount,
        outcome_entry.quantity,
        outcome_entry.image_url,
    )
    return (
        HTTPStatus.OK,
        {
            "status": "ok",
            "message": "Reward redeemed successfully.",
            "reward_key": normalized_key,
            "reward": reward_payload(outcome_tier, outcome_entry),
            "reel": reel,
            "selected_index": selected_index,
        },
    )
