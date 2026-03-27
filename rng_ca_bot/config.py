from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv

DEFAULT_TIER_ASSIGNMENT_WEIGHTS: dict[str, int] = {
    "easy": 12,
    "medium": 9,
    "hard": 6,
    "elite": 4,
    "master": 2,
    "grandmaster": 1,
}


@dataclass(slots=True)
class Settings:
    discord_token: str
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    scan_cron: str
    timezone: str
    user_source_sql: str
    runelite_account_type: str
    http_user_agent: str
    task_panel_channel_id: int | None
    highscores_panel_channel_id: int | None
    reward_payouts_channel_id: int | None
    log_level: str
    log_file: str | None
    tier_assignment_weights: dict[str, int]
    reward_keys_require_verification: bool
    reward_admin_api_key: str | None

    @property
    def db_settings(self) -> dict[str, Any]:
        return {
            "host": self.db_host,
            "port": self.db_port,
            "user": self.db_user,
            "password": self.db_password,
            "database": self.db_name,
            "autocommit": False,
        }


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    normalized = raw.strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be one of true/false, yes/no, on/off, or 1/0")


def _parse_tier_assignment_weights(raw: str | None) -> dict[str, int]:
    weights = dict(DEFAULT_TIER_ASSIGNMENT_WEIGHTS)
    if raw is None or raw.strip() == "":
        return weights

    for entry in raw.split(","):
        item = entry.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(
                "TIER_ASSIGNMENT_WEIGHTS entries must look like 'easy=12,medium=9,...'",
            )
        tier_label, weight_text = item.split("=", 1)
        tier_key = tier_label.strip().casefold()
        if tier_key not in DEFAULT_TIER_ASSIGNMENT_WEIGHTS:
            raise ValueError(f"Unknown tier in TIER_ASSIGNMENT_WEIGHTS: {tier_label!r}")
        try:
            weight_value = int(weight_text.strip())
        except ValueError as exc:
            raise ValueError(
                f"Invalid weight for tier {tier_label!r}: {weight_text!r}",
            ) from exc
        if weight_value < 0:
            raise ValueError(f"Tier weight must be >= 0 for {tier_label!r}")
        weights[tier_key] = weight_value

    return weights


def load_settings() -> Settings:
    load_dotenv()

    discord_token = os.getenv("DISCORD_TOKEN", "")
    if not discord_token:
        # Allow tests and non-discord tasks to run without token.
        discord_token = ""

    panel_channel_raw = os.getenv("DISCORD_TASK_PANEL_CHANNEL_ID", "").strip()
    task_panel_channel_id = int(panel_channel_raw) if panel_channel_raw else None
    highscores_channel_raw = os.getenv("DISCORD_HIGHSCORES_CHANNEL_ID", "").strip()
    highscores_panel_channel_id = int(highscores_channel_raw) if highscores_channel_raw else None
    reward_payouts_channel_raw = os.getenv("DISCORD_REWARD_PAYOUTS_CHANNEL_ID", "").strip()
    if not reward_payouts_channel_raw:
        reward_payouts_channel_raw = os.getenv("DISCORD_REWARDS_PAYOUTS_CHANNEL_ID", "").strip()
    reward_payouts_channel_id = int(reward_payouts_channel_raw) if reward_payouts_channel_raw else None
    log_file_raw = os.getenv("LOG_FILE", "logs/rng_ca_bot.log").strip()

    return Settings(
        discord_token=discord_token,
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=_get_int("DB_PORT", 3306),
        db_user=os.getenv("DB_USER", "rnguser"),
        db_password=os.getenv("DB_PASSWORD", ""),
        db_name=os.getenv("DB_NAME", "rngstreet"),
        scan_cron=os.getenv("SCAN_CRON", "0 3 * * *"),
        timezone=os.getenv("TIMEZONE", "UTC"),
        user_source_sql=os.getenv("USER_SOURCE_SQL", ""),
        runelite_account_type=os.getenv("RUNELITE_ACCOUNT_TYPE", "STANDARD"),
        http_user_agent=os.getenv("HTTP_USER_AGENT", "RNG-CA-Bot/1.0"),
        task_panel_channel_id=task_panel_channel_id,
        highscores_panel_channel_id=highscores_panel_channel_id,
        reward_payouts_channel_id=reward_payouts_channel_id,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        log_file=log_file_raw or None,
        tier_assignment_weights=_parse_tier_assignment_weights(os.getenv("TIER_ASSIGNMENT_WEIGHTS")),
        reward_keys_require_verification=_get_bool("REWARD_KEYS_REQUIRE_VERIFICATION", True),
        reward_admin_api_key=os.getenv("REWARD_ADMIN_API_KEY", "").strip() or None,
    )
