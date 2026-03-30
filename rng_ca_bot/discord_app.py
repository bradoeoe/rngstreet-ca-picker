from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import discord
from discord import app_commands
from discord.ext import commands

from .config import Settings
from .db import ScanRunResult
from .sync_service import (
    ActiveTaskAssignment,
    ActiveTaskSummary,
    BossImageMapping,
    CompletionResult,
    HighscoresSummary,
    RewardKeyStatusEntry,
    RewardPayoutEntry,
    RewardPayoutSummary,
    RandomTaskResult,
    RerollResult,
    SyncService,
    TaskRollKeyIssue,
    UserRewardKeySummary,
    UserTaskProfileSummary,
)

LOGGER = logging.getLogger(__name__)

PANEL_KEY = "global_task_panel"
HIGHSCORES_PANEL_KEY = "global_highscores_panel"
GET_TASK_CUSTOM_ID = "rngca:panel:get_task"
GET_FUN_TASK_CUSTOM_ID = "rngca:panel:get_fun_task"
REROLL_CUSTOM_ID = "rngca:panel:reroll"
COMPLETE_TASK_CUSTOM_ID = "rngca:panel:complete_task"
PROFILE_CUSTOM_ID = "rngca:panel:profile"
PROFILE_REWARDS_CUSTOM_ID = "rngca:profile:rewards"
MONTHLY_HIGHSCORES_CUSTOM_ID = "rngca:highscores:monthly"
ALL_TIME_HIGHSCORES_CUSTOM_ID = "rngca:highscores:all_time"
OVERALL_TIER_LEADERS_CUSTOM_ID = "rngca:highscores:tier_leaders"
ADMIN_SET_REROLLS_CUSTOM_ID = "rngca:admin:set_rerolls"
ADMIN_ADD_REROLLS_CUSTOM_ID = "rngca:admin:add_rerolls"
ADMIN_GIVE_REWARD_KEY_CUSTOM_ID = "rngca:admin:give_reward_key"
ADMIN_RESET_PENDING_CUSTOM_ID = "rngca:admin:reset_pending"
ADMIN_CLEAR_ACTIVE_CUSTOM_ID = "rngca:admin:clear_active"
ADMIN_PAYOUTS_CUSTOM_ID = "rngca:admin:payouts"
ADMIN_REFRESH_PANELS_CUSTOM_ID = "rngca:admin:refresh_panels"
PAYOUTS_REFRESH_CUSTOM_ID = "rngca:payouts:refresh"
PAYOUTS_MARK_PAID_CUSTOM_ID = "rngca:payouts:mark_paid"
PAYOUTS_UNDO_PAID_CUSTOM_ID = "rngca:payouts:undo_paid"

ACTION_GET = "get_task"
ACTION_FUN = "get_fun_task"
ACTION_REROLL = "reroll"
ACTION_COMPLETE = "complete_task"
ACTION_PROFILE = "profile"
HIGHSCORES_MODE_MONTHLY = "monthly"
HIGHSCORES_MODE_ALL_TIME = "all_time"
HIGHSCORES_MODE_TIER_LEADERS = "tier_leaders"
HIGHSCORES_PAGE_SIZE = 20
DISCORD_USER_MENTION_PATTERN = re.compile(r"^<@!?(\d+)>$")
PROFILE_ICON_IMAGE_URL = (
    "https://oldschool.runescape.wiki/w/Special:Redirect/file/"
    "Vampyric_slayer_helmet_detail.png"
)
PROFILE_BANNER_IMAGE_URL = "https://cdn.displate.com/artwork/1200x857/2025-11-25/c22bef74-5c80-4c40-a08b-f6721a207f6a.jpg"
PANEL_ICON_IMAGE_URL = "https://oldschool.runescape.wiki/images/Tzkal_slayer_helmet_chathead.png"
PANEL_BANNER_IMAGE_URL = "https://i.redd.it/vobh86y0aopz.jpg"
HIGHSCORES_ICON_IMAGE_URL = "https://oldschool.runescape.wiki/images/Ghommal%27s_lucky_penny_detail.png?75281"
HIGHSCORES_BANNER_IMAGE_URL = (
    "https://www.reddit.com/media?url=https%3A%2F%2Fpreview.redd.it%2F"
    "the-post-2021-decline-of-osrs-promo-art-quality-v0-vmv9v79akhme1.jpg%3F"
    "width%3D1080%26crop%3Dsmart%26auto%3Dwebp%26s%3D16120ca71ff408a8b50a478b3a8a56e8e074e704"
)
REWARDS_BANNER_IMAGE_URL = "https://i.servimg.com/u/f64/19/92/56/41/rs_art10.png"


@dataclass(slots=True)
class BotServices:
    sync_service: SyncService


def _truncate_text(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 3].rstrip() + "..."


def _discord_relative_timestamp(value: datetime | None) -> str:
    if value is None:
        return "unknown time"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    unix = int(value.timestamp())
    return f"<t:{unix}:R>"


def _compact_reward_key(reward_key: str) -> str:
    cleaned = reward_key.strip()
    if len(cleaned) <= 14:
        return cleaned
    return f"{cleaned[:10]}...{cleaned[-4:]}"


def _task_roll_mode_label(mode: str) -> str:
    normalized = (mode or "new").strip().casefold()
    if normalized == "reroll":
        return "Reroll active task"
    return "New task"


def _wiki_sync_incomplete_message(rsn: str) -> str:
    return (
        f"You haven't completed your task yet for *{rsn}*, "
        "if you have completed this task, make sure you open runelite with the WikiSync plugin running."
    )


def _build_task_roll_url(base_url: str, *, issued: TaskRollKeyIssue) -> str:
    fallback = "http://localhost:5173/?mode=task"
    candidate = (base_url or "").strip() or fallback
    try:
        parsed = urlsplit(candidate)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Task roll URL must include scheme and host")
    except Exception:
        parsed = urlsplit(fallback)

    query_pairs = parse_qsl(parsed.query, keep_blank_values=False)
    query: dict[str, str] = {key: value for key, value in query_pairs}
    query["mode"] = "task"
    query["task_roll_key"] = issued.roll_key
    encoded_query = urlencode(query)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, encoded_query, parsed.fragment))


def _build_reward_roll_url(base_url: str, *, reward_key: str) -> str:
    fallback = "http://localhost:5173/"
    candidate = (base_url or "").strip() or fallback
    try:
        parsed = urlsplit(candidate)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Reward URL must include scheme and host")
    except Exception:
        parsed = urlsplit(fallback)

    query_pairs = parse_qsl(parsed.query, keep_blank_values=False)
    query: dict[str, str] = {key: value for key, value in query_pairs}
    query["reward_key"] = reward_key.strip().upper()
    encoded_query = urlencode(query)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, encoded_query, parsed.fragment))


def _audit_log(interaction: discord.Interaction, event: str, **fields: object) -> None:
    extras = " ".join(
        f"{key}={value!r}"
        for key, value in fields.items()
        if value is not None and value != ""
    )
    LOGGER.info(
        "audit event=%s user_id=%s user=%r guild_id=%s channel_id=%s %s",
        event,
        getattr(interaction.user, "id", None),
        str(interaction.user),
        interaction.guild_id,
        interaction.channel_id,
        extras,
    )


def _parse_discord_user_id(value: str) -> str | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.isdigit():
        return cleaned
    match = DISCORD_USER_MENTION_PATTERN.fullmatch(cleaned)
    if match:
        return match.group(1)
    return None


def _normalize_optional_rsn(value: str) -> str | None:
    cleaned = value.strip()
    return cleaned or None


def _has_kick_members_permission(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        return False
    if isinstance(interaction.user, discord.Member):
        return bool(interaction.user.guild_permissions.kick_members)
    member = interaction.guild.get_member(interaction.user.id)
    return bool(member and member.guild_permissions.kick_members)


async def _ensure_kick_members_permission(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        message = "This admin action can only be used inside a server."
    elif _has_kick_members_permission(interaction):
        return True
    else:
        message = "You need **Kick Members** permission to use this admin action."

    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)
    return False


def _task_color(points: int | None, tier_label: str | None) -> discord.Color:
    if points is not None:
        by_points = {
            1: discord.Color.green(),
            2: discord.Color.from_rgb(88, 199, 76),
            3: discord.Color.gold(),
            4: discord.Color.orange(),
            5: discord.Color.from_rgb(255, 120, 40),
            6: discord.Color.red(),
        }
        if points in by_points:
            return by_points[points]

    tier = (tier_label or "").strip().casefold()
    if tier == "easy":
        return discord.Color.green()
    if tier == "medium":
        return discord.Color.gold()
    if tier == "hard":
        return discord.Color.orange()
    if tier == "elite":
        return discord.Color.red()
    if tier == "master":
        return discord.Color.purple()
    if tier == "grandmaster":
        return discord.Color.from_rgb(190, 80, 255)
    return discord.Color.blurple()


def _reward_status_label(status: str | None) -> str | None:
    normalized = (status or "").strip().casefold()
    if not normalized:
        return None
    if normalized == "ready":
        return "Ready to redeem"
    if normalized == "pending_verification":
        return "Verification required (WikiSync on PC)"
    if normalized == "redeemed":
        return "Redeemed"
    if normalized == "cancelled":
        return "Cancelled"
    return status


def _task_embed(
    rsn: str,
    task: RandomTaskResult,
    *,
    rerolls_remaining: int | None = None,
    reward_key: str | None = None,
    reward_status: str | None = None,
    reward_url: str | None = None,
) -> discord.Embed:
    embed = discord.Embed(
        title=task.task_name,
        description=_truncate_text(task.task_description or "No description available.", 4096),
        color=_task_color(task.points, task.tier_label),
        timestamp=datetime.now(UTC),
    )
    embed.add_field(name="Assigned", value=f"`{rsn}`", inline=True)
    embed.add_field(name="Tier", value=task.tier_label or "Unknown", inline=True)
    embed.add_field(name="Points", value=str(task.points) if task.points is not None else "-", inline=True)
    if task.npc:
        embed.add_field(name="NPC", value=task.npc, inline=True)
    if task.task_type:
        embed.add_field(name="Type", value=task.task_type, inline=True)
    embed.add_field(
        name="Tasks Left",
        value=f"**{max(task.eligible_count, 0)}**",
        inline=True,
    )
    if rerolls_remaining is not None:
        embed.add_field(name="Rerolls Remaining", value=f"**{max(int(rerolls_remaining), 0)}**", inline=True)
    useful_links: list[str] = []
    if task.npc_url:
        useful_links.append(f"[Boss Wiki]({task.npc_url})")
    if task.task_url:
        useful_links.append(f"[Task Wiki]({task.task_url})")
    if useful_links:
        embed.add_field(name="Useful links", value="\n".join(useful_links), inline=True)
    if reward_key:
        embed.add_field(name="Reward Key", value=f"`{reward_key}`", inline=False)
    if reward_url:
        embed.add_field(name="Open Reward Case", value=f"[Click to Redeem]({reward_url})", inline=False)
    if reward_status:
        embed.add_field(name="Reward Status", value=_reward_status_label(reward_status) or reward_status, inline=True)
    if task.npc_image_url:
        embed.set_thumbnail(url=task.npc_image_url)
    embed.set_footer(text=f"Combat Task Tracker • {rsn}")
    return embed


def _panel_embed() -> discord.Embed:
    summary_text = (
        "- **Get Task**: standard CA task assignment\n"
        "- **Task Choice**: reveal task now or open the fun roller link\n"
        "- **Active task**: opens task card with **Reroll** + **Case Reroll**\n"
        "- **Profile**: rerolls and linked-account progress"
    )
    embed = discord.Embed(
        title="RNG Street CA Challenge Board",
        color=discord.Color.from_rgb(232, 113, 38),
        timestamp=datetime.now(UTC),
    )
    embed.add_field(
        name="Quick Guide",
        value=summary_text,
        inline=False,
    )
    embed.add_field(
        name="Standard Flow",
        value=(
            "* Get a task\n"
            "* Choose Reveal Task or Fun Task link\n"
            "* Complete it\n"
            "* Repeat"
        ),
        inline=True,
    )
    embed.add_field(
        name="Fun Flow",
        value=(
            "* Press **Get Task**\n"
            "* Pick **Open Fun Task**\n"
            "* Repeat"
        ),
        inline=True,
    )
    embed.set_thumbnail(url=PANEL_ICON_IMAGE_URL)
    embed.set_image(url=PANEL_BANNER_IMAGE_URL)
    embed.set_footer(text="Requires WikiSync RuneLite plugin for live tracking")
    return embed


def _highscores_rank_badge(rank: int) -> str:
    if rank == 1:
        return "1st"
    if rank == 2:
        return "2nd"
    if rank == 3:
        return "3rd"
    return f"{rank}."


def _highscores_color(mode: str) -> discord.Color:
    if mode == HIGHSCORES_MODE_ALL_TIME:
        return discord.Color.gold()
    if mode == HIGHSCORES_MODE_TIER_LEADERS:
        return discord.Color.purple()
    return discord.Color.teal()


def _highscores_embed(
    summary: HighscoresSummary,
    *,
    mode: str,
    page: int = 0,
    page_size: int = HIGHSCORES_PAGE_SIZE,
    private_view: bool,
    highlight_rsn: str | None = None,
) -> discord.Embed:
    total_entries = len(summary.entries)
    total_pages = max((total_entries + page_size - 1) // page_size, 1)
    page_index = min(max(page, 0), total_pages - 1)
    start = page_index * page_size
    end = start + page_size
    page_entries = summary.entries[start:end]

    embed = discord.Embed(
        title=summary.title,
        description=summary.description,
        color=_highscores_color(mode),
        timestamp=datetime.now(UTC),
    )

    if page_entries:
        for entry in page_entries:
            is_highlighted = bool(highlight_rsn and entry.rsn.casefold() == highlight_rsn.casefold())
            entry_lines = _truncate_text(f"{entry.headline}\n{entry.detail}", 1016)
            embed.add_field(
                name=f"{_highscores_rank_badge(entry.rank)} {entry.rsn}{' | YOU' if is_highlighted else ''}",
                value=f"```{entry_lines}```",
                inline=False,
            )
    else:
        embed.add_field(name="Highscores", value=summary.empty_text, inline=False)

    footer = "Private highscores browser" if private_view else "Buttons below open private highscores views"
    footer = f"{footer} | Page {page_index + 1}/{total_pages}"
    if summary.reset_text:
        footer = f"{footer} | Reset {summary.reset_text}"
    embed.set_footer(text=footer)
    embed.set_thumbnail(url=HIGHSCORES_ICON_IMAGE_URL)
    embed.set_image(url=HIGHSCORES_BANNER_IMAGE_URL)
    return embed


def _payout_board_user_key(entry: RewardPayoutEntry) -> str:
    user_ref = (entry.discord_user_id or "").strip()
    if user_ref:
        parsed_user_id = _parse_discord_user_id(user_ref)
        if parsed_user_id:
            return parsed_user_id
        return user_ref
    rsn = (entry.rsn or "").strip()
    if rsn:
        return f"rsn:{rsn.casefold()}"
    return "unknown"


def _payout_board_user_label(
    user_key: str,
    entries: Sequence[RewardPayoutEntry],
    *,
    guild: discord.Guild | None = None,
) -> str:
    if user_key.startswith("rsn:"):
        rsn = entries[0].rsn if entries else "Unknown RSN"
        return f"RSN {rsn}"
    if user_key.isdigit():
        mention = f"<@{user_key}>"
        if guild is not None:
            member = guild.get_member(int(user_key))
            if member is not None:
                return f"{member.display_name} ({mention})"
        return mention
    if user_key == "unknown":
        return "Unknown User"
    fallback_rsn = entries[0].rsn if entries else ""
    if fallback_rsn:
        return f"{user_key} ({fallback_rsn})"
    return user_key


def _payout_board_user_lines(entries: Sequence[RewardPayoutEntry], *, max_lines: int = 6) -> str:
    if not entries:
        return "None"
    lines: list[str] = []
    visible_entries = list(entries)[:max_lines]
    for index, entry in enumerate(visible_entries, start=1):
        reward_text = _truncate_text(entry.reward_display_value, 46)
        tier_text = _truncate_text(entry.reward_tier or "Unknown", 16)
        rsn_text = _truncate_text(entry.rsn or "Unknown", 16)
        key_text = _compact_reward_key(entry.reward_key)
        lines.append(f"{index}. {rsn_text} | {reward_text} | {tier_text} | {key_text}")
    hidden = len(entries) - len(visible_entries)
    if hidden > 0:
        lines.append(f"... +{hidden} more")
    return _truncate_text("\n".join(lines), 1016)


def _reward_payouts_embed(summary: RewardPayoutSummary, *, guild: discord.Guild | None = None) -> discord.Embed:
    has_unpaid = summary.unpaid_count > 0
    color = discord.Color.orange() if has_unpaid else discord.Color.green()
    embed = discord.Embed(
        title="Reward Payout Board",
        description="Users with redeemed keys waiting for manual payout.",
        color=color,
        timestamp=datetime.now(UTC),
    )

    grouped_by_user: dict[str, list[RewardPayoutEntry]] = {}
    for entry in summary.unpaid_entries:
        grouped_by_user.setdefault(_payout_board_user_key(entry), []).append(entry)

    summary_lines = [
        f"unpaid payout keys  : {summary.unpaid_count}",
        f"users waiting       : {len(grouped_by_user)}",
        f"paid payout keys    : {summary.paid_count}",
    ]
    embed.add_field(name="Summary", value="```" + "\n".join(summary_lines) + "```", inline=False)

    if grouped_by_user:
        ordered_users = sorted(
            grouped_by_user.items(),
            key=lambda item: (-len(item[1]), item[0].casefold()),
        )
        max_user_fields = 9
        visible_users = ordered_users[:max_user_fields]
        for user_key, owed_entries in visible_users:
            user_label = _truncate_text(_payout_board_user_label(user_key, owed_entries, guild=guild), 240)
            embed.add_field(
                name=f"{user_label} ({len(owed_entries)})",
                value=f"```{_payout_board_user_lines(owed_entries)}```",
                inline=False,
            )
        hidden_users = len(ordered_users) - len(visible_users)
        if hidden_users > 0:
            embed.add_field(
                name="More Users",
                value=f"{hidden_users} additional user(s) have unpaid rewards not shown.",
                inline=False,
            )
    else:
        embed.add_field(name="Payout Queue", value="Nobody is waiting on payout right now.", inline=False)

    footer = "Use Refresh or Mark Paid below."
    embed.set_footer(text=footer)
    return embed


def _action_human_name(action: str) -> str:
    if action == ACTION_GET:
        return "Get Task"
    if action == ACTION_FUN:
        return "Get Fun Task"
    if action == ACTION_REROLL:
        return "Reroll"
    if action == ACTION_COMPLETE:
        return "Complete Task"
    if action == ACTION_PROFILE:
        return "Profile"
    return "Task Action"


def _profile_embed(profile: UserTaskProfileSummary) -> discord.Embed:
    total_accounts = len(profile.completed_tasks_by_account)
    total_completed_tasks = sum(account.completed_tasks for account in profile.completed_tasks_by_account)
    active_by_rsn: dict[str, ActiveTaskSummary] = {}
    for active in profile.active_tasks:
        key = active.rsn.casefold()
        if key not in active_by_rsn:
            active_by_rsn[key] = active

    summary_lines = [
        f"Rerolls remaining : {profile.rerolls_available}",
        f"accounts tracked : {total_accounts}",
        f"Bot tasks complete: {total_completed_tasks}",
        f"active tasks : {len(profile.active_tasks)}",
    ]

    embed = discord.Embed(
        title="Boss Progress",
        description="Track your Combat Achievement progress at a glance.",
        color=discord.Color.from_rgb(232, 113, 38),
        timestamp=datetime.now(UTC),
    )
    embed.add_field(name="Summary", value="```" + "\n".join(summary_lines) + "```", inline=False)

    if profile.active_tasks:
        active_lines = [
            f"`{active.rsn}`: {active.task.task_name}"
            for active in profile.active_tasks
        ]
        embed.add_field(
            name="Active Tasks",
            value=_truncate_text("\n".join(active_lines), 1024),
            inline=False,
        )
    else:
        embed.add_field(name="Active Tasks", value="None right now.", inline=False)

    if profile.completed_tasks_by_account:
        max_account_fields = 20
        visible_accounts = profile.completed_tasks_by_account[:max_account_fields]
        for account in visible_accounts:
            active_summary = active_by_rsn.get(account.rsn.casefold())
            current_task_label = active_summary.task.task_name if active_summary is not None else "None"
            code_lines = [
                f"rank : {account.rank_label}",
                f"tasks : {account.completed_ca_tasks} / {account.total_ca_tasks}",
                f"Current task : {current_task_label}",
            ]
            embed.add_field(
                name=account.rsn,
                value="```" + "\n".join(code_lines) + "```",
                inline=False,
            )
        hidden_count = len(profile.completed_tasks_by_account) - len(visible_accounts)
        if hidden_count > 0:
            embed.add_field(
                name="More Accounts",
                value=f"{hidden_count} additional account(s) not shown.",
                inline=False,
            )
    else:
        embed.add_field(name="Accounts", value="No account progress found yet.", inline=False)

    embed.set_thumbnail(url=PROFILE_ICON_IMAGE_URL)
    embed.set_image(url=PROFILE_BANNER_IMAGE_URL)
    embed.set_footer(text="Combat Task Tracker | Profile")
    return embed


def _reward_key_bucket_lines(
    entries: Sequence[RewardKeyStatusEntry],
    *,
    include_reward_value: bool = False,
) -> str:
    if not entries:
        return "None"
    lines: list[str] = []
    for entry in entries:
        rsn_label = entry.rsn or "Unknown RSN"
        task_label = f"Task {entry.task_id}" if entry.task_id is not None else "Task -"
        line = f"`{entry.reward_key}` | {rsn_label} | {task_label}"
        if include_reward_value:
            reward_label = entry.reward_display_value or "Reward -"
            line = f"{line} | {reward_label}"
        lines.append(line)
    return _truncate_text("\n".join(lines), 1024)


def _profile_rewards_embed(summary: UserRewardKeySummary) -> discord.Embed:
    summary_lines = [
        f"Ready reward keys   : {summary.unused_count}",
        f"Unpaid reward keys  : {summary.unpaid_count}",
    ]
    embed = discord.Embed(
        title="Reward Keys",
        description="Your reward-key status across all linked accounts.",
        color=discord.Color.gold(),
        timestamp=datetime.now(UTC),
    )
    embed.add_field(name="Summary", value="```" + "\n".join(summary_lines) + "```", inline=False)
    embed.add_field(
        name="Ready Reward Keys",
        value=_reward_key_bucket_lines(summary.unused_entries),
        inline=False,
    )
    embed.add_field(
        name="Unpaid Reward Keys",
        value=_reward_key_bucket_lines(summary.unpaid_entries, include_reward_value=True),
        inline=False,
    )
    embed.set_thumbnail(url=PROFILE_ICON_IMAGE_URL)
    embed.set_image(url=REWARDS_BANNER_IMAGE_URL)
    embed.set_footer(text=f"Showing up to {summary.limit_per_bucket} keys per category")
    return embed


def _fun_task_roll_embed(
    *,
    issued: TaskRollKeyIssue,
    roll_url: str,
) -> discord.Embed:
    mode_label = _task_roll_mode_label(issued.roll_mode)
    mode_note = "Uses your current active task as reroll source." if issued.roll_mode == "reroll" else "Assigns or reuses a task via web roller."
    embed = discord.Embed(
        title="Fun Task Key Ready",
        description=(
            f"Account: `{issued.rsn}`\n"
            f"Mode: **{mode_label}**\n"
            f"Key: `{issued.roll_key}`\n"
            f"[Open Task Roller]({roll_url})"
        ),
        color=discord.Color.from_rgb(232, 113, 38),
        timestamp=datetime.now(UTC),
    )
    embed.add_field(name="How This Key Works", value=mode_note, inline=False)
    embed.set_thumbnail(url=PANEL_ICON_IMAGE_URL)
    embed.set_image(url=PANEL_BANNER_IMAGE_URL)
    embed.set_footer(text="Generate as many fun-task keys as you want")
    return embed


def _fun_task_roll_link_view(roll_url: str) -> discord.ui.View:
    link_view = discord.ui.View(timeout=300)
    link_view.add_item(discord.ui.Button(label="Open Task Roller", style=discord.ButtonStyle.link, url=roll_url))
    return link_view


async def _send_ephemeral_followup(
    interaction: discord.Interaction,
    *,
    content: str | None = None,
    embed: discord.Embed | None = None,
    view: discord.ui.View | None = None,
) -> None:
    kwargs: dict[str, object] = {"ephemeral": True}
    if content is not None:
        kwargs["content"] = content
    if embed is not None:
        kwargs["embed"] = embed
    if view is not None:
        kwargs["view"] = view
    await interaction.followup.send(**kwargs)


async def _refresh_highscores_panel(bot: "RngCABot") -> None:
    try:
        await bot.ensure_highscores_panel_message()
    except Exception:
        LOGGER.exception("Could not refresh highscores panel after completion")

def _active_task_message_payload(
    bot: "RngCABot",
    *,
    owner_user_id: str,
    rsn: str,
    task: RandomTaskResult,
    rerolls_remaining: int | None = None,
    content: str | None = None,
) -> dict[str, object]:
    return {
        "content": content,
        "embed": _task_embed(rsn, task, rerolls_remaining=rerolls_remaining),
        "view": AssignedTaskView(bot, owner_user_id=owner_user_id, rsn=rsn),
    }


def _hidden_task_choice_embed(*, rsn: str, roll_url: str | None, reused_existing: bool = False) -> discord.Embed:
    description_lines = [
        f"Account: `{rsn}`",
        "Task: ||??????????||",
        "Status: existing active task found." if reused_existing else "Status: new task locked in.",
        "",
        "Choose one:",
        "- Reveal the assigned task now",
    ]
    if roll_url:
        description_lines.append("- Open the fun task roller link")
    embed = discord.Embed(
        title="Unknown Task",
        description="\n".join(description_lines),
        color=discord.Color.from_rgb(232, 113, 38),
        timestamp=datetime.now(UTC),
    )
    embed.set_thumbnail(url=PANEL_ICON_IMAGE_URL)
    embed.set_footer(text="Task hidden until you choose Reveal Task")
    return embed


class TaskRevealChoiceView(discord.ui.View):
    def __init__(
        self,
        bot: "RngCABot",
        *,
        owner_user_id: str,
        rsn: str,
        assignment: ActiveTaskAssignment,
        roll_url: str | None = None,
    ) -> None:
        super().__init__(timeout=600)
        self.bot = bot
        self.owner_user_id = owner_user_id
        self.rsn = rsn
        self.assignment = assignment
        self.roll_url = (roll_url or "").strip() or None
        if self.roll_url:
            self.add_item(
                discord.ui.Button(
                    label="Open Fun Task",
                    style=discord.ButtonStyle.link,
                    url=self.roll_url,
                    row=0,
                )
            )

    async def _ensure_owner(self, interaction: discord.Interaction) -> bool:
        if str(interaction.user.id) == self.owner_user_id:
            return True
        await interaction.response.send_message("This task choice belongs to another user.", ephemeral=True)
        return False

    @discord.ui.button(label="Reveal Task", style=discord.ButtonStyle.primary, row=0)
    async def reveal_task(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._ensure_owner(interaction):
            return

        assignment = self.assignment
        if assignment is None:
            _audit_log(interaction, "reveal_task_empty", rsn=self.rsn)
            await interaction.response.edit_message(
                content=f"No eligible tasks found for `{self.rsn}`.",
                embed=None,
                view=None,
            )
            return

        _audit_log(
            interaction,
            "reveal_task_success",
            rsn=assignment.rsn,
            task_id=assignment.task.task_id,
            task_name=assignment.task.task_name,
            reused_existing=assignment.reused_existing,
            tier=assignment.task.tier_label,
            points=assignment.task.points,
        )
        reveal_message = None
        if assignment.reused_existing:
            reveal_message = (
                f"`{assignment.rsn}` already has an active task. "
                "Use **Complete Task**, **Reroll**, or **Case Reroll** below."
            )
        await interaction.response.edit_message(
            **_active_task_message_payload(
                self.bot,
                owner_user_id=self.owner_user_id,
                rsn=assignment.rsn,
                task=assignment.task,
                rerolls_remaining=assignment.rerolls_remaining,
                content=reveal_message,
            )
        )

async def _send_assignment_response(
    bot: "RngCABot",
    interaction: discord.Interaction,
    owner_user_id: str,
    assignment: ActiveTaskAssignment,
    *,
    content: str | None = None,
) -> None:
    await _send_ephemeral_followup(
        interaction,
        **_active_task_message_payload(
            bot,
            owner_user_id=owner_user_id,
            rsn=assignment.rsn,
            task=assignment.task,
            rerolls_remaining=assignment.rerolls_remaining,
            content=content,
        ),
    )


async def _run_panel_action(
    bot: "RngCABot",
    interaction: discord.Interaction,
    action: str,
    discord_user_id: str,
    rsn: str,
) -> None:
    if action == ACTION_GET:
        try:
            assignment = await asyncio.to_thread(
                bot.services.sync_service.get_or_assign_active_task,
                discord_user_id,
                rsn,
            )
        except Exception:
            LOGGER.exception("Get Task failed for user %s rsn %s", discord_user_id, rsn)
            await _send_ephemeral_followup(
                interaction,
                content="Could not fetch a task right now. Please try again in a few seconds.",
            )
            return

        if assignment is None:
            _audit_log(interaction, "get_task_empty", rsn=rsn)
            await _send_ephemeral_followup(
                interaction,
                content=f"No eligible tasks found for `{rsn}`.",
            )
            return
        _audit_log(
            interaction,
            "get_task_assigned",
            rsn=assignment.rsn,
            task_id=assignment.task.task_id,
            task_name=assignment.task.task_name,
            reused_existing=assignment.reused_existing,
            tier=assignment.task.tier_label,
            points=assignment.task.points,
        )
        if assignment.reused_existing:
            await _send_assignment_response(
                bot,
                interaction,
                discord_user_id,
                assignment,
                content=(
                    f"`{assignment.rsn}` already has an active task. "
                    "Use **Complete Task**, **Reroll**, or **Case Reroll** below."
                ),
            )
            return

        roll_url: str | None = None
        try:
            issued = await asyncio.to_thread(
                bot.services.sync_service.issue_task_roll_key,
                discord_user_id,
                rsn=assignment.rsn,
                roll_mode="new",
            )
            if issued is not None:
                roll_url = _build_task_roll_url(bot.settings.task_roll_web_url, issued=issued)
                _audit_log(
                    interaction,
                    "get_task_choice_key_issued",
                    rsn=issued.rsn,
                    mode=issued.roll_mode,
                    key=_compact_reward_key(issued.roll_key),
                )
            else:
                _audit_log(interaction, "get_task_choice_key_empty", rsn=assignment.rsn)
        except Exception:
            LOGGER.exception("Could not issue task-choice key for user %s rsn %s", discord_user_id, assignment.rsn)
            _audit_log(interaction, "get_task_choice_key_error", rsn=assignment.rsn)

        await _send_ephemeral_followup(
            interaction,
            embed=_hidden_task_choice_embed(
                rsn=assignment.rsn,
                roll_url=roll_url,
                reused_existing=assignment.reused_existing,
            ),
            view=TaskRevealChoiceView(
                bot,
                owner_user_id=discord_user_id,
                rsn=assignment.rsn,
                assignment=assignment,
                roll_url=roll_url,
            ),
        )
        return

    if action == ACTION_FUN:
        try:
            has_active = await asyncio.to_thread(
                bot.services.sync_service.has_active_incomplete_task,
                discord_user_id,
                rsn,
            )
            if has_active:
                assignment = await asyncio.to_thread(
                    bot.services.sync_service.get_or_assign_active_task,
                    discord_user_id,
                    rsn,
                )
                if assignment is not None:
                    _audit_log(
                        interaction,
                        "get_fun_task_existing_active",
                        rsn=assignment.rsn,
                        task_id=assignment.task.task_id,
                        task_name=assignment.task.task_name,
                    )
                    await _send_assignment_response(
                        bot,
                        interaction,
                        discord_user_id,
                        assignment,
                        content=(
                            f"`{assignment.rsn}` already has an active task. "
                            "Use **Reroll** or **Case Reroll** below."
                        ),
                    )
                    return

            issued = await asyncio.to_thread(
                bot.services.sync_service.issue_task_roll_key,
                discord_user_id,
                rsn=rsn,
                roll_mode="new",
            )
        except Exception:
            LOGGER.exception("Get Fun Task failed for user %s rsn %s", discord_user_id, rsn)
            await _send_ephemeral_followup(
                interaction,
                content="Could not create a fun-task roll key right now. Please try again.",
            )
            return

        if issued is None:
            _audit_log(interaction, "get_fun_task_empty", rsn=rsn)
            await _send_ephemeral_followup(
                interaction,
                content=f"Could not issue a fun-task key for `{rsn}` right now.",
            )
            return

        roll_url = _build_task_roll_url(bot.settings.task_roll_web_url, issued=issued)
        _audit_log(
            interaction,
            "get_fun_task_key_issued",
            rsn=issued.rsn,
            mode=issued.roll_mode,
            key=_compact_reward_key(issued.roll_key),
        )

        await _send_ephemeral_followup(
            interaction,
            embed=_fun_task_roll_embed(issued=issued, roll_url=roll_url),
            view=_fun_task_roll_link_view(roll_url),
        )
        return

    if action == ACTION_COMPLETE:
        guild_id = str(interaction.guild_id) if interaction.guild_id is not None else None
        channel_id = str(interaction.channel_id) if interaction.channel_id is not None else None
        message_id = str(interaction.message.id) if interaction.message else None
        try:
            completed = await asyncio.to_thread(
                bot.services.sync_service.complete_active_task,
                discord_user_id,
                rsn,
                guild_id,
                channel_id,
                message_id,
            )
        except Exception:
            LOGGER.exception("Complete Task failed for user %s rsn %s", discord_user_id, rsn)
            await _send_ephemeral_followup(
                interaction,
                content="Could not mark your active task complete right now. Please try again.",
            )
            return

        if completed is None:
            _audit_log(interaction, "complete_missing_active", rsn=rsn)
            await _send_ephemeral_followup(
                interaction,
                content=f"No active task found for `{rsn}`. Click **Get Task** first.",
            )
            return

        if not completed.live_verified:
            _audit_log(
                interaction,
                "complete_verification_required",
                rsn=completed.rsn,
                task_id=completed.task.task_id,
                task_name=completed.task.task_name,
                live_verification_attempted=completed.live_verification_attempted,
            )
            await _send_ephemeral_followup(
                interaction,
                content=_wiki_sync_incomplete_message(completed.rsn),
            )
            return

        asyncio.create_task(_refresh_highscores_panel(bot))

        _audit_log(
            interaction,
            "complete_task",
            rsn=completed.rsn,
            task_id=completed.task.task_id,
            task_name=completed.task.task_name,
            reward_key=completed.reward_key,
            reward_status=completed.reward_status,
            live_verified=completed.live_verified,
            live_verification_attempted=completed.live_verification_attempted,
            awarded_rerolls=completed.awarded_rerolls,
            rerolls_remaining=completed.rerolls_remaining,
        )
        reward_url = (
            _build_reward_roll_url(bot.settings.reward_web_url, reward_key=completed.reward_key)
            if completed.reward_key
            else None
        )
        await _send_ephemeral_followup(
            interaction,
            embed=_task_embed(
                completed.rsn,
                completed.task,
                rerolls_remaining=completed.rerolls_remaining,
                reward_key=completed.reward_key,
                reward_status=completed.reward_status,
                reward_url=reward_url,
            ),
        )
        return

    if action == ACTION_REROLL:
        try:
            result = await asyncio.to_thread(
                bot.services.sync_service.reroll_active_task,
                discord_user_id,
                rsn,
            )
        except Exception:
            LOGGER.exception("Reroll failed for user %s rsn %s", discord_user_id, rsn)
            await _send_ephemeral_followup(
                interaction,
                content="Could not reroll right now. Please try again.",
            )
            return

        if result is None:
            _audit_log(interaction, "reroll_missing_active", rsn=rsn)
            await _send_ephemeral_followup(
                interaction,
                content=f"No active task found for `{rsn}`. Click **Get Task** first.",
            )
            return

        if result.replacement_task is None:
            if result.rerolls_remaining <= 0:
                _audit_log(interaction, "reroll_no_rerolls", rsn=rsn)
                await _send_ephemeral_followup(
                    interaction,
                    content=(
                        f"You have no rerolls left for `{rsn}`. "
                        "Click **Profile** to check your balance."
                    ),
                )
                return

            _audit_log(interaction, "reroll_no_alternative", rsn=rsn, rerolls_remaining=result.rerolls_remaining)
            await _send_ephemeral_followup(
                interaction,
                content=(
                    f"No alternative eligible task found for `{rsn}` right now. "
                    f"Your reroll was not spent. You still have **{result.rerolls_remaining}** available."
                ),
            )
            return

        _audit_log(
            interaction,
            "reroll_success",
            rsn=rsn,
            previous_task_id=result.previous_task.task_id,
            previous_task_name=result.previous_task.task_name,
            previous_task_image_url=result.previous_task.npc_image_url,
            new_task_id=result.replacement_task.task_id,
            new_task_name=result.replacement_task.task_name,
            new_task_image_url=result.replacement_task.npc_image_url,
            rerolls_remaining=result.rerolls_remaining,
        )
        await _send_ephemeral_followup(
            interaction,
            **_active_task_message_payload(
                bot,
                owner_user_id=discord_user_id,
                rsn=rsn,
                task=result.replacement_task,
                rerolls_remaining=result.rerolls_remaining,
                content=(
                    f"Rerolled `{rsn}` from **{result.previous_task.task_name}** "
                    f"to a new task. Rerolls left: **{result.rerolls_remaining}**."
                ),
            ),
        )
        return

    if action == ACTION_PROFILE:
        try:
            profile = await asyncio.to_thread(
                bot.services.sync_service.get_user_task_profile_summary,
                discord_user_id,
            )
        except Exception:
            LOGGER.exception("Profile lookup failed for user %s", discord_user_id)
            await _send_ephemeral_followup(
                interaction,
                content="Could not load your task profile right now. Please try again.",
            )
            return

        _audit_log(
            interaction,
            "profile_view",
            rerolls_available=profile.rerolls_available,
            accounts=len(profile.completed_tasks_by_account),
            active_tasks=len(profile.active_tasks),
        )
        await _send_ephemeral_followup(
            interaction,
            embed=_profile_embed(profile),
            view=ProfileActionsView(
                bot,
                owner_user_id=discord_user_id,
            ),
        )
        return

    await _send_ephemeral_followup(interaction, content="Unknown task action.")


def _load_highscores_summary(bot: "RngCABot", mode: str) -> HighscoresSummary:
    if mode == HIGHSCORES_MODE_ALL_TIME:
        return bot.services.sync_service.get_all_time_highscores_summary()
    if mode == HIGHSCORES_MODE_TIER_LEADERS:
        return bot.services.sync_service.get_overall_tier_leaders_summary()
    return bot.services.sync_service.get_monthly_highscores_summary()


def _load_reward_payout_summary(bot: "RngCABot") -> RewardPayoutSummary:
    return bot.services.sync_service.get_reward_payout_summary()


def _admin_panel_embed(default_target_user_id: str | None = None) -> discord.Embed:
    target_hint = default_target_user_id or "(enter per action)"
    embed = discord.Embed(
        title="Mod Tools",
        description="Moderator tools for rerolls, rewards, and task-state cleanup.",
        color=discord.Color.orange(),
        timestamp=datetime.now(UTC),
    )
    embed.add_field(
        name="Actions",
        value=(
            "`Set Rerolls` - set exact reroll count for one user\n"
            "`Add Rerolls` - add or subtract rerolls\n"
            "`Give Reward Key` - issue one or more reward keys for one user/RSN\n"
            "`Clear Active Task` - remove current assigned task(s)\n"
            "`Payout Board` - open unpaid payouts in admin\n"
            "`Refresh Panels` - refresh task + highscores boards"
        ),
        inline=False,
    )
    embed.add_field(
        name="Target User",
        value=f"`{target_hint}`",
        inline=False,
    )
    embed.set_footer(text="Mod tools")
    return embed


async def _open_admin_payout_board(
    bot: "RngCABot",
    interaction: discord.Interaction,
    *,
    owner_user_id: str,
    edit_existing: bool = False,
) -> None:
    try:
        summary = await asyncio.to_thread(_load_reward_payout_summary, bot)
    except Exception:
        LOGGER.exception("Could not load reward payout summary for admin board")
        message = "Could not load payout entries right now."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
        return

    view = AdminRewardPayoutsView(
        bot,
        owner_user_id=owner_user_id,
        has_unpaid=summary.unpaid_count > 0,
        has_paid=summary.paid_count > 0,
    )
    embed = _reward_payouts_embed(summary, guild=interaction.guild)

    if edit_existing:
        if interaction.response.is_done():
            await interaction.edit_original_response(content=None, embed=embed, view=view)
        else:
            await interaction.response.edit_message(content=None, embed=embed, view=view)
        return

    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


def _find_highscores_entry(summary: HighscoresSummary, rsn: str):
    target_rsn = rsn.casefold()
    return next((item for item in summary.entries if item.rsn.casefold() == target_rsn), None)


async def _show_highscores_browser(
    bot: "RngCABot",
    interaction: discord.Interaction,
    owner_user_id: str,
    mode: str,
    page: int = 0,
    *,
    edit_existing: bool,
    highlight_rsn: str | None = None,
) -> None:
    try:
        summary = await asyncio.to_thread(_load_highscores_summary, bot, mode)
    except Exception:
        LOGGER.exception("Highscores lookup failed for mode %s", mode)
        if interaction.response.is_done():
            await interaction.followup.send("Could not load highscores right now.", ephemeral=True)
        else:
            await interaction.response.send_message("Could not load highscores right now.", ephemeral=True)
        return

    total_pages = max((len(summary.entries) + HIGHSCORES_PAGE_SIZE - 1) // HIGHSCORES_PAGE_SIZE, 1)
    page_index = min(max(page, 0), total_pages - 1)
    embed = _highscores_embed(
        summary,
        mode=mode,
        page=page_index,
        private_view=True,
        highlight_rsn=highlight_rsn,
    )
    view = HighscoresBrowserView(
        bot,
        owner_user_id=owner_user_id,
        mode=mode,
        page=page_index,
        total_entries=len(summary.entries),
        highlight_rsn=highlight_rsn,
    )
    if edit_existing:
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=view)
        else:
            await interaction.response.edit_message(embed=embed, view=view)
    else:
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


async def _send_highscores_position(
    bot: "RngCABot",
    interaction: discord.Interaction,
    mode: str,
    rsn: str,
) -> None:
    try:
        summary = await asyncio.to_thread(_load_highscores_summary, bot, mode)
    except Exception:
        LOGGER.exception("Highscores position lookup failed for mode %s rsn %s", mode, rsn)
        await interaction.followup.send("Could not look up your position right now.", ephemeral=True)
        return

    entry = _find_highscores_entry(summary, rsn)
    if entry is None:
        await interaction.followup.send(
            f"`{rsn}` is not currently ranked on **{summary.title}**.",
            ephemeral=True,
        )
        return

    page_index = max((entry.rank - 1) // HIGHSCORES_PAGE_SIZE, 0)
    await _show_highscores_browser(
        bot,
        interaction,
        owner_user_id=str(interaction.user.id),
        mode=mode,
        page=page_index,
        edit_existing=True,
        highlight_rsn=entry.rsn,
    )


class ProfileActionsView(discord.ui.View):
    def __init__(self, bot: "RngCABot", *, owner_user_id: str) -> None:
        super().__init__(timeout=300)
        self.bot = bot
        self.owner_user_id = owner_user_id

    async def _ensure_owner(self, interaction: discord.Interaction) -> bool:
        if str(interaction.user.id) == self.owner_user_id:
            return True
        await interaction.response.send_message("This profile view belongs to another user.", ephemeral=True)
        return False

    @discord.ui.button(label="Rewards", style=discord.ButtonStyle.secondary, custom_id=PROFILE_REWARDS_CUSTOM_ID)
    async def rewards(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._ensure_owner(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        try:
            summary = await asyncio.to_thread(
                self.bot.services.sync_service.get_user_reward_key_summary,
                self.owner_user_id,
            )
        except Exception:
            LOGGER.exception("Reward summary lookup failed for user %s", self.owner_user_id)
            await interaction.followup.send(
                "Could not load your reward-key summary right now. Please try again.",
                ephemeral=True,
            )
            return

        _audit_log(
            interaction,
            "profile_rewards_view",
            unused_count=summary.unused_count,
            unpaid_count=summary.unpaid_count,
        )
        await interaction.followup.send(
            embed=_profile_rewards_embed(summary),
            ephemeral=True,
        )


class AccountPickerView(discord.ui.View):
    def __init__(self, bot: "RngCABot", owner_user_id: str, rsns: Sequence[str], action: str) -> None:
        super().__init__(timeout=120)
        self.bot = bot
        self.owner_user_id = owner_user_id
        self.action = action
        trimmed = list(rsns)[:25]
        self.truncated_count = max(len(rsns) - len(trimmed), 0)

        self.account_select = discord.ui.Select(
            placeholder=f"Choose account for {_action_human_name(action)}",
            min_values=1,
            max_values=1,
            options=[discord.SelectOption(label=rsn, value=rsn) for rsn in trimmed],
        )
        self.account_select.callback = self._select_callback
        self.add_item(self.account_select)

    async def _select_callback(self, interaction: discord.Interaction) -> None:
        if str(interaction.user.id) != self.owner_user_id:
            await interaction.response.send_message(
                "This selector belongs to another user.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)
        rsn = self.account_select.values[0]
        _audit_log(interaction, "task_account_selected", action=self.action, rsn=rsn)
        await _run_panel_action(
            self.bot,
            interaction,
            self.action,
            self.owner_user_id,
            rsn,
        )


class AssignedTaskView(discord.ui.View):
    def __init__(self, bot: "RngCABot", *, owner_user_id: str, rsn: str) -> None:
        super().__init__(timeout=900)
        self.bot = bot
        self.owner_user_id = owner_user_id
        self.rsn = rsn

    async def _ensure_owner(self, interaction: discord.Interaction) -> bool:
        if str(interaction.user.id) == self.owner_user_id:
            return True
        await interaction.response.send_message("This task card belongs to another user.", ephemeral=True)
        return False

    @discord.ui.button(label="Reroll", style=discord.ButtonStyle.secondary, row=0)
    async def reroll(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._ensure_owner(interaction):
            return

        try:
            result = await asyncio.to_thread(
                self.bot.services.sync_service.reroll_active_task,
                self.owner_user_id,
                self.rsn,
            )
        except Exception:
            LOGGER.exception("Task card reroll failed for user %s rsn %s", self.owner_user_id, self.rsn)
            await interaction.response.send_message("Could not reroll right now. Please try again.", ephemeral=True)
            return

        if result is None:
            _audit_log(interaction, "task_card_reroll_missing_active", rsn=self.rsn)
            await interaction.response.edit_message(
                content=f"No active task found for `{self.rsn}`. Click **Get Task** on the board for a new one.",
                embed=None,
                view=None,
            )
            return

        if result.replacement_task is None:
            if result.rerolls_remaining <= 0:
                _audit_log(interaction, "task_card_reroll_no_rerolls", rsn=self.rsn)
                await interaction.response.send_message(
                    f"You have no rerolls left for `{self.rsn}`. Click **Profile** to check your balance.",
                    ephemeral=True,
                )
                return

            _audit_log(
                interaction,
                "task_card_reroll_no_alternative",
                rsn=self.rsn,
                rerolls_remaining=result.rerolls_remaining,
            )
            await interaction.response.send_message(
                (
                    f"No alternative eligible task found for `{self.rsn}` right now. "
                    f"Your reroll was not spent. You still have **{result.rerolls_remaining}** available."
                ),
                ephemeral=True,
            )
            return

        _audit_log(
            interaction,
            "task_card_reroll_success",
            rsn=self.rsn,
            previous_task_id=result.previous_task.task_id,
            previous_task_name=result.previous_task.task_name,
            previous_task_image_url=result.previous_task.npc_image_url,
            new_task_id=result.replacement_task.task_id,
            new_task_name=result.replacement_task.task_name,
            new_task_image_url=result.replacement_task.npc_image_url,
            rerolls_remaining=result.rerolls_remaining,
        )
        await interaction.response.edit_message(
            **_active_task_message_payload(
                self.bot,
                owner_user_id=self.owner_user_id,
                rsn=self.rsn,
                task=result.replacement_task,
                rerolls_remaining=result.rerolls_remaining,
            ),
        )

    @discord.ui.button(label="Case Reroll", style=discord.ButtonStyle.primary, row=0)
    async def fun_reroll(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._ensure_owner(interaction):
            return

        try:
            issued = await asyncio.to_thread(
                self.bot.services.sync_service.issue_task_roll_key,
                self.owner_user_id,
                rsn=self.rsn,
                roll_mode="reroll",
            )
        except Exception:
            LOGGER.exception("Task card case reroll key issue failed for user %s rsn %s", self.owner_user_id, self.rsn)
            await interaction.response.send_message(
                "Could not create a case-reroll key right now. Please try again.",
                ephemeral=True,
            )
            return

        if issued is None:
            await interaction.response.send_message(
                f"Could not issue a case-reroll key for `{self.rsn}` right now.",
                ephemeral=True,
            )
            return

        roll_url = _build_task_roll_url(self.bot.settings.task_roll_web_url, issued=issued)
        _audit_log(
            interaction,
            "task_card_fun_reroll_key_issued",
            rsn=issued.rsn,
            mode=issued.roll_mode,
            key=_compact_reward_key(issued.roll_key),
        )
        await interaction.response.send_message(
            embed=_fun_task_roll_embed(issued=issued, roll_url=roll_url),
            view=_fun_task_roll_link_view(roll_url),
            ephemeral=True,
        )

    @discord.ui.button(label="Complete Task", style=discord.ButtonStyle.success, row=0)
    async def complete_task(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._ensure_owner(interaction):
            return

        guild_id = str(interaction.guild_id) if interaction.guild_id is not None else None
        channel_id = str(interaction.channel_id) if interaction.channel_id is not None else None
        message_id = str(interaction.message.id) if interaction.message else None
        try:
            completed = await asyncio.to_thread(
                self.bot.services.sync_service.complete_active_task,
                self.owner_user_id,
                self.rsn,
                guild_id,
                channel_id,
                message_id,
            )
        except Exception:
            LOGGER.exception("Task card completion failed for user %s rsn %s", self.owner_user_id, self.rsn)
            await interaction.response.send_message(
                "Could not mark your active task complete right now. Please try again.",
                ephemeral=True,
            )
            return

        if completed is None:
            _audit_log(interaction, "task_card_complete_missing_active", rsn=self.rsn)
            await interaction.response.edit_message(
                content=f"No active task found for `{self.rsn}`. Click **Get Task** on the board for a new one.",
                embed=None,
                view=None,
            )
            return

        if not completed.live_verified:
            _audit_log(
                interaction,
                "task_card_complete_verification_required",
                rsn=completed.rsn,
                task_id=completed.task.task_id,
                task_name=completed.task.task_name,
                live_verification_attempted=completed.live_verification_attempted,
            )
            await interaction.response.send_message(
                _wiki_sync_incomplete_message(completed.rsn),
                ephemeral=True,
            )
            return

        asyncio.create_task(_refresh_highscores_panel(self.bot))

        _audit_log(
            interaction,
            "task_card_complete",
            rsn=completed.rsn,
            task_id=completed.task.task_id,
            task_name=completed.task.task_name,
            reward_key=completed.reward_key,
            reward_status=completed.reward_status,
            live_verified=completed.live_verified,
            live_verification_attempted=completed.live_verification_attempted,
            awarded_rerolls=completed.awarded_rerolls,
            rerolls_remaining=completed.rerolls_remaining,
        )
        reward_url = (
            _build_reward_roll_url(self.bot.settings.reward_web_url, reward_key=completed.reward_key)
            if completed.reward_key
            else None
        )
        await interaction.response.edit_message(
            content=None,
            embed=_task_embed(
                completed.rsn,
                completed.task,
                rerolls_remaining=completed.rerolls_remaining,
                reward_key=completed.reward_key,
                reward_status=completed.reward_status,
                reward_url=reward_url,
            ),
            view=None,
        )


class GlobalTaskPanelView(discord.ui.View):
    def __init__(self, bot: "RngCABot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    async def _start_action(self, interaction: discord.Interaction, action: str) -> None:
        await interaction.response.defer(ephemeral=True)
        discord_user_id = str(interaction.user.id)

        if action == ACTION_PROFILE:
            await _run_panel_action(
                self.bot,
                interaction,
                action,
                discord_user_id,
                "",
            )
            return

        try:
            rsns = await asyncio.to_thread(
                self.bot.services.sync_service.resolve_rsns_for_discord_user,
                discord_user_id,
            )
        except Exception:
            LOGGER.exception("Could not resolve RSNs for user %s", discord_user_id)
            await _send_ephemeral_followup(
                interaction,
                content="Could not resolve your account mapping right now. Please try again shortly.",
            )
            return

        if not rsns:
            await _send_ephemeral_followup(
                interaction,
                content=(
                    "No RSN mapping found for your Discord ID. "
                    "Ask an admin to set your `members.DISCORD_ID` and `members.RSN` mapping."
                ),
            )
            return

        if len(rsns) > 1:
            picker = AccountPickerView(self.bot, discord_user_id, rsns, action)
            notice = f"You have multiple RSNs. Pick which account for **{_action_human_name(action)}**:"
            if picker.truncated_count > 0:
                notice = (
                    f"You have multiple RSNs. Pick account for **{_action_human_name(action)}** "
                    f"(showing first 25, {picker.truncated_count} not shown):"
                )
            await _send_ephemeral_followup(
                interaction,
                content=notice,
                view=picker,
            )
            return

        await _run_panel_action(
            self.bot,
            interaction,
            action,
            discord_user_id,
            rsns[0],
        )

    @discord.ui.button(label="Get Task", style=discord.ButtonStyle.primary, custom_id=GET_TASK_CUSTOM_ID)
    async def get_task(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._start_action(interaction, ACTION_GET)

    @discord.ui.button(label="Profile", style=discord.ButtonStyle.secondary, custom_id=PROFILE_CUSTOM_ID)
    async def profile(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._start_action(interaction, ACTION_PROFILE)


class HighscoresPanelView(discord.ui.View):
    def __init__(self, bot: "RngCABot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    async def _open_browser(self, interaction: discord.Interaction, mode: str) -> None:
        _audit_log(interaction, "highscores_open", mode=mode)
        await _show_highscores_browser(
            self.bot,
            interaction,
            owner_user_id=str(interaction.user.id),
            mode=mode,
            page=0,
            edit_existing=False,
        )

    @discord.ui.button(
        label="All-Time Bot",
        style=discord.ButtonStyle.primary,
        custom_id=ALL_TIME_HIGHSCORES_CUSTOM_ID,
    )
    async def all_time(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._open_browser(interaction, HIGHSCORES_MODE_ALL_TIME)

    @discord.ui.button(
        label="Overall Leaders",
        style=discord.ButtonStyle.secondary,
        custom_id=OVERALL_TIER_LEADERS_CUSTOM_ID,
    )
    async def tier_leaders(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._open_browser(interaction, HIGHSCORES_MODE_TIER_LEADERS)


class HighscoresAccountPickerView(discord.ui.View):
    def __init__(
        self,
        bot: "RngCABot",
        owner_user_id: str,
        rsns: Sequence[str],
        mode: str,
    ) -> None:
        super().__init__(timeout=120)
        self.bot = bot
        self.owner_user_id = owner_user_id
        self.mode = mode
        trimmed = list(rsns)[:25]
        self.truncated_count = max(len(rsns) - len(trimmed), 0)

        self.account_select = discord.ui.Select(
            placeholder="Choose account for your leaderboard position",
            min_values=1,
            max_values=1,
            options=[discord.SelectOption(label=rsn, value=rsn) for rsn in trimmed],
        )
        self.account_select.callback = self._select_callback
        self.add_item(self.account_select)

    async def _select_callback(self, interaction: discord.Interaction) -> None:
        if str(interaction.user.id) != self.owner_user_id:
            await interaction.response.send_message("This selector belongs to another user.", ephemeral=True)
            return

        try:
            summary = await asyncio.to_thread(_load_highscores_summary, self.bot, self.mode)
        except Exception:
            LOGGER.exception("Highscores position lookup failed for mode %s", self.mode)
            await interaction.response.edit_message(
                content="Could not look up your position right now.",
                view=None,
            )
            return

        rsn = self.account_select.values[0]
        entry = _find_highscores_entry(summary, rsn)
        if entry is None:
            _audit_log(interaction, "highscores_me_unranked", mode=self.mode, rsn=rsn)
            await interaction.response.send_message(
                f"`{rsn}` is not currently ranked on **{summary.title}**.",
                ephemeral=True,
            )
            return

        page_index = max((entry.rank - 1) // HIGHSCORES_PAGE_SIZE, 0)
        _audit_log(
            interaction,
            "highscores_me_jump",
            mode=self.mode,
            rsn=entry.rsn,
            rank=entry.rank,
            page=page_index + 1,
        )
        await interaction.response.edit_message(
            content=None,
            embed=_highscores_embed(
                summary,
                mode=self.mode,
                page=page_index,
                private_view=True,
                highlight_rsn=entry.rsn,
            ),
            view=HighscoresBrowserView(
                self.bot,
                owner_user_id=self.owner_user_id,
                mode=self.mode,
                page=page_index,
                total_entries=len(summary.entries),
                highlight_rsn=entry.rsn,
            ),
        )


class HighscoresBrowserView(discord.ui.View):
    def __init__(
        self,
        bot: "RngCABot",
        *,
        owner_user_id: str,
        mode: str,
        page: int,
        total_entries: int,
        highlight_rsn: str | None = None,
    ) -> None:
        super().__init__(timeout=300)
        self.bot = bot
        self.owner_user_id = owner_user_id
        self.mode = mode
        self.page = max(page, 0)
        self.total_entries = max(total_entries, 0)
        self.total_pages = max((self.total_entries + HIGHSCORES_PAGE_SIZE - 1) // HIGHSCORES_PAGE_SIZE, 1)
        self.highlight_rsn = highlight_rsn
        self._apply_button_state()

    def _apply_button_state(self) -> None:
        self.prev_page.disabled = self.page <= 0
        self.next_page.disabled = self.page >= self.total_pages - 1
        self.all_time_mode.style = (
            discord.ButtonStyle.primary if self.mode == HIGHSCORES_MODE_ALL_TIME else discord.ButtonStyle.secondary
        )
        self.tier_leaders_mode.style = (
            discord.ButtonStyle.primary
            if self.mode == HIGHSCORES_MODE_TIER_LEADERS
            else discord.ButtonStyle.secondary
        )

    async def _ensure_owner(self, interaction: discord.Interaction) -> bool:
        if str(interaction.user.id) == self.owner_user_id:
            return True
        await interaction.response.send_message("This highscores view belongs to another user.", ephemeral=True)
        return False

    async def _go_to(self, interaction: discord.Interaction, *, mode: str | None = None, page: int | None = None) -> None:
        if not await self._ensure_owner(interaction):
            return
        await _show_highscores_browser(
            self.bot,
            interaction,
            owner_user_id=self.owner_user_id,
            mode=mode or self.mode,
            page=self.page if page is None else page,
            edit_existing=True,
            highlight_rsn=self.highlight_rsn,
        )

    @discord.ui.button(label="All-Time Bot", style=discord.ButtonStyle.secondary, row=0)
    async def all_time_mode(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._go_to(interaction, mode=HIGHSCORES_MODE_ALL_TIME, page=0)

    @discord.ui.button(label="Overall Leaders", style=discord.ButtonStyle.secondary, row=0)
    async def tier_leaders_mode(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._go_to(interaction, mode=HIGHSCORES_MODE_TIER_LEADERS, page=0)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def prev_page(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._go_to(interaction, page=max(self.page - 1, 0))

    @discord.ui.button(label="Me", style=discord.ButtonStyle.success, row=1)
    async def me(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._ensure_owner(interaction):
            return
        await interaction.response.defer()
        try:
            rsns = await asyncio.to_thread(
                self.bot.services.sync_service.resolve_rsns_for_discord_user,
                self.owner_user_id,
            )
        except Exception:
            LOGGER.exception("Could not resolve RSNs for highscores lookup user %s", self.owner_user_id)
            await interaction.followup.send("Could not resolve your account mapping right now.", ephemeral=True)
            return

        if not rsns:
            await interaction.followup.send(
                "No RSN mapping found for your Discord ID.",
                ephemeral=True,
            )
            return

        if len(rsns) > 1:
            picker = HighscoresAccountPickerView(self.bot, self.owner_user_id, rsns, self.mode)
            notice = "Pick which account to check on the highscores:"
            if picker.truncated_count > 0:
                notice = (
                    f"Pick account for highscores position (showing first 25, {picker.truncated_count} not shown):"
                )
            await interaction.edit_original_response(content=notice, view=picker)
            return

        _audit_log(interaction, "highscores_me_single_account", mode=self.mode, rsn=rsns[0])
        await _send_highscores_position(self.bot, interaction, self.mode, rsns[0])

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, row=1)
    async def next_page(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._go_to(interaction, page=min(self.page + 1, self.total_pages - 1))


class _AdminActionModal(discord.ui.Modal):
    def __init__(
        self,
        bot: "RngCABot",
        *,
        title: str,
        default_target_user_id: str | None = None,
    ) -> None:
        super().__init__(title=title)
        self.bot = bot
        self.target_user_input = discord.ui.TextInput(
            label="Target user",
            placeholder="Discord user ID or @mention",
            required=True,
            max_length=40,
            default=default_target_user_id or "",
        )
        self.add_item(self.target_user_input)

    def _target_user_id(self) -> str | None:
        return _parse_discord_user_id(str(self.target_user_input.value))


class AdminSetRerollsModal(_AdminActionModal):
    def __init__(self, bot: "RngCABot", *, default_target_user_id: str | None = None) -> None:
        super().__init__(
            bot,
            title="Set Rerolls",
            default_target_user_id=default_target_user_id,
        )
        self.total_input = discord.ui.TextInput(
            label="New reroll total",
            placeholder="0",
            required=True,
            max_length=10,
        )
        self.add_item(self.total_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await _ensure_kick_members_permission(interaction):
            return
        target_user_id = self._target_user_id()
        if target_user_id is None:
            await interaction.response.send_message(
                "Provide a valid Discord user ID or mention for the target user.",
                ephemeral=True,
            )
            return
        try:
            new_total = int(str(self.total_input.value).strip())
        except ValueError:
            await interaction.response.send_message("Reroll total must be a whole number.", ephemeral=True)
            return
        if new_total < 0:
            await interaction.response.send_message("Rerolls cannot be negative.", ephemeral=True)
            return
        try:
            result = await asyncio.to_thread(
                self.bot.services.sync_service.admin_set_rerolls,
                target_user_id,
                new_total,
            )
        except Exception:
            LOGGER.exception("Admin set rerolls failed for target user %s", target_user_id)
            await interaction.response.send_message(
                "Could not update rerolls right now. Please try again.",
                ephemeral=True,
            )
            return
        _audit_log(
            interaction,
            "admin_set_rerolls",
            target_user_id=target_user_id,
            previous_rerolls=result.previous_rerolls,
            current_rerolls=result.current_rerolls,
        )
        await interaction.response.send_message(
            (
                f"Updated rerolls for <@{target_user_id}>: "
                f"**{result.previous_rerolls} -> {result.current_rerolls}**."
            ),
            ephemeral=True,
        )


class AdminAddRerollsModal(_AdminActionModal):
    def __init__(self, bot: "RngCABot", *, default_target_user_id: str | None = None) -> None:
        super().__init__(
            bot,
            title="Add Rerolls",
            default_target_user_id=default_target_user_id,
        )
        self.delta_input = discord.ui.TextInput(
            label="Reroll delta",
            placeholder="+1 or -1",
            required=True,
            max_length=10,
        )
        self.add_item(self.delta_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await _ensure_kick_members_permission(interaction):
            return
        target_user_id = self._target_user_id()
        if target_user_id is None:
            await interaction.response.send_message(
                "Provide a valid Discord user ID or mention for the target user.",
                ephemeral=True,
            )
            return
        try:
            delta = int(str(self.delta_input.value).strip())
        except ValueError:
            await interaction.response.send_message("Reroll delta must be a whole number.", ephemeral=True)
            return
        if delta == 0:
            await interaction.response.send_message("Reroll delta cannot be zero.", ephemeral=True)
            return
        try:
            result = await asyncio.to_thread(
                self.bot.services.sync_service.admin_adjust_rerolls,
                target_user_id,
                delta,
            )
        except Exception:
            LOGGER.exception("Admin add rerolls failed for target user %s", target_user_id)
            await interaction.response.send_message(
                "Could not update rerolls right now. Please try again.",
                ephemeral=True,
            )
            return
        _audit_log(
            interaction,
            "admin_adjust_rerolls",
            target_user_id=target_user_id,
            delta=delta,
            previous_rerolls=result.previous_rerolls,
            current_rerolls=result.current_rerolls,
        )
        await interaction.response.send_message(
            (
                f"Applied reroll delta **{delta:+d}** for <@{target_user_id}>. "
                f"Now **{result.current_rerolls}** (was {result.previous_rerolls})."
            ),
            ephemeral=True,
        )


class AdminGiveRewardKeyModal(_AdminActionModal):
    def __init__(self, bot: "RngCABot", *, default_target_user_id: str | None = None) -> None:
        super().__init__(
            bot,
            title="Give Reward Key",
            default_target_user_id=default_target_user_id,
        )
        self.rsn_input = discord.ui.TextInput(
            label="RSN",
            placeholder="Exact RSN for this reward key",
            required=True,
            max_length=32,
        )
        self.task_id_input = discord.ui.TextInput(
            label="Task ID (optional)",
            placeholder="Leave blank to use active task ID for this RSN",
            required=False,
            max_length=10,
        )
        self.quantity_input = discord.ui.TextInput(
            label="Quantity",
            placeholder="1",
            default="1",
            required=True,
            max_length=2,
        )
        self.add_item(self.rsn_input)
        self.add_item(self.task_id_input)
        self.add_item(self.quantity_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await _ensure_kick_members_permission(interaction):
            return

        target_user_id = self._target_user_id()
        if target_user_id is None:
            await interaction.response.send_message(
                "Provide a valid Discord user ID or mention for the target user.",
                ephemeral=True,
            )
            return

        rsn_value = _normalize_optional_rsn(str(self.rsn_input.value) or "")
        if rsn_value is None:
            await interaction.response.send_message("RSN is required.", ephemeral=True)
            return

        raw_task_id = str(self.task_id_input.value or "").strip()
        task_id: int | None = None
        if raw_task_id:
            try:
                task_id = int(raw_task_id)
            except ValueError:
                await interaction.response.send_message("Task ID must be a whole number.", ephemeral=True)
                return
            if task_id <= 0:
                await interaction.response.send_message("Task ID must be a positive integer.", ephemeral=True)
                return

        try:
            quantity = int(str(self.quantity_input.value or "").strip())
        except ValueError:
            await interaction.response.send_message("Quantity must be a whole number.", ephemeral=True)
            return
        if quantity <= 0:
            await interaction.response.send_message("Quantity must be at least 1.", ephemeral=True)
            return
        if quantity > 25:
            await interaction.response.send_message("Quantity cannot exceed 25.", ephemeral=True)
            return

        try:
            issued_results = await asyncio.to_thread(
                self.bot.services.sync_service.admin_issue_reward_key,
                target_user_id,
                rsn=rsn_value,
                task_id=task_id,
                quantity=quantity,
            )
        except ValueError as exc:
            await interaction.response.send_message(str(exc), ephemeral=True)
            return
        except Exception:
            LOGGER.exception(
                "Admin give reward key failed for target user %s rsn %s task_id %s quantity %s",
                target_user_id,
                rsn_value,
                task_id,
                quantity,
            )
            await interaction.response.send_message(
                "Could not issue a reward key right now. Please try again.",
                ephemeral=True,
            )
            return

        if issued_results is None:
            await interaction.response.send_message(
                (
                    f"No active task was found for `{rsn_value}`. "
                    "Enter a Task ID to force a reward key for a specific task."
                ),
                ephemeral=True,
            )
            return

        if not issued_results:
            await interaction.response.send_message("No reward keys were issued.", ephemeral=True)
            return

        first_result = issued_results[0]
        _audit_log(
            interaction,
            "admin_give_reward_key",
            target_user_id=target_user_id,
            rsn=first_result.rsn,
            quantity_requested=quantity,
            quantity_issued=len(issued_results),
            reward_keys=", ".join(_compact_reward_key(entry.reward_key) for entry in issued_results),
        )

        if len(issued_results) == 1:
            result = issued_results[0]
            reward_url = _build_reward_roll_url(self.bot.settings.reward_web_url, reward_key=result.reward_key)
            source_label = "active task" if result.used_active_task else f"task {result.task_id}"
            status_label = _reward_status_label(result.reward_status) or result.reward_status
            created_label = "new key" if result.created_new else "existing key"
            if result.reward_status.strip().casefold() == "redeemed":
                await interaction.response.send_message(
                    (
                        f"Found an existing redeemed reward key for <@{target_user_id}> `{result.rsn}` ({source_label}).\n"
                        f"Key: `{result.reward_key}` ({created_label}, status: {status_label}).\n"
                        "Use a different Task ID to issue another usable reward key."
                    ),
                    ephemeral=True,
                )
                return
            await interaction.response.send_message(
                (
                    f"Issued reward key for <@{target_user_id}> `{result.rsn}` ({source_label}).\n"
                    f"Key: `{result.reward_key}` ({created_label}, status: {status_label}).\n"
                    f"Redeem link: {reward_url}"
                ),
                ephemeral=True,
            )
            return

        summary_lines: list[str] = []
        for entry in issued_results:
            source_label = "active task" if entry.used_active_task else f"task {entry.task_id}"
            status_label = _reward_status_label(entry.reward_status) or entry.reward_status
            created_label = "new" if entry.created_new else "existing"
            summary_lines.append(f"`{entry.reward_key}` - {source_label} - {created_label} - {status_label}")
        details = _truncate_text("\n".join(summary_lines), 1400)
        await interaction.response.send_message(
            (
                f"Issued **{len(issued_results)}** reward keys for <@{target_user_id}> `{first_result.rsn}`.\n"
                f"{details}"
            ),
            ephemeral=True,
        )


class AdminClearActiveTaskModal(_AdminActionModal):
    def __init__(self, bot: "RngCABot", *, default_target_user_id: str | None = None) -> None:
        super().__init__(
            bot,
            title="Clear Active Task",
            default_target_user_id=default_target_user_id,
        )
        self.rsn_input = discord.ui.TextInput(
            label="RSN (optional)",
            placeholder="Leave blank to clear all active tasks for the user",
            required=False,
            max_length=32,
        )
        self.add_item(self.rsn_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await _ensure_kick_members_permission(interaction):
            return
        target_user_id = self._target_user_id()
        if target_user_id is None:
            await interaction.response.send_message(
                "Provide a valid Discord user ID or mention for the target user.",
                ephemeral=True,
            )
            return
        rsn_value = _normalize_optional_rsn(str(self.rsn_input.value))
        try:
            result = await asyncio.to_thread(
                self.bot.services.sync_service.admin_clear_active_tasks,
                target_user_id,
                rsn=rsn_value,
            )
        except Exception:
            LOGGER.exception(
                "Admin clear active task failed for target user %s rsn %s",
                target_user_id,
                rsn_value,
            )
            await interaction.response.send_message(
                "Could not clear active task(s) right now. Please try again.",
                ephemeral=True,
            )
            return
        touched_label = ", ".join(result.touched_rsns) if result.touched_rsns else "none"
        _audit_log(
            interaction,
            "admin_clear_active_tasks",
            target_user_id=target_user_id,
            rsn=rsn_value,
            cleared_tasks=result.cleared_tasks,
            touched_rsns=len(result.touched_rsns),
        )
        await interaction.response.send_message(
            (
                f"Cleared **{result.cleared_tasks}** active task row(s) for <@{target_user_id}>.\n"
                f"RSNs touched: `{touched_label}`"
            ),
            ephemeral=True,
        )


class AdminPanelView(discord.ui.View):
    def __init__(
        self,
        bot: "RngCABot",
        *,
        owner_user_id: str,
        default_target_user_id: str | None = None,
    ) -> None:
        super().__init__(timeout=900)
        self.bot = bot
        self.owner_user_id = owner_user_id
        self.default_target_user_id = default_target_user_id

    async def _ensure_owner(self, interaction: discord.Interaction) -> bool:
        if str(interaction.user.id) == self.owner_user_id:
            return True
        await interaction.response.send_message("This admin panel belongs to another user.", ephemeral=True)
        return False

    async def _open_modal(self, interaction: discord.Interaction, modal: discord.ui.Modal) -> None:
        if not await self._ensure_owner(interaction):
            return
        if not await _ensure_kick_members_permission(interaction):
            return
        await interaction.response.send_modal(modal)

    @discord.ui.button(
        label="Set Rerolls",
        style=discord.ButtonStyle.secondary,
        custom_id=ADMIN_SET_REROLLS_CUSTOM_ID,
        row=0,
    )
    async def set_rerolls(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._open_modal(
            interaction,
            AdminSetRerollsModal(
                self.bot,
                default_target_user_id=self.default_target_user_id,
            ),
        )

    @discord.ui.button(
        label="Add Rerolls",
        style=discord.ButtonStyle.secondary,
        custom_id=ADMIN_ADD_REROLLS_CUSTOM_ID,
        row=0,
    )
    async def add_rerolls(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._open_modal(
            interaction,
            AdminAddRerollsModal(
                self.bot,
                default_target_user_id=self.default_target_user_id,
            ),
        )

    @discord.ui.button(
        label="Give Reward Key",
        style=discord.ButtonStyle.secondary,
        custom_id=ADMIN_GIVE_REWARD_KEY_CUSTOM_ID,
        row=0,
    )
    async def give_reward_key(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._open_modal(
            interaction,
            AdminGiveRewardKeyModal(
                self.bot,
                default_target_user_id=self.default_target_user_id,
            ),
        )

    @discord.ui.button(
        label="Clear Active Task",
        style=discord.ButtonStyle.secondary,
        custom_id=ADMIN_CLEAR_ACTIVE_CUSTOM_ID,
        row=1,
    )
    async def clear_active_task(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._open_modal(
            interaction,
            AdminClearActiveTaskModal(
                self.bot,
                default_target_user_id=self.default_target_user_id,
            ),
        )

    @discord.ui.button(
        label="Payout Board",
        style=discord.ButtonStyle.secondary,
        custom_id=ADMIN_PAYOUTS_CUSTOM_ID,
        row=2,
    )
    async def payout_board(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._ensure_owner(interaction):
            return
        if not await _ensure_kick_members_permission(interaction):
            return
        _audit_log(interaction, "admin_payout_board_open")
        await _open_admin_payout_board(
            self.bot,
            interaction,
            owner_user_id=self.owner_user_id,
        )

    @discord.ui.button(
        label="Refresh Panels",
        style=discord.ButtonStyle.primary,
        custom_id=ADMIN_REFRESH_PANELS_CUSTOM_ID,
        row=2,
    )
    async def refresh_panels(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._ensure_owner(interaction):
            return
        if not await _ensure_kick_members_permission(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        status_lines: list[str] = []
        has_error = False
        panel_actions = [
            ("Task panel", self.bot.ensure_task_panel_message),
            ("Highscores panel", self.bot.ensure_highscores_panel_message),
        ]
        for panel_name, action in panel_actions:
            try:
                refreshed = await action()
                status = "ok" if refreshed else "not configured"
                status_lines.append(f"{panel_name}: {status}")
            except Exception:
                LOGGER.exception("Admin refresh failed for %s", panel_name)
                has_error = True
                status_lines.append(f"{panel_name}: error")
        _audit_log(
            interaction,
            "admin_refresh_panels",
            status="; ".join(status_lines),
            has_error=has_error,
        )
        prefix = "Panel refresh completed." if not has_error else "Panel refresh completed with errors."
        await interaction.followup.send(
            f"{prefix}\n" + "\n".join(status_lines),
            ephemeral=True,
        )


class RewardPayoutMarkPaidPickerView(discord.ui.View):
    def __init__(
        self,
        bot: "RngCABot",
        *,
        owner_user_id: str,
        unpaid_entries: Sequence[RewardPayoutEntry],
    ) -> None:
        super().__init__(timeout=120)
        self.bot = bot
        self.owner_user_id = owner_user_id
        trimmed = list(unpaid_entries)[:25]
        self.truncated_count = max(len(unpaid_entries) - len(trimmed), 0)
        self.unpaid_entries = {entry.reward_key: entry for entry in trimmed}

        options = [
            discord.SelectOption(
                label=_truncate_text(f"{entry.rsn} • {entry.reward_display_value}", 100),
                value=entry.reward_key,
                description=_truncate_text(f"{entry.reward_tier} • {_compact_reward_key(entry.reward_key)}", 100),
            )
            for entry in trimmed
        ]
        self.reward_select = discord.ui.Select(
            placeholder="Choose reward to mark paid",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.reward_select.callback = self._select_callback
        self.add_item(self.reward_select)

    async def _select_callback(self, interaction: discord.Interaction) -> None:
        if str(interaction.user.id) != self.owner_user_id:
            await interaction.response.send_message("This payout selector belongs to another user.", ephemeral=True)
            return

        reward_key = self.reward_select.values[0]
        try:
            payout = await asyncio.to_thread(
                self.bot.services.sync_service.mark_reward_paid,
                reward_key,
                actor=str(interaction.user),
            )
        except Exception:
            LOGGER.exception("Could not mark reward %s as paid", reward_key)
            await interaction.response.edit_message(
                content="Could not mark that reward as paid right now.",
                view=None,
            )
            return

        if payout is None:
            await interaction.response.edit_message(
                content="That reward could not be marked as paid. It may have been updated already.",
                view=None,
            )
            return

        _audit_log(
            interaction,
            "reward_payout_mark_paid",
            reward_key=payout.reward_key,
            rsn=payout.rsn,
            reward=payout.reward_display_value,
        )
        await interaction.response.edit_message(
            content=f"Marked `{payout.rsn}` • **{payout.reward_display_value}** as paid.",
            view=None,
        )


class RewardPayoutUndoPaidPickerView(discord.ui.View):
    def __init__(
        self,
        bot: "RngCABot",
        *,
        owner_user_id: str,
        paid_entries: Sequence[RewardPayoutEntry],
    ) -> None:
        super().__init__(timeout=120)
        self.bot = bot
        self.owner_user_id = owner_user_id
        trimmed = list(paid_entries)[:25]
        self.truncated_count = max(len(paid_entries) - len(trimmed), 0)

        options = [
            discord.SelectOption(
                label=_truncate_text(f"{entry.rsn} • {entry.reward_display_value}", 100),
                value=entry.reward_key,
                description=_truncate_text(
                    f"Paid by {entry.payout_marked_by or 'Unknown'} • {_compact_reward_key(entry.reward_key)}",
                    100,
                ),
            )
            for entry in trimmed
        ]
        self.reward_select = discord.ui.Select(
            placeholder="Choose paid reward to undo",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.reward_select.callback = self._select_callback
        self.add_item(self.reward_select)

    async def _select_callback(self, interaction: discord.Interaction) -> None:
        if str(interaction.user.id) != self.owner_user_id:
            await interaction.response.send_message("This payout selector belongs to another user.", ephemeral=True)
            return

        reward_key = self.reward_select.values[0]
        try:
            payout = await asyncio.to_thread(
                self.bot.services.sync_service.mark_reward_unpaid,
                reward_key,
                actor=str(interaction.user),
            )
        except Exception:
            LOGGER.exception("Could not undo payout for reward %s", reward_key)
            await interaction.response.edit_message(
                content="Could not undo that payout right now.",
                view=None,
            )
            return

        if payout is None:
            await interaction.response.edit_message(
                content="That reward could not be switched back to unpaid.",
                view=None,
            )
            return

        _audit_log(
            interaction,
            "reward_payout_undo_paid",
            reward_key=payout.reward_key,
            rsn=payout.rsn,
            reward=payout.reward_display_value,
        )
        await interaction.response.edit_message(
            content=f"Moved `{payout.rsn}` • **{payout.reward_display_value}** back to unpaid.",
            view=None,
        )


class AdminRewardPayoutsView(discord.ui.View):
    def __init__(
        self,
        bot: "RngCABot",
        *,
        owner_user_id: str,
        has_unpaid: bool = True,
        has_paid: bool = True,
    ) -> None:
        super().__init__(timeout=900)
        self.bot = bot
        self.owner_user_id = owner_user_id
        self.mark_paid.disabled = not has_unpaid
        self.undo_paid.disabled = not has_paid

    async def _ensure_owner(self, interaction: discord.Interaction) -> bool:
        if str(interaction.user.id) == self.owner_user_id:
            return True
        await interaction.response.send_message("This payout board belongs to another user.", ephemeral=True)
        return False

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, custom_id=PAYOUTS_REFRESH_CUSTOM_ID)
    async def refresh(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._ensure_owner(interaction):
            return
        if not await _ensure_kick_members_permission(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        _audit_log(interaction, "admin_payout_board_refresh")
        await _open_admin_payout_board(
            self.bot,
            interaction,
            owner_user_id=self.owner_user_id,
            edit_existing=True,
        )

    @discord.ui.button(label="Mark Paid", style=discord.ButtonStyle.success, custom_id=PAYOUTS_MARK_PAID_CUSTOM_ID)
    async def mark_paid(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._ensure_owner(interaction):
            return
        if not await _ensure_kick_members_permission(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        try:
            summary = await asyncio.to_thread(
                self.bot.services.sync_service.get_reward_payout_summary,
                unpaid_limit=25,
                paid_limit=8,
            )
        except Exception:
            LOGGER.exception("Could not load reward payout summary for mark-paid flow")
            await interaction.followup.send("Could not load payout entries right now.", ephemeral=True)
            return

        if not summary.unpaid_entries:
            await interaction.followup.send("No unpaid rewards are waiting right now.", ephemeral=True)
            return

        picker = RewardPayoutMarkPaidPickerView(
            self.bot,
            owner_user_id=str(interaction.user.id),
            unpaid_entries=summary.unpaid_entries,
        )
        notice = "Choose a redeemed reward to mark as paid:"
        if picker.truncated_count > 0:
            notice = f"Choose reward to mark paid (showing first 25, {picker.truncated_count} not shown):"
        await interaction.followup.send(content=notice, view=picker, ephemeral=True)

    @discord.ui.button(label="Undo Paid", style=discord.ButtonStyle.danger, custom_id=PAYOUTS_UNDO_PAID_CUSTOM_ID)
    async def undo_paid(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._ensure_owner(interaction):
            return
        if not await _ensure_kick_members_permission(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        try:
            summary = await asyncio.to_thread(
                self.bot.services.sync_service.get_reward_payout_summary,
                unpaid_limit=12,
                paid_limit=25,
            )
        except Exception:
            LOGGER.exception("Could not load reward payout summary for undo-paid flow")
            await interaction.followup.send("Could not load paid payout entries right now.", ephemeral=True)
            return

        if not summary.recent_paid_entries:
            await interaction.followup.send("No paid rewards are available to undo right now.", ephemeral=True)
            return

        picker = RewardPayoutUndoPaidPickerView(
            self.bot,
            owner_user_id=str(interaction.user.id),
            paid_entries=summary.recent_paid_entries,
        )
        notice = "Choose a paid reward to move back into the unpaid queue:"
        if picker.truncated_count > 0:
            notice = f"Choose paid reward to undo (showing first 25, {picker.truncated_count} not shown):"
        await interaction.followup.send(content=notice, view=picker, ephemeral=True)


class RngCABot(commands.Bot):
    def __init__(self, settings: Settings, services: BotServices) -> None:
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)
        self.settings = settings
        self.services = services
        self._panel_listener_view = GlobalTaskPanelView(self)
        self._highscores_listener_view = HighscoresPanelView(self)

    def _register_app_commands(self) -> None:
        @self.tree.command(name="admin", description="Open moderator controls")
        @app_commands.guild_only()
        @app_commands.default_permissions(kick_members=True)
        @app_commands.describe(member="Optional member to prefill in admin actions")
        async def admin_panel(interaction: discord.Interaction, member: discord.Member | None = None) -> None:
            if not await _ensure_kick_members_permission(interaction):
                return
            default_target_user_id = str(member.id) if member is not None else None
            _audit_log(
                interaction,
                "admin_panel_open",
                default_target_user_id=default_target_user_id,
            )
            await interaction.response.send_message(
                embed=_admin_panel_embed(default_target_user_id),
                view=AdminPanelView(
                    self,
                    owner_user_id=str(interaction.user.id),
                    default_target_user_id=default_target_user_id,
                ),
                ephemeral=True,
            )

    async def setup_hook(self) -> None:
        self.add_view(self._panel_listener_view)
        self.add_view(self._highscores_listener_view)
        self.tree.clear_commands(guild=None)
        self._register_app_commands()
        try:
            await self.tree.sync()
        except Exception:
            LOGGER.exception("Failed to sync command tree")

    def _get_panel_record(self, panel_key: str) -> dict | None:
        with self.services.sync_service.db.connection() as conn:
            return self.services.sync_service.db.get_bot_panel(conn, panel_key)

    def _save_panel_record(self, panel_key: str, guild_id: str | None, channel_id: str, message_id: str) -> None:
        with self.services.sync_service.db.connection() as conn:
            try:
                self.services.sync_service.db.upsert_bot_panel(
                    conn,
                    panel_key=panel_key,
                    guild_id=guild_id,
                    channel_id=channel_id,
                    message_id=message_id,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    async def _ensure_panel_message(
        self,
        *,
        panel_key: str,
        channel_id: int | None,
        embed: discord.Embed,
        view: discord.ui.View,
        panel_name: str,
        force_recreate: bool = False,
    ) -> bool:
        if channel_id is None:
            LOGGER.info("%s not configured; skipping %s setup.", panel_name, panel_key)
            return False

        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except Exception:
                LOGGER.exception("Could not fetch %s channel %s", panel_name, channel_id)
                return False

        if not isinstance(channel, discord.abc.Messageable):
            LOGGER.warning(
                "Configured %s channel is not messageable: %s",
                panel_name,
                channel_id,
            )
            return False

        panel_record = await asyncio.to_thread(self._get_panel_record, panel_key)
        if panel_record and not force_recreate:
            record_channel_id = str(panel_record.get("channel_id") or "").strip()
            record_message_id = str(panel_record.get("message_id") or "").strip()
            if record_channel_id == str(channel_id) and record_message_id:
                fetch_message = getattr(channel, "fetch_message", None)
                if callable(fetch_message):
                    try:
                        panel_message = await fetch_message(int(record_message_id))
                        await panel_message.edit(embed=embed, view=view)
                        return True
                    except discord.NotFound:
                        LOGGER.info("Stored %s message %s not found; creating a new one", panel_name, record_message_id)
                    except Exception:
                        LOGGER.exception("Could not restore existing %s message; creating a new one", panel_name)

        message = await channel.send(embed=embed, view=view)
        guild_id = str(message.guild.id) if message.guild else None
        await asyncio.to_thread(
            self._save_panel_record,
            panel_key,
            guild_id,
            str(message.channel.id),
            str(message.id),
        )
        LOGGER.info(
            "%s is active in channel %s message %s",
            panel_name,
            message.channel.id,
            message.id,
        )
        return True

    async def ensure_task_panel_message(self, force_recreate: bool = False) -> bool:
        return await self._ensure_panel_message(
            panel_key=PANEL_KEY,
            channel_id=self.settings.task_panel_channel_id,
            embed=_panel_embed(),
            view=GlobalTaskPanelView(self),
            panel_name="Task panel",
            force_recreate=force_recreate,
        )

    async def ensure_highscores_panel_message(self, force_recreate: bool = False) -> bool:
        summary = await asyncio.to_thread(_load_highscores_summary, self, HIGHSCORES_MODE_MONTHLY)
        return await self._ensure_panel_message(
            panel_key=HIGHSCORES_PANEL_KEY,
            channel_id=self.settings.highscores_panel_channel_id,
            embed=_highscores_embed(
                summary,
                mode=HIGHSCORES_MODE_MONTHLY,
                page=0,
                private_view=False,
            ),
            view=HighscoresPanelView(self),
            panel_name="Highscores panel",
            force_recreate=force_recreate,
        )

    async def post_scan_status(self, result: ScanRunResult) -> None:
        LOGGER.info(
            f"Daily scan completed | run `{result.run_id}` | status `{result.status}` | "
            f"success `{result.success_users}` failed `{result.failed_users}` / total `{result.total_users}`"
        )
        try:
            await self.ensure_highscores_panel_message()
        except Exception:
            LOGGER.exception("Could not refresh highscores panel after scan")


async def post_boss_image_mappings_once(
    settings: Settings,
    sync_service: SyncService,
    *,
    channel_id: int,
) -> int:
    mappings = await asyncio.to_thread(sync_service.get_boss_image_mappings)
    if not mappings:
        LOGGER.info("No boss image mappings found in the CA catalog.")
        return 0

    client = discord.Client(intents=discord.Intents.none())
    posted = 0
    startup_error: Exception | None = None

    @client.event
    async def on_ready() -> None:
        nonlocal posted, startup_error
        try:
            channel = client.get_channel(channel_id)
            if channel is None:
                channel = await client.fetch_channel(channel_id)
            if not isinstance(channel, discord.abc.Messageable):
                raise RuntimeError(f"Channel {channel_id} is not messageable")

            for mapping in mappings:
                image_url = mapping.npc_image_url or "No image URL available."
                await channel.send(f"**{mapping.npc}**\n{image_url}")
                posted += 1

            LOGGER.info("Posted %s boss image mappings to channel %s", posted, channel_id)
        except Exception as exc:
            startup_error = exc
            LOGGER.exception("Could not post boss image mappings to channel %s", channel_id)
        finally:
            await client.close()

    await client.start(settings.discord_token)
    if startup_error is not None:
        raise startup_error
    return posted
