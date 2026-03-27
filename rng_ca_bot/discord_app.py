from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Sequence

import discord
from discord.ext import commands

from .config import Settings
from .db import ScanRunResult
from .sync_service import (
    ActiveTaskAssignment,
    BossImageMapping,
    CompletionResult,
    HighscoresSummary,
    RewardPayoutEntry,
    RewardPayoutSummary,
    RandomTaskResult,
    RerollResult,
    SyncService,
    UserTaskProfileSummary,
)

LOGGER = logging.getLogger(__name__)

PANEL_KEY = "global_task_panel"
HIGHSCORES_PANEL_KEY = "global_highscores_panel"
PAYOUTS_PANEL_KEY = "reward_payouts_panel"
GET_TASK_CUSTOM_ID = "rngca:panel:get_task"
REROLL_CUSTOM_ID = "rngca:panel:reroll"
COMPLETE_TASK_CUSTOM_ID = "rngca:panel:complete_task"
PROFILE_CUSTOM_ID = "rngca:panel:profile"
MONTHLY_HIGHSCORES_CUSTOM_ID = "rngca:highscores:monthly"
ALL_TIME_HIGHSCORES_CUSTOM_ID = "rngca:highscores:all_time"
OVERALL_TIER_LEADERS_CUSTOM_ID = "rngca:highscores:tier_leaders"
PAYOUTS_REFRESH_CUSTOM_ID = "rngca:payouts:refresh"
PAYOUTS_MARK_PAID_CUSTOM_ID = "rngca:payouts:mark_paid"
PAYOUTS_UNDO_PAID_CUSTOM_ID = "rngca:payouts:undo_paid"

ACTION_GET = "get_task"
ACTION_REROLL = "reroll"
ACTION_COMPLETE = "complete_task"
ACTION_PROFILE = "profile"
HIGHSCORES_MODE_MONTHLY = "monthly"
HIGHSCORES_MODE_ALL_TIME = "all_time"
HIGHSCORES_MODE_TIER_LEADERS = "tier_leaders"
HIGHSCORES_PAGE_SIZE = 20
PAYOUTS_REFRESH_INTERVAL_SECONDS = 60


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
        return "Pending verification"
    if normalized == "redeemed":
        return "Redeemed"
    if normalized == "cancelled":
        return "Cancelled"
    return status


def _task_embed(
    rsn: str,
    task: RandomTaskResult,
    *,
    reward_key: str | None = None,
    reward_status: str | None = None,
) -> discord.Embed:
    embed = discord.Embed(
        title=task.task_name,
        description=_truncate_text(task.task_description or "No description available.", 4096),
        color=_task_color(task.points, task.tier_label),
        timestamp=datetime.now(UTC),
    )
    if task.npc_url:
        embed.url = task.npc_url
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
        inline=False,
    )
    if reward_key:
        embed.add_field(name="Reward Key", value=f"`{reward_key}`", inline=False)
    if reward_status:
        embed.add_field(name="Reward Status", value=_reward_status_label(reward_status) or reward_status, inline=True)
    if task.npc_image_url:
        embed.set_thumbnail(url=task.npc_image_url)
    embed.set_footer(text=f"Combat Task Tracker • {rsn}")
    return embed


def _panel_embed() -> discord.Embed:
    embed = discord.Embed(
        title="RNG Street CA Challenge Board!",
        description=(
            "Take on randomly assigned Combat Achievements\n"
            "across your all of your accounts.\n"
            "Can you earn a spot on the leaderboards?\n"
            "(Requires wikisync runelite plugin to work correctly)"
        ),
        color=discord.Color.from_rgb(232, 113, 38),
    )
    embed.add_field(
        name="Your challenge",
        value=(
            "* Get a task\n"
            "* Complete it\n"
            "* Repeat"
        ),
        inline=True,
    )
    embed.add_field(
        name="System",
        value=(
            "* Multi-account support\n"
            "* Limited rerolls\n"
            "* Progress tracking\n"
        ),
        inline=True,
    )
    embed.set_thumbnail(url="https://oldschool.runescape.wiki/images/Tzkal_slayer_helmet_chathead.png")
    embed.set_image(url="https://i.redd.it/vobh86y0aopz.jpg")
    embed.set_footer(text="RNG Street CA / PvM Challenge System")
    return embed


def _highscores_rank_badge(rank: int) -> str:
    if rank == 1:
        return "🥇"
    if rank == 2:
        return "🥈"
    if rank == 3:
        return "🥉"
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
    if summary.reset_text:
        embed.add_field(name="Reset", value=summary.reset_text, inline=True)
    embed.add_field(name="Page", value=f"{page_index + 1}/{total_pages}", inline=True)
    embed.add_field(name="Tracked", value=f"**{total_entries}**", inline=True)

    if page_entries:
        for entry in page_entries:
            is_highlighted = bool(highlight_rsn and entry.rsn.casefold() == highlight_rsn.casefold())
            embed.add_field(
                name=f"{_highscores_rank_badge(entry.rank)} `{entry.rsn}`{' • YOU' if is_highlighted else ''}",
                value=f"**{entry.headline}**\n{entry.detail}",
                inline=False,
            )
    else:
        embed.add_field(name="Highscores", value=summary.empty_text, inline=False)

    footer = "Private highscores browser" if private_view else "Buttons below open private highscores views"
    embed.set_footer(text=footer)
    return embed


def _reward_payout_status_line(entry: RewardPayoutEntry, *, paid: bool) -> str:
    if paid:
        paid_by = entry.payout_marked_by or "Unknown admin"
        paid_at = _discord_relative_timestamp(entry.payout_marked_at)
        return (
            f"`{entry.rsn}` • **{entry.reward_display_value}**\n"
            f"Paid by **{paid_by}** {paid_at}"
        )

    return (
        f"`{entry.rsn}` • **{entry.reward_display_value}** • `{_compact_reward_key(entry.reward_key)}`\n"
        f"Redeemed {_discord_relative_timestamp(entry.redeemed_at)}"
    )


def _reward_payouts_embed(summary: RewardPayoutSummary) -> discord.Embed:
    has_unpaid = summary.unpaid_count > 0
    color = discord.Color.orange() if has_unpaid else discord.Color.green()
    embed = discord.Embed(
        title="Reward Payout Board",
        description="Redeemed reward keys waiting on manual payout. Keep this in an admin-only channel.",
        color=color,
        timestamp=datetime.now(UTC),
    )
    embed.add_field(name="Needs Payout", value=f"**{summary.unpaid_count}**", inline=True)
    embed.add_field(name="Paid Out", value=f"**{summary.paid_count}**", inline=True)
    embed.add_field(name="Updated", value=_discord_relative_timestamp(datetime.now(UTC)), inline=True)

    if summary.unpaid_entries:
        unpaid_lines = [_reward_payout_status_line(entry, paid=False) for entry in summary.unpaid_entries]
        embed.add_field(
            name="Waiting Now",
            value=_truncate_text("\n\n".join(unpaid_lines), 1024),
            inline=False,
        )
    else:
        embed.add_field(name="Waiting Now", value="Nothing is waiting on payout right now.", inline=False)

    if summary.recent_paid_entries:
        paid_lines = [_reward_payout_status_line(entry, paid=True) for entry in summary.recent_paid_entries]
        embed.add_field(
            name="Recently Paid",
            value=_truncate_text("\n\n".join(paid_lines), 1024),
            inline=False,
        )
    else:
        embed.add_field(name="Recently Paid", value="No paid rewards have been recorded yet.", inline=False)

    footer = "Use Refresh or Mark Paid below. The board refreshes automatically every minute."
    embed.set_footer(text=footer)
    return embed


def _action_human_name(action: str) -> str:
    if action == ACTION_GET:
        return "Get Task"
    if action == ACTION_REROLL:
        return "Reroll"
    if action == ACTION_COMPLETE:
        return "Complete Task"
    if action == ACTION_PROFILE:
        return "Profile"
    return "Task Action"


def _profile_embed(profile: UserTaskProfileSummary) -> discord.Embed:
    embed = discord.Embed(
        title="Task Profile",
        description="Your CA challenge progress across bot-assigned tasks.",
        color=discord.Color.blurple(),
        timestamp=datetime.now(UTC),
    )
    embed.add_field(name="Rerolls", value=f"**{profile.rerolls_available}**", inline=True)

    if profile.completed_tasks_by_account:
        completed_lines = [
            f"`{account.rsn}`: **{account.completed_tasks}** bot tasks | **{account.total_points} pts**"
            for account in profile.completed_tasks_by_account
        ]
        embed.add_field(
            name="Account Progress",
            value=_truncate_text("\n".join(completed_lines), 1024),
            inline=False,
        )
    else:
        embed.add_field(name="Account Progress", value="No account progress found yet.", inline=False)

    if profile.active_tasks:
        active_lines = [
            f"`{active.rsn}`: {active.task.task_name} (`ID {active.task.task_id}`)"
            for active in profile.active_tasks
        ]
        embed.add_field(
            name="Active Accounts",
            value=_truncate_text("\n".join(active_lines), 1024),
            inline=False,
        )
    else:
        embed.add_field(name="Active Accounts", value="None right now.", inline=False)

    embed.set_footer(text="Combat Task Tracker")
    return embed


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


async def _refresh_reward_payouts_panel(bot: "RngCABot") -> None:
    try:
        await bot.ensure_reward_payouts_panel_message()
    except Exception:
        LOGGER.exception("Could not refresh reward payouts panel")


def _active_task_message_payload(
    bot: "RngCABot",
    *,
    owner_user_id: str,
    rsn: str,
    task: RandomTaskResult,
    content: str | None = None,
) -> dict[str, object]:
    return {
        "content": content,
        "embed": _task_embed(rsn, task),
        "view": AssignedTaskView(bot, owner_user_id=owner_user_id, rsn=rsn),
    }

async def _send_assignment_response(
    bot: "RngCABot",
    interaction: discord.Interaction,
    owner_user_id: str,
    assignment: ActiveTaskAssignment,
) -> None:
    await _send_ephemeral_followup(
        interaction,
        **_active_task_message_payload(
            bot,
            owner_user_id=owner_user_id,
            rsn=assignment.rsn,
            task=assignment.task,
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
        await _send_assignment_response(bot, interaction, discord_user_id, assignment)
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
        await _send_ephemeral_followup(
            interaction,
            embed=_task_embed(
                completed.rsn,
                completed.task,
                reward_key=completed.reward_key,
                reward_status=completed.reward_status,
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
        await _send_ephemeral_followup(interaction, embed=_profile_embed(profile))
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

    @discord.ui.button(label="Reroll", style=discord.ButtonStyle.secondary)
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
            ),
        )

    @discord.ui.button(label="Complete Task", style=discord.ButtonStyle.success)
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
        await interaction.response.edit_message(
            content=None,
            embed=_task_embed(
                completed.rsn,
                completed.task,
                reward_key=completed.reward_key,
                reward_status=completed.reward_status,
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
        asyncio.create_task(_refresh_reward_payouts_panel(self.bot))
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
        asyncio.create_task(_refresh_reward_payouts_panel(self.bot))
        await interaction.response.edit_message(
            content=f"Moved `{payout.rsn}` • **{payout.reward_display_value}** back to unpaid.",
            view=None,
        )


class RewardPayoutPanelView(discord.ui.View):
    def __init__(self, bot: "RngCABot", *, has_unpaid: bool = True, has_paid: bool = True) -> None:
        super().__init__(timeout=None)
        self.bot = bot
        self.mark_paid.disabled = not has_unpaid
        self.undo_paid.disabled = not has_paid

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, custom_id=PAYOUTS_REFRESH_CUSTOM_ID)
    async def refresh(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await interaction.response.defer(ephemeral=True)
        await _refresh_reward_payouts_panel(self.bot)
        _audit_log(interaction, "reward_payouts_refresh")
        await interaction.followup.send("Reward payout board refreshed.", ephemeral=True)

    @discord.ui.button(label="Mark Paid", style=discord.ButtonStyle.success, custom_id=PAYOUTS_MARK_PAID_CUSTOM_ID)
    async def mark_paid(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
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
        self._reward_payouts_listener_view = RewardPayoutPanelView(self)
        self._reward_payouts_refresh_task: asyncio.Task | None = None

    async def setup_hook(self) -> None:
        self.add_view(self._panel_listener_view)
        self.add_view(self._highscores_listener_view)
        self.add_view(self._reward_payouts_listener_view)
        self.tree.clear_commands(guild=None)
        try:
            await self.tree.sync()
        except Exception:
            LOGGER.exception("Failed to sync empty command tree")

    def start_reward_payouts_refresh_loop(self) -> None:
        if self.settings.reward_payouts_channel_id is None:
            return
        if self._reward_payouts_refresh_task is not None and not self._reward_payouts_refresh_task.done():
            return
        self._reward_payouts_refresh_task = asyncio.create_task(self._reward_payouts_refresh_loop())

    async def _reward_payouts_refresh_loop(self) -> None:
        try:
            while not self.is_closed():
                await asyncio.sleep(PAYOUTS_REFRESH_INTERVAL_SECONDS)
                await self.ensure_reward_payouts_panel_message()
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("Reward payouts refresh loop failed")

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

    async def ensure_reward_payouts_panel_message(self, force_recreate: bool = False) -> bool:
        summary = await asyncio.to_thread(_load_reward_payout_summary, self)
        return await self._ensure_panel_message(
            panel_key=PAYOUTS_PANEL_KEY,
            channel_id=self.settings.reward_payouts_channel_id,
            embed=_reward_payouts_embed(summary),
            view=RewardPayoutPanelView(
                self,
                has_unpaid=summary.unpaid_count > 0,
                has_paid=summary.paid_count > 0,
            ),
            panel_name="Reward payouts panel",
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
        try:
            await self.ensure_reward_payouts_panel_message()
        except Exception:
            LOGGER.exception("Could not refresh reward payouts panel after scan")


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
