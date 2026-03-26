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
from .sync_service import ActiveTaskAssignment, RandomTaskResult, SyncService

LOGGER = logging.getLogger(__name__)

PANEL_KEY = "global_task_panel"
GET_TASK_CUSTOM_ID = "rngca:panel:get_task"
TOO_HARD_CUSTOM_ID = "rngca:panel:too_hard"
COMPLETE_TASK_CUSTOM_ID = "rngca:panel:complete_task"

ACTION_GET = "get_task"
ACTION_TOO_HARD = "too_hard"
ACTION_COMPLETE = "complete_task"


@dataclass(slots=True)
class BotServices:
    sync_service: SyncService


def _truncate_text(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 3].rstrip() + "..."


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


def _task_embed(rsn: str, task: RandomTaskResult) -> discord.Embed:
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
        name="Tasks Available",
        value=f"**{max(task.eligible_count, 0)}**",
        inline=False,
    )
    if task.npc_image_url:
        embed.set_thumbnail(url=task.npc_image_url)
    embed.set_footer(text=f"Combat Task Tracker • {rsn}")
    return embed


def _panel_embed() -> discord.Embed:
    return discord.Embed(
        title="RNG Street CA Challenge Board",
        description=(
            "Grab a random CA and send it.\n"
            "**Get Task** = pull your active task.\n"
            "**Too Hard** = reroll to a lower-point task for that account.\n"
            "**Complete Task** = claim completion."
        ),
        color=discord.Color.gold(),
    )


def _action_human_name(action: str) -> str:
    if action == ACTION_GET:
        return "Get Task"
    if action == ACTION_TOO_HARD:
        return "Too Hard"
    if action == ACTION_COMPLETE:
        return "Complete Task"
    return "Task Action"


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


async def _send_assignment_response(
    interaction: discord.Interaction,
    assignment: ActiveTaskAssignment,
) -> None:
    headline = "Current active task" if assignment.reused_existing else "New task assigned"
    await _send_ephemeral_followup(
        interaction,
        content=f"{headline} for `{assignment.rsn}`:",
        embed=_task_embed(assignment.rsn, assignment.task),
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
            await _send_ephemeral_followup(
                interaction,
                content=f"No eligible tasks found for `{rsn}`.",
            )
            return
        await _send_assignment_response(interaction, assignment)
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
            await _send_ephemeral_followup(
                interaction,
                content=f"No active task found for `{rsn}`. Click **Get Task** first.",
            )
            return

        completed_rsn, task = completed
        await _send_ephemeral_followup(
            interaction,
            content=(
                f"Marked complete for `{completed_rsn}`: **{task.task_name}** (`ID {task.task_id}`). "
                "Click **Get Task** for your next one."
            ),
        )
        return

    if action == ACTION_TOO_HARD:
        try:
            result = await asyncio.to_thread(
                bot.services.sync_service.reroll_active_task_too_hard,
                discord_user_id,
                rsn,
            )
        except Exception:
            LOGGER.exception("Too Hard reroll failed for user %s rsn %s", discord_user_id, rsn)
            await _send_ephemeral_followup(
                interaction,
                content="Could not reroll right now. Please try again.",
            )
            return

        if result is None:
            await _send_ephemeral_followup(
                interaction,
                content=f"No active task found for `{rsn}`. Click **Get Task** first.",
            )
            return

        if result.replacement_task is None:
            points = result.previous_task.points
            if points is not None:
                await _send_ephemeral_followup(
                    interaction,
                    content=(
                        f"No easier eligible task found under `{points}` points for `{rsn}`. "
                        "Try completing this one or wait for the next scan cycle."
                    ),
                )
            else:
                await _send_ephemeral_followup(
                    interaction,
                    content=f"No alternative eligible task found for `{rsn}` right now.",
                )
            return

        await _send_ephemeral_followup(
            interaction,
            content=(
                f"Rerolled `{rsn}` from **{result.previous_task.task_name}** "
                f"to an easier task:"
            ),
            embed=_task_embed(rsn, result.replacement_task),
        )
        return

    await _send_ephemeral_followup(interaction, content="Unknown task action.")


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
        await _run_panel_action(
            self.bot,
            interaction,
            self.action,
            self.owner_user_id,
            rsn,
        )


class GlobalTaskPanelView(discord.ui.View):
    def __init__(self, bot: "RngCABot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    async def _start_action(self, interaction: discord.Interaction, action: str) -> None:
        await interaction.response.defer(ephemeral=True)
        discord_user_id = str(interaction.user.id)

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

    @discord.ui.button(label="Too Hard", style=discord.ButtonStyle.secondary, custom_id=TOO_HARD_CUSTOM_ID)
    async def too_hard(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._start_action(interaction, ACTION_TOO_HARD)

    @discord.ui.button(
        label="Complete Task",
        style=discord.ButtonStyle.success,
        custom_id=COMPLETE_TASK_CUSTOM_ID,
    )
    async def complete_task(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        await self._start_action(interaction, ACTION_COMPLETE)


class RngCABot(commands.Bot):
    def __init__(self, settings: Settings, services: BotServices) -> None:
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)
        self.settings = settings
        self.services = services
        self._panel_listener_view = GlobalTaskPanelView(self)

    async def setup_hook(self) -> None:
        self.add_view(self._panel_listener_view)
        self.tree.clear_commands(guild=None)
        try:
            await self.tree.sync()
        except Exception:
            LOGGER.exception("Failed to sync empty command tree")

    def _get_panel_record(self) -> dict | None:
        with self.services.sync_service.db.connection() as conn:
            return self.services.sync_service.db.get_bot_panel(conn, PANEL_KEY)

    def _save_panel_record(self, guild_id: str | None, channel_id: str, message_id: str) -> None:
        with self.services.sync_service.db.connection() as conn:
            try:
                self.services.sync_service.db.upsert_bot_panel(
                    conn,
                    panel_key=PANEL_KEY,
                    guild_id=guild_id,
                    channel_id=channel_id,
                    message_id=message_id,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    async def ensure_task_panel_message(self, force_recreate: bool = False) -> bool:
        if self.settings.task_panel_channel_id is None:
            LOGGER.info("DISCORD_TASK_PANEL_CHANNEL_ID not configured; skipping task panel setup.")
            return False

        channel = self.get_channel(self.settings.task_panel_channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(self.settings.task_panel_channel_id)
            except Exception:
                LOGGER.exception("Could not fetch task panel channel %s", self.settings.task_panel_channel_id)
                return False

        if not isinstance(channel, discord.abc.Messageable):
            LOGGER.warning(
                "Configured task panel channel is not messageable: %s",
                self.settings.task_panel_channel_id,
            )
            return False

        panel_record = await asyncio.to_thread(self._get_panel_record)
        if panel_record and not force_recreate:
            record_channel_id = str(panel_record.get("channel_id") or "").strip()
            record_message_id = str(panel_record.get("message_id") or "").strip()
            if record_channel_id == str(self.settings.task_panel_channel_id) and record_message_id:
                fetch_message = getattr(channel, "fetch_message", None)
                if callable(fetch_message):
                    try:
                        panel_message = await fetch_message(int(record_message_id))
                        await panel_message.edit(embed=_panel_embed(), view=GlobalTaskPanelView(self))
                        return True
                    except discord.NotFound:
                        LOGGER.info("Stored panel message %s not found; creating a new one", record_message_id)
                    except Exception:
                        LOGGER.exception("Could not restore existing task panel message; creating a new one")

        message = await channel.send(embed=_panel_embed(), view=GlobalTaskPanelView(self))
        guild_id = str(message.guild.id) if message.guild else None
        await asyncio.to_thread(
            self._save_panel_record,
            guild_id,
            str(message.channel.id),
            str(message.id),
        )
        LOGGER.info(
            "Task panel is active in channel %s message %s",
            message.channel.id,
            message.id,
        )
        return True

    async def post_scan_status(self, result: ScanRunResult) -> None:
        LOGGER.info(
            f"Daily scan completed | run `{result.run_id}` | status `{result.status}` | "
            f"success `{result.success_users}` failed `{result.failed_users}` / total `{result.total_users}`"
        )
