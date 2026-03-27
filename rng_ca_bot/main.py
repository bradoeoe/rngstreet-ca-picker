from __future__ import annotations

import asyncio
import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .config import load_settings
from .db import Database
from .discord_app import BotServices, RngCABot, post_boss_image_mappings_once
from .scheduler import DailyScanScheduler
from .sync_service import SyncService

LOGGER = logging.getLogger(__name__)


def _configure_logging(level: str, log_file: str | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                log_path,
                maxBytes=2_000_000,
                backupCount=5,
                encoding="utf-8",
            )
        )

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
        force=True,
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RuneLite daily sync + CA Discord bot")
    parser.add_argument(
        "--scan-once",
        action="store_true",
        help="Run one immediate scan and exit (no Discord login)",
    )
    parser.add_argument(
        "--refresh-catalog",
        action="store_true",
        help="Refresh CA task catalog from wiki and exit",
    )
    parser.add_argument(
        "--show-player",
        metavar="RSN",
        help="Print latest stored snapshot/progress summary for one RSN and exit",
    )
    parser.add_argument(
        "--post-boss-images",
        metavar="CHANNEL_ID",
        type=int,
        help="Post one-off boss -> image embeds into a Discord channel and exit",
    )
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    settings = load_settings()
    _configure_logging(settings.log_level, settings.log_file)

    repo_root = Path(__file__).resolve().parent.parent
    migrations_dir = repo_root / "sql"

    db = Database(settings)
    db.run_migrations(migrations_dir)
    sync_service = SyncService(settings=settings, db=db)

    if args.refresh_catalog:
        count = sync_service.refresh_catalog()
        LOGGER.info("Catalog refreshed with %s tasks", count)
        return

    if args.scan_once:
        result = sync_service.run_daily_scan(trigger_source="manual")
        LOGGER.info(
            "Scan completed | run=%s status=%s success=%s failed=%s total=%s",
            result.run_id,
            result.status,
            result.success_users,
            result.failed_users,
            result.total_users,
        )
        return

    if args.show_player:
        summary = sync_service.get_player_debug_summary(args.show_player)
        if summary is None:
            LOGGER.info("No stored data found for RSN '%s'", args.show_player)
            return
        print(json.dumps(summary, indent=2))
        return

    if args.post_boss_images:
        if not settings.discord_token:
            raise SystemExit("DISCORD_TOKEN is required to post boss image mappings")
        posted = asyncio.run(
            post_boss_image_mappings_once(
                settings,
                sync_service,
                channel_id=int(args.post_boss_images),
            )
        )
        LOGGER.info("Boss image mapping post finished | channel=%s posted=%s", args.post_boss_images, posted)
        return

    if not settings.discord_token:
        raise SystemExit("DISCORD_TOKEN is required to run bot mode")

    services = BotServices(sync_service=sync_service)
    bot = RngCABot(settings=settings, services=services)
    scheduler = DailyScanScheduler(settings=settings, bot=bot, sync_service=sync_service)

    started_scheduler = False
    ensured_task_panel = False
    ensured_highscores_panel = False

    @bot.event
    async def on_ready() -> None:
        nonlocal started_scheduler, ensured_task_panel, ensured_highscores_panel
        LOGGER.info("Discord bot online as %s", bot.user)
        if not ensured_task_panel:
            try:
                await bot.ensure_task_panel_message()
            except Exception:
                LOGGER.exception("Task panel setup failed during startup")
            ensured_task_panel = True
        if not ensured_highscores_panel:
            try:
                await bot.ensure_highscores_panel_message()
            except Exception:
                LOGGER.exception("Highscores panel setup failed during startup")
            ensured_highscores_panel = True
        if not started_scheduler:
            scheduler.start()
            started_scheduler = True

    @bot.event
    async def close() -> None:
        await scheduler.stop()
        await super(RngCABot, bot).close()

    bot.run(settings.discord_token, log_handler=None)


if __name__ == "__main__":
    main()
