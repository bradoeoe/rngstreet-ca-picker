from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from .config import load_settings
from .db import Database
from .discord_app import BotServices, RngCABot
from .scheduler import DailyScanScheduler
from .sync_service import SyncService

LOGGER = logging.getLogger(__name__)


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
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
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    settings = load_settings()
    _configure_logging(settings.log_level)

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

    if not settings.discord_token:
        raise SystemExit("DISCORD_TOKEN is required to run bot mode")

    services = BotServices(sync_service=sync_service)
    bot = RngCABot(settings=settings, services=services)
    scheduler = DailyScanScheduler(settings=settings, bot=bot, sync_service=sync_service)

    started_scheduler = False
    ensured_task_panel = False

    @bot.event
    async def on_ready() -> None:
        nonlocal started_scheduler, ensured_task_panel
        LOGGER.info("Discord bot online as %s", bot.user)
        if not ensured_task_panel:
            try:
                await bot.ensure_task_panel_message()
            except Exception:
                LOGGER.exception("Task panel setup failed during startup")
            ensured_task_panel = True
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
