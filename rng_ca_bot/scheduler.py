from __future__ import annotations

import asyncio
import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import Settings
from .discord_app import RngCABot
from .sync_service import SyncService

LOGGER = logging.getLogger(__name__)


class DailyScanScheduler:
    def __init__(self, settings: Settings, bot: RngCABot, sync_service: SyncService) -> None:
        self.settings = settings
        self.bot = bot
        self.sync_service = sync_service
        self.scheduler = AsyncIOScheduler()

    def start(self) -> None:
        try:
            tz = ZoneInfo(self.settings.timezone)
        except Exception:
            LOGGER.warning("Invalid TIMEZONE '%s'; defaulting to UTC", self.settings.timezone)
            tz = ZoneInfo("UTC")

        trigger = CronTrigger.from_crontab(self.settings.scan_cron, timezone=tz)
        self.scheduler.add_job(
            self._run_scan_job,
            trigger=trigger,
            id="daily_scan",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
        self.scheduler.start()
        LOGGER.info(
            "Daily scan scheduler started with cron '%s' timezone '%s'",
            self.settings.scan_cron,
            tz,
        )

    async def stop(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    async def _run_scan_job(self) -> None:
        LOGGER.info("Starting scheduled scan run")
        try:
            result = await asyncio.to_thread(self.sync_service.run_daily_scan, "scheduled")
        except Exception:
            LOGGER.exception("Scheduled scan failed")
            return

        await self.bot.post_scan_status(result)
