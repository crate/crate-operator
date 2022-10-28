import logging
from typing import Any

from crate.operator.backup import change_backup_schedule
from crate.operator.utils import crate
from crate.operator.utils.kopf import StateBasedSubHandler
import kopf
from crate.operator.webhooks import WebhookEvent, WebhookStatus


class BackupScheduleUpdateSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_create_failed_notification)
    async def handle(  # type: ignore
            self,
            namespace: str,
            name: str,
            spec: kopf.Spec,
            old: kopf.Body,
            diff: kopf.Diff,
            logger: logging.Logger,
            **kwargs: Any,
    ):
        payload = {"old_schedule": None, "new_schedule": None}
        await change_backup_schedule(logger, namespace, name, "new_value")
        await self.send_notification_now(
            logger,
            WebhookEvent.COMPUTE_CHANGED,
            payload,
            WebhookStatus.IN_PROGRESS,
        )

        self.schedule_notification(
            WebhookEvent.BACKUP_SCHEDULE_CHANGED,
            payload,
            WebhookStatus.SUCCESS,
        )
