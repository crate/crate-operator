import logging
from typing import Any

import kopf

from crate.operator.backup import change_backup_schedule
from crate.operator.config import config
from crate.operator.utils import crate
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.webhooks import (
    WebhookBackupScheduleUpdatePayload,
    WebhookEvent,
    WebhookStatus,
)


class BackupScheduleUpdateSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(config.SCALING_TIMEOUT))
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

        diff_found = None
        for d in diff:
            if d.operation == "change" and d.field == (
                "spec",
                "backups",
                "aws",
                "cron",
            ):
                diff_found = d

        assert diff_found is not None
        payload = WebhookBackupScheduleUpdatePayload(new_schedule=d.new)

        await self.send_notification_now(
            logger,
            WebhookEvent.BACKUP_SCHEDULE_UPDATE,
            payload,
            WebhookStatus.IN_PROGRESS,
        )

        await change_backup_schedule(namespace, name, payload["new_schedule"])

        self.schedule_notification(
            WebhookEvent.BACKUP_SCHEDULE_UPDATE,
            payload,
            WebhookStatus.SUCCESS,
        )
