import logging

import kopf

from crate.operator.backup import update_backup_schedule_in_cronjob
from crate.operator.utils.kopf import send_webhook_notification
from crate.operator.webhooks import (
    WebhookBackupScheduleUpdatePayload,
    WebhookEvent,
    WebhookStatus,
)


async def update_backup_schedule(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    logger: logging.Logger,
    **_kwargs,
):
    d = diff[0]
    if d.operation != "change" and d.field != (
        "spec",
        "backups",
        "aws",
        "cron",
    ):
        return

    payload = WebhookBackupScheduleUpdatePayload(backup_schedule=d.new)

    await send_webhook_notification(
        namespace,
        name,
        logger,
        WebhookEvent.BACKUP_SCHEDULE_UPDATE,
        payload,
        WebhookStatus.IN_PROGRESS,
    )

    await update_backup_schedule_in_cronjob(namespace, name, payload["backup_schedule"])

    await send_webhook_notification(
        namespace,
        name,
        logger,
        WebhookEvent.BACKUP_SCHEDULE_UPDATE,
        payload,
        WebhookStatus.SUCCESS,
    )
