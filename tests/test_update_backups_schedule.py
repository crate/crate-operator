from time import sleep
from unittest import mock

import pytest
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi
from kubernetes_asyncio.client.api.batch_v1_api import BatchV1Api

from crate.operator.constants import API_GROUP, RESOURCE_CRATEDB
from crate.operator.webhooks import WebhookEvent, WebhookStatus

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    is_kopf_handler_finished,
    start_cluster,
    was_notification_sent,
)


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client._send")
async def test_update_backups_schedule(
    mock_send_notification,
    faker,
    namespace,
    kopf_runner,
    api_client,
):

    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    batch = BatchV1Api(api_client)
    name = faker.domain_word()

    backups_spec = {
        "aws": {
            "accessKeyId": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
            "basePath": faker.uri_path() + "/",
            "cron": "1 2 3 4 5",
            "region": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
            "bucket": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
            "secretAccessKey": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
            "endpointUrl": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                    "optional": True,
                },
            },
        },
    }

    await start_cluster(name, namespace, core, coapi, 1, backups_spec=backups_spec)
    cronjob_pre = await batch.read_namespaced_cron_job(
        namespace=namespace.metadata.name, name=f"create-snapshot-{name}"
    )
    assert cronjob_pre.spec.schedule == backups_spec["aws"]["cron"]

    body_changes = [
        {
            "op": "replace",
            "path": "/spec/backups/aws/cron",
            "value": "39 * * * *",
        },
    ]

    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=body_changes,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.backup_schedule_update",
        err_msg="Backup schedule change has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.after_cluster_update",
        err_msg="After cluster update has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.notify_success_update",
        err_msg="Success notification has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    sleep(10)

    cronjob_post = await batch.read_namespaced_cron_job(
        namespace=namespace.metadata.name, name=f"create-snapshot-{name}"
    )
    assert cronjob_post.spec.schedule == body_changes[0]["value"]

    notification_success_call = mock.call(
        WebhookEvent.BACKUP_SCHEDULE_CHANGED,
        WebhookStatus.SUCCESS,
        namespace.metadata.name,
        name,
        backup_schedule_changed_data=mock.ANY,
        unsafe=mock.ANY,
        logger=mock.ANY,
    )
    assert await was_notification_sent(
        mock_send_notification=mock_send_notification, call=notification_success_call
    ), "A success notification was expected but was not sent"
