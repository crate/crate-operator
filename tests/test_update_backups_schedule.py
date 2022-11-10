from unittest import mock

import pytest
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi
from kubernetes_asyncio.client.api.batch_v1_api import BatchV1Api

from crate.operator.constants import API_GROUP, RESOURCE_CRATEDB
from crate.operator.cratedb import connection_factory
from crate.operator.webhooks import WebhookEvent, WebhookStatus

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    is_cluster_healthy,
    is_cronjob_schedule_matching,
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

    host, password = await start_cluster(
        name, namespace, core, coapi, 1, backups_spec=backups_spec
    )
    conn_factory = connection_factory(host, password)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        conn_factory,
        1,
        err_msg="Cluster wasn't healthy",
        timeout=DEFAULT_TIMEOUT,
    )

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
        is_cronjob_schedule_matching,
        batch,
        namespace.metadata.name,
        f"create-snapshot-{name}",
        body_changes[0]["value"],
        err_msg="The backup cronjob schedule does not match",
        timeout=DEFAULT_TIMEOUT,
    )

    notification_success_call = mock.call(
        WebhookEvent.BACKUP_SCHEDULE_UPDATE,
        WebhookStatus.SUCCESS,
        namespace.metadata.name,
        name,
        backup_schedule_data=mock.ANY,
        unsafe=mock.ANY,
        logger=mock.ANY,
    )

    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        notification_success_call,
        err_msg="A success notification was expected but was not sent",
        timeout=DEFAULT_TIMEOUT,
    )
