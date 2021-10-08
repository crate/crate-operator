from unittest import mock

import pytest
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi

from crate.operator.utils.kubeapi import get_service_public_hostname
from crate.operator.webhooks import (
    WebhookEvent,
    WebhookInfoChangedPayload,
    WebhookStatus,
)
from tests.utils import DEFAULT_TIMEOUT, assert_wait_for, start_cluster


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_get_external_ip(
    mock_send_notification: mock.AsyncMock,
    faker,
    namespace,
    cleanup_handler,
    kopf_runner,
    api_client,
):
    """
    Please note that this test requires having a Load Balancer implementation.

    This will work fine on Azure, however needs MetalLB enabled if running on microk8s.
    """
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    await start_cluster(
        name, namespace, cleanup_handler, core, coapi, 1, wait_for_healthy=False
    )
    ip = await get_service_public_hostname(core, namespace.metadata.name, name)

    await assert_wait_for(
        True,
        _notification_sent,
        mock_send_notification,
        err_msg="Did not notify external IP being added to the service.",
        timeout=DEFAULT_TIMEOUT * 5,  # can take a while to obtain external IP
    )

    mock_send_notification.assert_called_once_with(
        namespace.metadata.name,
        name,
        WebhookEvent.INFO_CHANGED,
        WebhookInfoChangedPayload(external_ip=ip),
        WebhookStatus.SUCCESS,
        mock.ANY,
        unsafe=True,
    )


async def _notification_sent(mock_send_notification: mock.AsyncMock):
    return mock_send_notification.call_count > 0
