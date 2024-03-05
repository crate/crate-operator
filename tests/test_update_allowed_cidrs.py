# CrateDB Kubernetes Operator
#
# Licensed to Crate.IO GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

from unittest import mock

import pytest
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi

from crate.operator.constants import (
    API_GROUP,
    KOPF_STATE_STORE_PREFIX,
    RESOURCE_CRATEDB,
)
from crate.operator.grand_central import read_grand_central_ingress
from crate.operator.webhooks import (
    WebhookAction,
    WebhookEvent,
    WebhookFeedbackPayload,
    WebhookOperation,
    WebhookStatus,
)

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    is_kopf_handler_finished,
    start_cluster,
    was_notification_sent,
)

pytestmark = [pytest.mark.k8s, pytest.mark.asyncio]


@pytest.mark.parametrize(
    "initial, updated",
    [
        (None, ["0.0.0.0/0", "192.168.1.1/32"]),
        (["0.0.0.0/0"], ["0.0.0.0/0", "192.168.1.1/32"]),
        (["0.0.0.0/0"], []),
    ],
)
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_update_cidrs(
    mock_send_notification: mock.AsyncMock,
    initial,
    updated,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
        wait_for_healthy=True,
        additional_cluster_spec={
            "allowedCIDRs": initial,
            "externalDNS": "my-crate-cluster.aks1.eastus.azure.cratedb-dev.net.",
        },
        grand_central_spec={
            "backendEnabled": True,
            "backendImage": "cloud.registry.cr8.net/crate/grand-central:latest",
            "apiUrl": "https://my-cratedb-api.cloud/",
            "jwkUrl": "https://my-cratedb-api.cloud/api/v2/meta/jwk/",
        },
    )

    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        mock.call(
            namespace.metadata.name,
            name,
            WebhookEvent.FEEDBACK,
            WebhookFeedbackPayload(
                message="The cluster has been created successfully.",
                operation=WebhookOperation.CREATE,
                action=WebhookAction.CREATE,
            ),
            WebhookStatus.SUCCESS,
            mock.ANY,
        ),
        err_msg="Did not notify cluster creation status update.",
        timeout=DEFAULT_TIMEOUT,
    )

    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "replace",
                "path": "/spec/cluster/allowedCIDRs",
                "value": updated,
            }
        ],
    )

    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        mock.call(
            namespace.metadata.name,
            name,
            WebhookEvent.FEEDBACK,
            WebhookFeedbackPayload(
                message="Updating IP Network Whitelist.",
                operation=WebhookOperation.UPDATE,
                action=WebhookAction.ALLOWED_CIDR_UPDATE,
            ),
            WebhookStatus.IN_PROGRESS,
            mock.ANY,
        ),
        err_msg="Did not notify IP Network status update.",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/service_cidr_changes/spec.cluster.allowedCIDRs",
        err_msg="Scaling has not finished",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        _are_source_ranges_updated,
        core,
        name,
        namespace.metadata.name,
        updated,
        err_msg="Source ranges have not been updated to the expected ones",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        mock.call(
            namespace.metadata.name,
            name,
            WebhookEvent.FEEDBACK,
            WebhookFeedbackPayload(
                message="IP Network Whitelist updated successfully.",
                operation=WebhookOperation.UPDATE,
                action=WebhookAction.ALLOWED_CIDR_UPDATE,
            ),
            WebhookStatus.SUCCESS,
            mock.ANY,
        ),
        err_msg="Did not notify IP Network status update.",
        timeout=DEFAULT_TIMEOUT,
    )


async def _are_source_ranges_updated(core, name, namespace, cidr_list):
    service = await core.read_namespaced_service(f"crate-{name}", namespace)
    ingress = await read_grand_central_ingress(namespace=namespace, name=name)
    actual = cidr_list if len(cidr_list) > 0 else None

    return (
        service.spec.load_balancer_source_ranges == actual
        and ingress.metadata.annotations.get(
            "nginx.ingress.kubernetes.io/whitelist-source-range"
        )
        == ",".join(cidr_list)
    )
