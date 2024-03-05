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

from crate.operator.webhooks import (
    WebhookEvent,
    WebhookInfoChangedPayload,
    WebhookStatus,
)
from tests.utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    start_cluster,
    was_notification_sent,
)


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_get_external_ip(
    mock_send_notification: mock.AsyncMock,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    ip, _ = await start_cluster(name, namespace, core, coapi, 1, wait_for_healthy=False)

    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        mock.call(
            namespace.metadata.name,
            name,
            WebhookEvent.INFO_CHANGED,
            WebhookInfoChangedPayload(external_ip=ip),
            WebhookStatus.SUCCESS,
            mock.ANY,
            unsafe=True,
        ),
        err_msg="Did not notify external IP being added to the service.",
        timeout=DEFAULT_TIMEOUT * 5,  # can take a while to obtain external IP
    )
