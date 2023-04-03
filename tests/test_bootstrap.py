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

import asyncio
from typing import Any, List, Mapping
from unittest import mock

import pytest
from kubernetes_asyncio.client import (
    CoreV1Api,
    CustomObjectsApi,
    V1ObjectMeta,
    V1Secret,
)
from psycopg2 import DatabaseError, OperationalError

from crate.operator.constants import LABEL_USER_PASSWORD
from crate.operator.cratedb import get_connection
from crate.operator.utils.formatting import b64encode
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
    start_cluster,
    was_notification_sent,
)

pytestmark = [pytest.mark.k8s, pytest.mark.asyncio]


async def does_user_exist(host: str, password: str, username: str) -> bool:
    try:
        async with get_connection(host, password, username, timeout=5.0) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                row = await cursor.fetchone()
                return bool(row and row[0] == 1)
    except (DatabaseError, OperationalError, asyncio.exceptions.TimeoutError):
        return False


@pytest.mark.parametrize("allowed_cidrs", [None, ["1.1.1.1/32"]])
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_bootstrap_users(
    mock_send_notification: mock.AsyncMock,
    allowed_cidrs,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    password1 = faker.password(length=40)
    password2 = faker.password(length=30)
    username1 = faker.user_name()
    username2 = faker.user_name()

    await asyncio.gather(
        core.create_namespaced_secret(
            namespace=namespace.metadata.name,
            body=V1Secret(
                data={"password": b64encode(password1)},
                metadata=V1ObjectMeta(name=f"user-{name}-1"),
                type="Opaque",
            ),
        ),
        core.create_namespaced_secret(
            namespace=namespace.metadata.name,
            body=V1Secret(
                data={"password": b64encode(password2)},
                metadata=V1ObjectMeta(name=f"user-{name}-2"),
                type="Opaque",
            ),
        ),
    )

    users: List[Mapping[str, Any]] = [
        {
            "name": username1,
            "password": {
                "secretKeyRef": {
                    "key": "password",
                    "name": f"user-{name}-1",
                }
            },
        },
        {
            "name": username2,
            "password": {
                "secretKeyRef": {
                    "key": "password",
                    "name": f"user-{name}-2",
                }
            },
        },
    ]

    host, password = await start_cluster(name, namespace, core, coapi, 1, users=users)

    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        mock.call(
            namespace.metadata.name,
            name,
            WebhookEvent.FEEDBACK,
            WebhookFeedbackPayload(
                message=(
                    "Cluster creation started. Waiting for the node(s) to be "
                    "created and creating other required resources."
                ),
                operation=WebhookOperation.CREATE,
                action=WebhookAction.CREATE,
            ),
            WebhookStatus.IN_PROGRESS,
            mock.ANY,
        ),
        err_msg="Did not notify cluster creation status update.",
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

    await assert_wait_for(
        True, does_user_exist, host, password1, username1, timeout=DEFAULT_TIMEOUT * 3
    )

    await assert_wait_for(
        True, does_user_exist, host, password2, username2, timeout=DEFAULT_TIMEOUT * 3
    )

    secret_user_1 = await core.read_namespaced_secret(
        namespace=namespace.metadata.name, name=f"user-{name}-1"
    )
    secret_user_2 = await core.read_namespaced_secret(
        namespace=namespace.metadata.name, name=f"user-{name}-2"
    )

    assert LABEL_USER_PASSWORD in secret_user_1.metadata.labels
    assert LABEL_USER_PASSWORD in secret_user_2.metadata.labels
