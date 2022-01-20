# CrateDB Kubernetes Operator
# Copyright (C) 2020 Crate.IO GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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


async def is_password_set(host: str, system_password: str, user: str) -> bool:
    try:
        async with get_connection(host, system_password, user, timeout=5.0) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                row = await cursor.fetchone()
                return bool(row and row[0] == 1)
    except (DatabaseError, OperationalError, asyncio.exceptions.TimeoutError):
        return False


@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_update_cluster_password(
    mock_send_notification: mock.AsyncMock, faker, namespace, kopf_runner, api_client
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    password = faker.password(length=40)
    new_password = faker.password(length=40)
    username = faker.user_name()

    await asyncio.gather(
        core.create_namespaced_secret(
            namespace=namespace.metadata.name,
            body=V1Secret(
                data={"password": b64encode(password)},
                metadata=V1ObjectMeta(
                    name=f"user-password-{name}-0", labels={LABEL_USER_PASSWORD: "true"}
                ),
                type="Opaque",
            ),
        ),
    )

    users: List[Mapping[str, Any]] = [
        {
            "name": username,
            "password": {
                "secretKeyRef": {
                    "key": "password",
                    "name": f"user-password-{name}-0",
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
                message="The cluster has been created successfully.",
                operation=WebhookOperation.CREATE,
            ),
            WebhookStatus.SUCCESS,
            mock.ANY,
        ),
        err_msg="Did not notify cluster creation status update.",
        timeout=DEFAULT_TIMEOUT,
    )

    await core.patch_namespaced_secret(
        namespace=namespace.metadata.name,
        name=f"user-password-{name}-0",
        body=V1Secret(
            data={"password": b64encode(new_password)},
        ),
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
                message="Updating password.",
                operation=WebhookOperation.UPDATE,
            ),
            WebhookStatus.IN_PROGRESS,
            mock.ANY,
        ),
        err_msg="Did not notify user password status start.",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        is_password_set,
        host,
        new_password,
        username,
        timeout=DEFAULT_TIMEOUT * 5,
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
                message="Password updated successfully.",
                operation=WebhookOperation.UPDATE,
            ),
            WebhookStatus.SUCCESS,
            mock.ANY,
        ),
        err_msg="Did not notify user password status success.",
        timeout=DEFAULT_TIMEOUT,
    )
