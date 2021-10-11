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

from unittest import mock

import pytest
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi

from crate.operator.cratedb import create_user, get_healthiness, get_number_of_nodes
from crate.operator.webhooks import (
    WebhookClusterHealthPayload,
    WebhookEvent,
    WebhookStatus,
)
from tests.test_load_balancer_updates import _notification_sent

from .utils import DEFAULT_TIMEOUT, assert_wait_for, start_cluster

pytestmark = pytest.mark.asyncio


async def test_create_user(faker):
    password = faker.password()
    username = faker.user_name()
    username_ident = f'"{username}"'  # This is to check that "quote_ident" is called
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (0,)
    with mock.patch("crate.operator.cratedb.quote_ident", return_value=username_ident):
        await create_user(cursor, username, password)

    cursor.fetchone.assert_awaited_once()
    cursor.execute.assert_has_awaits(
        [
            mock.call(
                "SELECT count(*) = 1 FROM sys.users WHERE name = %s", (username,)
            ),
            mock.call(
                f"CREATE USER {username_ident} WITH (password = %s)", (password,)
            ),
            mock.call(f"GRANT ALL PRIVILEGES TO {username_ident}"),
        ]
    )


async def test_create_user_duplicate(faker):
    password = faker.password()
    username = faker.user_name()
    username_ident = f'"{username}"'  # This is to check that "quote_ident" is called
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (1,)
    with mock.patch("crate.operator.cratedb.quote_ident", return_value=username_ident):
        await create_user(cursor, username, password)

    cursor.fetchone.assert_awaited_once()
    cursor.execute.assert_has_awaits(
        [
            mock.call(
                "SELECT count(*) = 1 FROM sys.users WHERE name = %s", (username,)
            ),
            mock.call(f"GRANT ALL PRIVILEGES TO {username_ident}"),
        ]
    )


@pytest.mark.parametrize("n", [-1, 0, 1, 3])
async def test_get_number_of_nodes(n):
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (n,) if n is not None else None
    assert (await get_number_of_nodes(cursor)) == n
    cursor.execute.assert_awaited_once_with("SELECT COUNT(*) FROM sys.nodes")
    cursor.fetchone.assert_awaited_once()


@pytest.mark.parametrize("healthiness", [None, 0, 1, 2])
async def test_get_healthiness(healthiness):
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (healthiness,) if healthiness is not None else None
    assert (await get_healthiness(cursor)) == healthiness
    cursor.execute.assert_awaited_once_with("SELECT MAX(severity) FROM sys.health")
    cursor.fetchone.assert_awaited_once()


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_cratedb_health_ping(
    mock_send_notification: mock.AsyncMock,
    faker,
    namespace,
    cleanup_handler,
    kopf_runner,
    api_client,
):

    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    await start_cluster(
        name,
        namespace,
        cleanup_handler,
        core,
        coapi,
        1,
        wait_for_healthy=True,
    )

    await assert_wait_for(
        True,
        _notification_sent,
        mock_send_notification,
        err_msg="Failed to ping cluster.",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    mock_send_notification.assert_called_with(
        namespace.metadata.name,
        name,
        WebhookEvent.HEALTH,
        WebhookClusterHealthPayload(status="GREEN"),
        WebhookStatus.SUCCESS,
        mock.ANY,
        unsafe=False,
    )
