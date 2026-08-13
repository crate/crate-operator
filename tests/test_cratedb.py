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

from crate.operator.cratedb import (
    create_user,
    get_healthiness,
    get_number_of_nodes,
    is_cluster_healthy,
)
from crate.operator.webhooks import (
    WebhookClusterHealthPayload,
    WebhookEvent,
    WebhookStatus,
)

from .utils import (
    CRATE_VERSION,
    CRATE_VERSION_WITH_JWT,
    DEFAULT_TIMEOUT,
    assert_wait_for,
    start_cluster,
    was_notification_sent,
)

pytestmark = pytest.mark.asyncio


@mock.patch("crate.operator.cratedb.get_cratedb_resource")
async def test_create_user(mock_get_cratedb_resource: mock.AsyncMock, faker):
    password = faker.password()
    username = faker.user_name()
    username_ident = f'"{username}"'  # This is to check that "quote_ident" is called
    namespace = faker.uuid4()
    name = faker.domain_word()
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (0,)
    mock_get_cratedb_resource.return_value = {
        "spec": {"cluster": {"version": CRATE_VERSION}}
    }
    with mock.patch("crate.operator.cratedb.quote_ident", return_value=username_ident):
        await create_user(cursor, namespace, name, username, password)

    cursor.fetchone.assert_awaited_once()
    cursor.execute.assert_has_awaits(
        [
            mock.call(
                "SELECT count(*) = 1 FROM sys.users WHERE name = %s", (username,)
            ),
            mock.call(
                f"CREATE USER {username_ident} WITH (password = %s)", (password,)
            ),
            mock.call(f"GRANT ALL TO {username_ident}"),
            mock.call(f"DENY ALL ON SCHEMA gc TO {username_ident}"),
        ]
    )


@mock.patch("crate.operator.cratedb.get_cratedb_resource")
async def test_create_user_duplicate(mock_get_cratedb_resource: mock.AsyncMock, faker):
    password = faker.password()
    username = faker.user_name()
    username_ident = f'"{username}"'  # This is to check that "quote_ident" is called
    namespace = faker.uuid4()
    name = faker.domain_word()
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (1,)
    mock_get_cratedb_resource.return_value = {
        "spec": {"cluster": {"version": CRATE_VERSION}}
    }
    with mock.patch("crate.operator.cratedb.quote_ident", return_value=username_ident):
        await create_user(cursor, namespace, name, username, password)

    cursor.fetchone.assert_awaited_once()
    cursor.execute.assert_has_awaits(
        [
            mock.call(
                "SELECT count(*) = 1 FROM sys.users WHERE name = %s", (username,)
            ),
            mock.call(f"GRANT ALL TO {username_ident}"),
            mock.call(f"DENY ALL ON SCHEMA gc TO {username_ident}"),
        ]
    )


@mock.patch("crate.operator.cratedb.get_cratedb_resource")
async def test_create_user_with_jwt(mock_get_cratedb_resource: mock.AsyncMock, faker):
    password = faker.password()
    username = faker.user_name()
    username_ident = f'"{username}"'  # This is to check that "quote_ident" is called
    namespace = faker.uuid4()
    name = faker.domain_word()
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (0,)
    mock_get_cratedb_resource.return_value = {
        "spec": {
            "cluster": {"version": CRATE_VERSION_WITH_JWT},
            "grandCentral": {"jwkUrl": "https://my-cratedb-api.cloud/api/v2/meta/jwk/"},
        }
    }
    with mock.patch("crate.operator.cratedb.quote_ident", return_value=username_ident):
        await create_user(cursor, namespace, name, username, password)

    cursor.fetchone.assert_awaited_once()
    cursor.execute.assert_has_awaits(
        [
            mock.call(
                "SELECT count(*) = 1 FROM sys.users WHERE name = %s", (username,)
            ),
            mock.call(
                f"""CREATE USER {username_ident} WITH (password = %s, """
                f"""jwt = {{"iss" = %s, "username" = %s, "aud" = %s}})""",
                (
                    password,
                    "https://my-cratedb-api.cloud/api/v2/meta/jwk/",
                    username,
                    name,
                ),
            ),
            mock.call(f"GRANT ALL TO {username_ident}"),
            mock.call(f"DENY ALL ON SCHEMA gc TO {username_ident}"),
        ]
    )


@mock.patch("crate.operator.cratedb.get_cratedb_resource")
async def test_create_user_custom_privileges(
    mock_get_cratedb_resource: mock.AsyncMock, faker
):
    password = faker.password()
    username = faker.user_name()
    username_ident = f'"{username}"'  # This is to check that "quote_ident" is called
    namespace = faker.uuid4()
    name = faker.domain_word()
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (0,)
    mock_get_cratedb_resource.return_value = {
        "spec": {"cluster": {"version": CRATE_VERSION}}
    }
    with mock.patch("crate.operator.cratedb.quote_ident", return_value=username_ident):
        await create_user(cursor, namespace, name, username, password, ["DQL", "DML"])

    cursor.fetchone.assert_awaited_once()
    cursor.execute.assert_has_awaits(
        [
            mock.call(
                "SELECT count(*) = 1 FROM sys.users WHERE name = %s", (username,)
            ),
            mock.call(
                f"CREATE USER {username_ident} WITH (password = %s)", (password,)
            ),
            mock.call(f"GRANT DQL,DML TO {username_ident}"),
            mock.call(f"DENY ALL ON SCHEMA gc TO {username_ident}"),
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
    cursor.execute.assert_awaited_once_with(
        "SELECT max(severity) FROM ("
        "  SELECT CAST(max(severity) as integer) as severity FROM sys.health"
        "  UNION ALL"
        "  SELECT CAST(3 as integer) as severity FROM sys.cluster"
        "    WHERE master_node is null"
        ") sub;"
    )
    cursor.fetchone.assert_awaited_once()


class _FakeCursor:
    def __init__(self, rows):
        self._rows = list(rows)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def execute(self, sql, *args):
        return None

    async def fetchone(self):
        return self._rows.pop(0)


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def cursor(self):
        return _FakeCursor(self._rows)


class _conn_factory_for:
    """
    A ``conn_factory`` that returns one reading per connection.

    ``readings`` is a list of ``(num_nodes, healthiness)`` tuples, one per poll
    ``is_cluster_healthy`` performs. ``healthiness`` of ``None`` simulates an
    empty ``sys.health`` (a cluster with no tables). ``calls`` records how many
    connections were opened, so tests can assert that a failing reading
    short-circuits the remaining polls.
    """

    def __init__(self, readings):
        self._remaining = list(readings)
        self.calls = 0

    def __call__(self):
        self.calls += 1
        num_nodes, healthiness = self._remaining.pop(0)
        rows = [
            (num_nodes,),
            (healthiness,) if healthiness is not None else None,
        ]
        return _FakeConn(rows)


@pytest.mark.parametrize(
    "readings, expected",
    [
        # Steady GREEN across every poll -> healthy.
        ([(3, 1), (3, 1), (3, 1)], True),
        # Empty cluster (no tables): no health rows -> healthy.
        ([(3, None), (3, None), (3, None)], True),
        # A GREEN cluster under ingest is still GREEN on every poll -> healthy.
        # (Shards initializing/relocating from new partitions or rebalancing
        # must NOT be treated as unhealthy; that would stall an upgrade.)
        ([(3, 1), (3, 1), (3, 1)], True),
        # Not all nodes back yet -> not healthy (short-circuits on first poll).
        ([(2, 1)], False),
        # YELLOW/RED -> not healthy.
        ([(3, 2)], False),
        ([(3, 3)], False),
    ],
)
async def test_is_cluster_healthy_readings(readings, expected):
    logger = mock.Mock()
    conn_factory = _conn_factory_for(readings)
    result = await is_cluster_healthy(
        conn_factory,
        expected_nodes=3,
        logger=logger,
        stability_checks=len(readings),
        stability_delay=0,
    )
    assert result is expected


async def test_is_cluster_healthy_requires_stable_readings():
    logger = mock.Mock()
    # First poll catches a stale GREEN, the second sees the cluster go YELLOW
    # as the master recomputes health. A single snapshot used to pass here and
    # let the next node be terminated too early (crate/cloud#3062); requiring
    # consecutive readings must now report the cluster as not healthy.
    conn_factory = _conn_factory_for([(3, 1), (3, 2)])
    result = await is_cluster_healthy(
        conn_factory,
        expected_nodes=3,
        logger=logger,
        stability_checks=3,
        stability_delay=0,
    )
    assert result is False


async def test_is_cluster_healthy_recovers_after_flap():
    logger = mock.Mock()
    # A cluster that is briefly YELLOW then settles to a stable GREEN across the
    # required number of consecutive polls is healthy.
    conn_factory = _conn_factory_for([(3, 1), (3, 1), (3, 1)])
    result = await is_cluster_healthy(
        conn_factory,
        expected_nodes=3,
        logger=logger,
        stability_checks=3,
        stability_delay=0,
    )
    assert result is True
    assert conn_factory.calls == 3


async def test_is_cluster_healthy_stops_polling_on_first_failure():
    logger = mock.Mock()
    # A failing first reading must short-circuit without polling again, so only
    # one connection is opened even though three checks were requested.
    conn_factory = _conn_factory_for([(2, 1)])
    result = await is_cluster_healthy(
        conn_factory,
        expected_nodes=3,
        logger=logger,
        stability_checks=3,
        stability_delay=0,
    )
    assert result is False
    assert conn_factory.calls == 1


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_cratedb_health_ping(
    mock_send_notification: mock.AsyncMock,
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
    )

    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        mock.call(
            namespace.metadata.name,
            name,
            WebhookEvent.HEALTH,
            WebhookClusterHealthPayload(status="GREEN"),
            WebhookStatus.SUCCESS,
            mock.ANY,
            retry=False,
        ),
        err_msg="Failed to ping cluster.",
        timeout=DEFAULT_TIMEOUT * 5,
    )
