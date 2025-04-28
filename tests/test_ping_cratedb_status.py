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

import logging
from asyncio import TimeoutError
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp.client_exceptions import ClientConnectorError

from crate.operator.handlers.handle_ping_cratedb_status import (
    CLUSTER_STATUS_KEY,
    ping_cratedb_status,
)
from crate.operator.prometheus import PrometheusClusterStatus


@pytest.fixture
def mock_cratedb_connection():
    mock_cursor = AsyncMock()
    mock_cursor.__aenter__.return_value = mock_cursor
    mock_cursor.__aexit__.return_value = None

    mock_conn = AsyncMock()
    mock_conn.__aenter__.return_value = mock_conn
    mock_conn.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cursor

    patcher = patch("crate.operator.cratedb.aiopg.connect", return_value=mock_conn)
    mocked_connect = patcher.start()

    yield {
        "mock_connect": mocked_connect,
        "mock_conn": mock_conn,
        "mock_cursor": mock_cursor,
    }

    patcher.stop()


@pytest.mark.asyncio
async def test_ping_cratedb_status_success(mock_cratedb_connection):
    namespace = "test-ns"
    name = "test-cluster"
    cluster_name = "test-cluster"
    desired_instances = 1

    patch_obj = MagicMock()
    logger = MagicMock(spec=logging.Logger)

    mock_api_client = AsyncMock()
    mock_api_client.__aenter__.return_value = mock_api_client

    mock_core = MagicMock()

    with (
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.GlobalApiClient",
            return_value=mock_api_client,
        ),
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.CoreV1Api",
            return_value=mock_core,
        ),
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.get_host",
            new_callable=AsyncMock,
            return_value="crate.db.local",
        ),
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.get_system_user_password",  # noqa
            new_callable=AsyncMock,
            return_value="secret-password",
        ),
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.get_healthiness",
            new_callable=AsyncMock,
            return_value="GREEN",
        ),
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.report_cluster_status"
        ) as mock_report,
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.webhook_client.send_notification",  # noqa
            new_callable=AsyncMock,
        ),
    ):

        await ping_cratedb_status(
            namespace=namespace,
            name=name,
            cluster_name=cluster_name,
            desired_instances=desired_instances,
            patch=patch_obj,
            logger=logger,
        )

    mock_conn = mock_cratedb_connection["mock_conn"]
    mock_cursor = mock_cratedb_connection["mock_cursor"]

    mock_conn.__aenter__.assert_awaited_once()
    mock_conn.cursor.assert_called_once()
    mock_cursor.__aenter__.assert_awaited_once()

    patch_obj.status.__setitem__.assert_called_once_with(
        CLUSTER_STATUS_KEY, {"health": PrometheusClusterStatus.GREEN.name}
    )

    mock_report.assert_called_once_with(
        name, cluster_name, namespace, PrometheusClusterStatus.GREEN
    )


fake_key = MagicMock()
fake_key.ssl = None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "side_effect, expected_log_method, expected_log_message",
    [
        (
            ClientConnectorError(fake_key, OSError("kaboom")),
            "warning",
            "Transient Kubernetes API connection error during health check",
        ),
        (
            TimeoutError("timed out"),
            "warning",
            "Timeout while connecting to CrateDB during health check",
        ),
        (
            Exception("generic failure"),
            "warning",
            "Unexpected error during CrateDB health check",
        ),
    ],
)
async def test_ping_cratedb_status_exceptions(
    side_effect, expected_log_method, expected_log_message
):
    namespace = "test-ns"
    name = "test-cluster"
    cluster_name = "test-cluster"
    desired_instances = 1

    patch_obj = MagicMock()
    logger = MagicMock(spec=logging.Logger)

    mock_api_client_cm = AsyncMock()
    mock_core = MagicMock()

    with (
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.GlobalApiClient",
            return_value=mock_api_client_cm,
        ),
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.CoreV1Api",
            return_value=mock_core,
        ),
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.get_host",
            side_effect=side_effect,
        ),
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.get_system_user_password",  # noqa
            new_callable=AsyncMock,
        ),
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.report_cluster_status"
        ) as mock_report,
        patch(
            "crate.operator.handlers.handle_ping_cratedb_status.webhook_client.send_notification",  # noqa
            new_callable=AsyncMock,
        ),
    ):

        await ping_cratedb_status(
            namespace=namespace,
            name=name,
            cluster_name=cluster_name,
            desired_instances=desired_instances,
            patch=patch_obj,
            logger=logger,
        )

        # validate patch to status UNREACHABLE
        patch_obj.status.__setitem__.assert_called_once_with(
            CLUSTER_STATUS_KEY, {"health": PrometheusClusterStatus.UNREACHABLE.name}
        )

        # validate the correct logger method was called
        log_method = getattr(logger, expected_log_method)
        log_method.assert_called_once()
        assert expected_log_message in log_method.call_args[0][0]

        # check status was reported
        mock_report.assert_called_once_with(
            name, cluster_name, namespace, PrometheusClusterStatus.UNREACHABLE
        )
