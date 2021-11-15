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
import base64
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

from .utils import DEFAULT_TIMEOUT, assert_wait_for, start_cluster

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


async def was_license_set(
    mock_obj: mock.AsyncMock, core, namespace, master_node_pod, has_ssl, license
):
    try:
        mock_obj.assert_awaited_once_with(
            core, namespace, master_node_pod, has_ssl, license, mock.ANY
        )
    except AssertionError:
        return False
    else:
        return True


@mock.patch("crate.operator.bootstrap.bootstrap_license")
@mock.patch("crate.operator.bootstrap.bootstrap_system_user")
async def test_bootstrap_license(
    _bootstrap_system_user: mock.AsyncMock,
    bootstrap_license_mock: mock.AsyncMock,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    license = base64.b64encode(faker.binary(64)).decode()

    await core.create_namespaced_secret(
        namespace=namespace.metadata.name,
        body=V1Secret(
            data={"license": b64encode(license)},
            metadata=V1ObjectMeta(name=f"license-{name}"),
            type="Opaque",
        ),
    )
    await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
        wait_for_healthy=False,
        additional_cluster_spec={
            "license": {
                "secretKeyRef": {"key": "license", "name": f"license-{name}"},
            },
        },
    )
    await assert_wait_for(
        True,
        was_license_set,
        bootstrap_license_mock,
        mock.ANY,
        namespace.metadata.name,
        f"crate-data-hot-{name}-0",
        False,
        {"secretKeyRef": {"key": "license", "name": f"license-{name}"}},
        timeout=DEFAULT_TIMEOUT * 3,
    )


@pytest.mark.parametrize("allowed_cidrs", [None, ["1.1.1.1/32"]])
async def test_bootstrap_users(
    allowed_cidrs, faker, namespace, kopf_runner, api_client
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

    users = [
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
