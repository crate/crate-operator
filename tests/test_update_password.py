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


async def is_password_set(host: str, system_password: str, user: str) -> bool:
    try:
        async with get_connection(host, system_password, user, timeout=5.0) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                row = await cursor.fetchone()
                return bool(row and row[0] == 1)
    except (DatabaseError, OperationalError, asyncio.exceptions.TimeoutError):
        return False


async def test_update_cluster_password(faker, namespace, kopf_runner, api_client):
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
                    name=f"user-{name}", labels={LABEL_USER_PASSWORD: "true"}
                ),
                type="Opaque",
            ),
        ),
    )

    users = [
        {
            "name": username,
            "password": {
                "secretKeyRef": {
                    "key": "password",
                    "name": f"user-{name}",
                }
            },
        },
    ]

    host, password = await start_cluster(name, namespace, core, coapi, 1, users=users)

    await core.patch_namespaced_secret(
        namespace=namespace.metadata.name,
        name=f"user-{name}",
        body=V1Secret(
            data={"password": b64encode(new_password)},
        ),
    )

    await assert_wait_for(
        True,
        is_password_set,
        host,
        new_password,
        username,
        timeout=DEFAULT_TIMEOUT * 5,
    )
