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

from crate.operator.constants import (
    API_GROUP,
    BACKOFF_TIME,
    RESOURCE_CRATEDB,
    SYSTEM_USERNAME,
)
from crate.operator.cratedb import get_connection
from crate.operator.utils.formatting import b64encode
from crate.operator.utils.kubeapi import get_public_host, get_system_user_password

from .utils import assert_wait_for

pytestmark = [pytest.mark.k8s, pytest.mark.asyncio]


async def does_user_exist(host: str, password: str, username: str) -> bool:
    try:
        async with get_connection(host, password, username, timeout=5.0) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                row = await cursor.fetchone()
                return bool(row and row[0] == 1)
    except (DatabaseError, OperationalError):
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
    bootstrap_system_user: mock.AsyncMock,
    bootstrap_license_mock: mock.AsyncMock,
    faker,
    namespace,
    cleanup_handler,
    kopf_runner,
):
    coapi = CustomObjectsApi()
    core = CoreV1Api()
    name = faker.domain_word()
    license = base64.b64encode(faker.binary(64)).decode()

    cleanup_handler.append(
        core.delete_persistent_volume(name=f"temp-pv-{namespace.metadata.name}-{name}"),
    )
    await core.create_namespaced_secret(
        namespace=namespace.metadata.name,
        body=V1Secret(
            data={"license": b64encode(license)},
            metadata=V1ObjectMeta(name=f"license-{name}"),
            type="Opaque",
        ),
    )
    await coapi.create_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        body={
            "apiVersion": "cloud.crate.io/v1",
            "kind": "CrateDB",
            "metadata": {"name": name},
            "spec": {
                "cluster": {
                    "imageRegistry": "crate",
                    "license": {
                        "secretKeyRef": {"key": "license", "name": f"license-{name}"},
                    },
                    "name": "my-crate-cluster",
                    "version": "4.1.5",
                },
                "nodes": {
                    "data": [
                        {
                            "name": "data",
                            "replicas": 1,
                            "resources": {
                                "cpus": 0.5,
                                "memory": "1Gi",
                                "heapRatio": 0.25,
                                "disk": {
                                    "storageClass": "default",
                                    "size": "16GiB",
                                    "count": 1,
                                },
                            },
                        }
                    ]
                },
            },
        },
    )
    await assert_wait_for(
        True,
        was_license_set,
        bootstrap_license_mock,
        mock.ANY,
        namespace.metadata.name,
        f"crate-data-data-{name}-0",
        False,
        {"secretKeyRef": {"key": "license", "name": f"license-{name}"}},
        timeout=BACKOFF_TIME * 3,
    )


@mock.patch("crate.operator.bootstrap.bootstrap_license")
async def test_bootstrap_users(
    bootstrap_license_mock: mock.AsyncMock,
    faker,
    namespace,
    cleanup_handler,
    kopf_runner,
):
    coapi = CustomObjectsApi()
    core = CoreV1Api()
    name = faker.domain_word()
    password1 = faker.password(length=40)
    password2 = faker.password(length=30)
    username1 = faker.user_name()
    username2 = faker.user_name()

    cleanup_handler.append(
        core.delete_persistent_volume(name=f"temp-pv-{namespace.metadata.name}-{name}")
    )
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

    await coapi.create_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        body={
            "apiVersion": "cloud.crate.io/v1",
            "kind": "CrateDB",
            "metadata": {"name": name},
            "spec": {
                "cluster": {
                    "imageRegistry": "crate",
                    "name": "my-crate-cluster",
                    "version": "4.1.5",
                },
                "nodes": {
                    "data": [
                        {
                            "name": "data",
                            "replicas": 1,
                            "resources": {
                                "cpus": 0.5,
                                "memory": "1Gi",
                                "heapRatio": 0.25,
                                "disk": {
                                    "storageClass": "default",
                                    "size": "16GiB",
                                    "count": 1,
                                },
                            },
                        }
                    ]
                },
                "users": [
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
                ],
            },
        },
    )

    host = await asyncio.wait_for(
        get_public_host(core, namespace.metadata.name, name),
        timeout=BACKOFF_TIME * 5,  # It takes a while to retrieve an external IP on AKS.
    )

    password_system = await get_system_user_password(
        namespace.metadata.name, name, core
    )
    await assert_wait_for(
        True,
        does_user_exist,
        host,
        password_system,
        SYSTEM_USERNAME,
        timeout=BACKOFF_TIME * 5,
    )

    await assert_wait_for(
        True, does_user_exist, host, password1, username1, timeout=BACKOFF_TIME * 3,
    )

    await assert_wait_for(
        True, does_user_exist, host, password2, username2, timeout=BACKOFF_TIME * 3,
    )
