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
import logging
from typing import Any, Callable, List, Mapping, Optional, Tuple
from unittest import mock

import psycopg2
from aiopg import Connection
from kubernetes_asyncio.client import (
    BatchV1Api,
    BatchV1beta1Api,
    CoreV1Api,
    CustomObjectsApi,
    V1Namespace,
)

from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    KOPF_STATE_STORE_PREFIX,
    RESOURCE_CRATEDB,
)
from crate.operator.cratedb import (
    connection_factory,
    get_cluster_settings,
    get_healthiness,
    get_number_of_nodes,
)
from crate.operator.utils.kubeapi import (
    get_public_host_for_testing,
    get_system_user_password,
)

logger = logging.getLogger(__name__)

CRATE_VERSION = "4.6.4"
DEFAULT_TIMEOUT = 60


async def assert_wait_for(
    condition, coro_func, *args, err_msg="", timeout=DEFAULT_TIMEOUT, delay=2, **kwargs
):
    ret_val = await coro_func(*args, **kwargs)
    duration = 0.0
    while ret_val is not condition:
        await asyncio.sleep(delay)
        ret_val = await coro_func(*args, **kwargs)
        if ret_val is not condition and duration > timeout:
            break
        else:
            duration += delay
    assert ret_val is condition, err_msg


async def does_namespace_exist(core, namespace: str) -> bool:
    namespaces = await core.list_namespace()
    return namespace in (ns.metadata.name for ns in namespaces.items)


async def start_cluster(
    name: str,
    namespace: V1Namespace,
    core: CoreV1Api,
    coapi: CustomObjectsApi,
    hot_nodes: int = 0,
    crate_version: str = CRATE_VERSION,
    wait_for_healthy: bool = True,
    additional_cluster_spec: Optional[Mapping[str, Any]] = None,
    users: Optional[List[Mapping[str, Any]]] = None,
) -> Tuple[str, str]:
    additional_cluster_spec = additional_cluster_spec if additional_cluster_spec else {}
    body: dict = {
        "apiVersion": "cloud.crate.io/v1",
        "kind": "CrateDB",
        "metadata": {"name": name},
        "spec": {
            "cluster": {
                "imageRegistry": "crate",
                "name": "my-crate-cluster",
                "version": crate_version,
                **additional_cluster_spec,  # type: ignore
            },
            "nodes": {
                "data": [
                    {
                        "name": "hot",
                        "replicas": hot_nodes,
                        "resources": {
                            "cpus": 2,
                            "memory": "4Gi",
                            "heapRatio": 0.25,
                            "disk": {
                                "storageClass": "default",
                                "size": "16GiB",
                                "count": 1,
                            },
                        },
                    },
                ]
            },
        },
    }
    if users:
        body["spec"]["users"] = users

    await coapi.create_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        body=body,
    )

    host = await asyncio.wait_for(
        get_public_host_for_testing(core, namespace.metadata.name, name),
        # It takes a while to retrieve an external IP on AKS.
        timeout=DEFAULT_TIMEOUT * 5,
    )
    password = await get_system_user_password(core, namespace.metadata.name, name)

    if wait_for_healthy:
        # The timeouts are pretty high here since in Azure it's sometimes
        # non-deterministic how long provisioning a pod will actually take.
        await assert_wait_for(
            True,
            is_kopf_handler_finished,
            coapi,
            name,
            namespace.metadata.name,
            f"{KOPF_STATE_STORE_PREFIX}/cluster_create",
            err_msg="Cluster has not finished bootstrapping",
            timeout=DEFAULT_TIMEOUT * 5,
        )

        await assert_wait_for(
            True,
            is_cluster_healthy,
            connection_factory(host, password),
            hot_nodes,
            err_msg="Cluster wasn't healthy after 5 minutes.",
            timeout=DEFAULT_TIMEOUT * 5,
        )

    return host, password


async def is_cluster_healthy(
    conn_factory: Callable[[], Connection], expected_num_nodes: int
):
    try:
        async with conn_factory() as conn:
            async with conn.cursor() as cursor:
                num_nodes = await get_number_of_nodes(cursor)
                healthines = await get_healthiness(cursor)
                return expected_num_nodes == num_nodes and healthines in {1, None}
    except (psycopg2.DatabaseError, asyncio.exceptions.TimeoutError):
        return False


async def is_kopf_handler_finished(
    coapi: CustomObjectsApi, name, namespace: str, handler_name: str
):
    cratedb = await coapi.get_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace,
        name=name,
    )

    handler_status = cratedb["metadata"].get("annotations", {}).get(handler_name, None)
    return handler_status is None


async def create_test_sys_jobs_table(conn_factory):
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            table_name = config.JOBS_TABLE
            logger.info(f"Creating {table_name}")
            await cursor.execute(
                f"CREATE TABLE {table_name} (id INTEGER, stmt VARCHAR)"
            )


async def insert_test_snapshot_job(conn_factory):
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            table_name = config.JOBS_TABLE
            logger.info(f"Creating {table_name}")
            await cursor.execute(
                f"INSERT INTO {table_name} (id, stmt) VALUES (1, 'CREATE SNAPSHOT ...')"
            )
            await cursor.execute(f"REFRESH TABLE {table_name}")


async def clear_test_snapshot_jobs(conn_factory):
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            table_name = config.JOBS_TABLE
            logger.info(f"Creating {table_name}")
            await cursor.execute(f"DELETE FROM {table_name}")
            await cursor.execute(f"REFRESH TABLE {table_name}")


async def create_fake_snapshot_job(api_client, name, namespace):
    """
    As the name implies, this creates a k8s job that looks like a snapshot job.
    It pulls busybox and sleeps for 60s, long enough for scaling to block.
    """
    batch = BatchV1Api(api_client)
    body = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"cluster-backup-{name}",
            "labels": {
                "app.kubernetes.io/component": "backup",
                "app.kubernetes.io/managed-by": "crate-operator",
                "app.kubernetes.io/name": name,
                "app.kubernetes.io/part-of": "cratedb",
            },
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "busybox",
                            "image": "busybox",
                            "command": ["sleep", "60"],
                        }
                    ],
                    "restartPolicy": "Never",
                }
            },
        },
    }
    await batch.create_namespaced_job(namespace, body)


async def create_fake_cronjob(api_client, name, namespace):
    """
    As the name implies, this creates a scheduled CronJob.

    This can be used in tests to check for cronjobs existing, and their statuses.
    """
    batch = BatchV1beta1Api(api_client)
    body = {
        "apiVersion": "batch/v1beta1",
        "kind": "CronJob",
        "metadata": {
            "name": f"create-snapshot-{name}",
            "labels": {
                "app.kubernetes.io/component": "backup",
                "app.kubernetes.io/managed-by": "crate-operator",
                "app.kubernetes.io/name": name,
                "app.kubernetes.io/part-of": "cratedb",
            },
        },
        "spec": {
            "jobTemplate": {
                "metadata": {"name": name},
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "busybox",
                                    "image": "busybox",
                                    "command": ["sleep", "60"],
                                }
                            ],
                            "restartPolicy": "Never",
                        }
                    },
                },
            },
            "schedule": "* * 1 1 0",
        },
    }
    await batch.create_namespaced_cron_job(namespace, body)


async def delete_fake_snapshot_job(api_client, name, namespace):
    batch = BatchV1Api(api_client)
    await batch.delete_namespaced_job(f"cluster-backup-{name}", namespace)


async def cluster_routing_allocation_enable_equals(
    conn_factory: Callable[[], Connection], expected_value: str
) -> bool:
    try:
        async with conn_factory() as conn:
            async with conn.cursor() as cursor:
                cluster_settings = await get_cluster_settings(cursor)

                value = (
                    cluster_settings.get("cluster", {})
                    .get("routing", {})
                    .get("allocation", {})
                    .get("enable", "")
                )
                return value == expected_value
    except (psycopg2.DatabaseError, asyncio.exceptions.TimeoutError):
        return False


async def was_notification_sent(
    mock_send_notification: mock.AsyncMock, call: mock.call
):
    if mock_send_notification.call_count == 0:
        return False

    try:
        mock_send_notification.assert_has_calls([call], any_order=False)
        return True
    except AssertionError:
        return False
