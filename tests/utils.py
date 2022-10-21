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
import logging
import os
from typing import Any, Callable, List, Mapping, Optional, Set, Tuple
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
from prometheus_client import REGISTRY

from crate.operator.backup import create_backups
from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    DATA_NODE_NAME,
    KOPF_STATE_STORE_PREFIX,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_PART_OF,
    RESOURCE_CRATEDB,
)
from crate.operator.cratedb import (
    connection_factory,
    get_cluster_settings,
    get_healthiness,
    get_number_of_nodes,
)
from crate.operator.utils.kubeapi import (
    get_service_public_hostname,
    get_system_user_password,
)

logger = logging.getLogger(__name__)

CRATE_VERSION = "5.0.0"
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


async def do_pods_exist(core: CoreV1Api, namespace: str, expected: Set[str]) -> bool:
    pods = await core.list_namespaced_pod(namespace=namespace)
    return expected.issubset({p.metadata.name for p in pods.items})


async def do_pod_ids_exist(core: CoreV1Api, namespace: str, pod_ids: Set[str]) -> bool:
    pods = await core.list_namespaced_pod(namespace=namespace)
    return bool(pod_ids.intersection({p.metadata.uid for p in pods.items}))


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
    resource_requests: Optional[Mapping[str, Any]] = None,
    backup_spec: Optional[Mapping[str, Any]] = None,
) -> Tuple[str, str]:
    additional_cluster_spec = additional_cluster_spec if additional_cluster_spec else {}
    body: dict = {
        "apiVersion": "cloud.crate.io/v1",
        "kind": "CrateDB",
        "metadata": {
            "name": name,
            "annotations": {
                "testing": f"{os.getpid()}",
                "test-name": os.environ.get("PYTEST_CURRENT_TEST", "")
                .split(":")[-1]
                .split(" ")[0],
            },
        },
        "spec": {
            "backups": backup_spec if backup_spec else None,
            "cluster": {
                "imageRegistry": "crate",
                "name": "my-crate-cluster",
                "version": crate_version,
                **additional_cluster_spec,  # type: ignore
            },
            "nodes": {
                "data": [
                    {
                        "name": DATA_NODE_NAME,
                        "replicas": hot_nodes,
                        "resources": {
                            "limits": {
                                "cpu": 2,
                                "memory": "4Gi",
                            },
                            "heapRatio": 0.25,
                            "disk": {
                                "storageClass": config.DEBUG_VOLUME_STORAGE_CLASS,
                                "size": "16GiB",
                                "count": 1,
                            },
                        },
                    },
                ]
            },
        },
    }

    if resource_requests:
        body["spec"]["nodes"]["data"][0]["resources"]["requests"] = {
            "cpu": resource_requests["cpu"],
            "memory": resource_requests["memory"],
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
        get_service_public_hostname(core, namespace.metadata.name, name),
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


async def start_backup_metrics(
    name: str,
    namespace: V1Namespace,
    faker,
):
    backups_spec = {
        "aws": {
            "accessKeyId": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
            "basePath": faker.uri_path() + "/",
            "cron": "1 2 3 4 5",
            "region": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
            "bucket": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
            "secretAccessKey": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
        },
    }

    await create_backups(
        None,
        namespace.metadata.name,
        name,
        {
            LABEL_COMPONENT: "backup",
            LABEL_MANAGED_BY: "crate-operator",
            LABEL_NAME: name,
            LABEL_PART_OF: "cratedb",
        },
        32581,
        23851,
        backups_spec,
        None,
        True,
        logger,
    )


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


def get_latest_metric_value(name: str, cluster_id: str):
    metrics = list(REGISTRY.collect())
    metric = next(filter(lambda m: m.name == name, metrics), None)
    if not metric:
        return None
    latest = next(
        filter(
            lambda s: s.labels["cluster_id"] == cluster_id,
            metric.samples,
        ),
        None,
    )
    return latest.value if latest else None
