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
import sys
from unittest import mock

import pytest
from kubernetes_asyncio.client import (
    AppsV1Api,
    BatchV1beta1Api,
    CoreV1Api,
    CustomObjectsApi,
    V1Namespace,
)

from crate.operator.constants import (
    API_GROUP,
    BACKUP_METRICS_DEPLOYMENT_NAME,
    DATA_NODE_NAME,
    KOPF_STATE_STORE_PREFIX,
    RESOURCE_CRATEDB,
)
from crate.operator.cratedb import connection_factory
from crate.operator.create import get_statefulset_crate_command
from crate.operator.operations import get_pods_in_deployment, get_pods_in_statefulset
from crate.operator.scale import parse_replicas, patch_command
from crate.operator.webhooks import WebhookEvent, WebhookStatus

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    clear_test_snapshot_jobs,
    create_fake_cronjob,
    create_fake_snapshot_job,
    create_test_sys_jobs_table,
    delete_fake_snapshot_job,
    insert_test_snapshot_job,
    is_cluster_healthy,
    is_kopf_handler_finished,
    start_backup_metrics,
    start_cluster,
    was_notification_sent,
)


@pytest.mark.parametrize(
    "input,expected",
    [
        ("0", 0),
        ("5", 5),
        ("all", sys.maxsize),
        ("0-1", 0),
        ("1-7", 1),
        ("5-all", 5),
        ("all-all", sys.maxsize),
    ],
)
def test_parse_replicas(input, expected):
    assert parse_replicas(input) == expected


@pytest.mark.parametrize(
    "total, quorum, new_total, new_quorum",
    [(1, 1, 2, 2), (2, 2, 3, 2), (4, 3, 8, 5), (16, 9, 31, 16)],
)
def test_patch_sts_command(total, quorum, new_total, new_quorum):
    cmd = get_statefulset_crate_command(
        namespace="some-namespace",
        name="cluster1",
        master_nodes=["node-0", "node-1", "node-2"],
        total_nodes_count=total,
        data_nodes_count=total,
        crate_node_name_prefix="node-",
        cluster_name="my-cluster",
        node_name="node",
        node_spec={"resources": {"limits": {"cpu": 1}, "disk": {"count": 1}}},
        cluster_settings=None,
        has_ssl=False,
        is_master=True,
        is_data=True,
        crate_version="4.7.0",
    )
    new_cmd = patch_command(cmd, new_total)
    assert f"-Cgateway.recover_after_data_nodes={new_quorum}" in new_cmd
    assert f"-Cgateway.expected_data_nodes={new_total}" in new_cmd


@pytest.mark.parametrize(
    "total, quorum, new_total, new_quorum",
    [(1, 1, 2, 2), (2, 2, 3, 2), (4, 3, 8, 5), (16, 9, 31, 16)],
)
def test_patch_sts_command_deprecated(total, quorum, new_total, new_quorum):
    cmd = get_statefulset_crate_command(
        namespace="some-namespace",
        name="cluster1",
        master_nodes=["node-0", "node-1", "node-2"],
        total_nodes_count=total,
        data_nodes_count=total,
        crate_node_name_prefix="node-",
        cluster_name="my-cluster",
        node_name="node",
        node_spec={"resources": {"limits": {"cpu": 1}, "disk": {"count": 1}}},
        cluster_settings=None,
        has_ssl=False,
        is_master=True,
        is_data=True,
        crate_version="4.6.3",
    )
    new_cmd = patch_command(cmd, new_total)
    assert f"-Cgateway.recover_after_nodes={new_quorum}" in new_cmd
    assert f"-Cgateway.expected_nodes={new_total}" in new_cmd


@pytest.mark.k8s
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "repl_hot_from,repl_hot_to",  # noqa
    [
        (1, 2),  # scale up from 1 to 2 data nodes
        (3, 2),  # scale down from 3 to 2 data nodes
    ],
)
@mock.patch("crate.operator.webhooks.webhook_client._send")
async def test_scale_cluster(
    mock_send_notification,
    repl_hot_from,
    repl_hot_to,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(
        name,
        namespace,
        core,
        coapi,
        repl_hot_from,
    )

    conn_factory = connection_factory(host, password)
    await create_test_sys_jobs_table(conn_factory)

    await _scale_cluster(coapi, name, namespace, repl_hot_to)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        repl_hot_to,
        err_msg="Cluster wasn't healthy after 5 minutes.",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Scaling has not finished",
        timeout=DEFAULT_TIMEOUT,
    )

    notification_success_call = mock.call(
        WebhookEvent.SCALE,
        WebhookStatus.SUCCESS,
        namespace.metadata.name,
        name,
        scale_data=mock.ANY,
        unsafe=mock.ANY,
        logger=mock.ANY,
    )
    assert await was_notification_sent(
        mock_send_notification=mock_send_notification, call=notification_success_call
    ), "A success notification was expected but was not sent"


@pytest.mark.k8s
@pytest.mark.asyncio
async def test_suspend_resume_cluster(
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    apps = AppsV1Api(api_client)
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    # Create a cluster with 1 node
    host, password = await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
    )

    conn_factory = connection_factory(host, password)
    await create_test_sys_jobs_table(conn_factory)

    await start_backup_metrics(name, namespace, faker)

    await assert_wait_for(
        True,
        does_deployment_exist,
        apps,
        namespace.metadata.name,
        BACKUP_METRICS_DEPLOYMENT_NAME.format(name=name),
    )

    # Request the cluster to be suspended
    await _scale_cluster(coapi, name, namespace, 0)

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Scaling down has not finished",
        timeout=DEFAULT_TIMEOUT * 2,
    )

    await assert_wait_for(
        False,
        does_backup_metrics_pod_exist,
        core,
        name,
        namespace.metadata.name,
        err_msg="Backup metrics has not been scaled down.",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        False,
        do_crate_pods_exist,
        core,
        name,
        namespace.metadata.name,
        DATA_NODE_NAME,
        err_msg="CrateDB pods still exist.",
        timeout=DEFAULT_TIMEOUT,
    )

    # Request the cluster to be resumed
    await _scale_cluster(coapi, name, namespace, 1)

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Scaling up has not finished",
        timeout=DEFAULT_TIMEOUT * 2,
    )

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        1,
        err_msg="Cluster wasn't healthy after 5 minutes.",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await assert_wait_for(
        True,
        does_backup_metrics_pod_exist,
        core,
        name,
        namespace.metadata.name,
        err_msg="Backup metrics has not been scaled up.",
        timeout=DEFAULT_TIMEOUT,
    )


@pytest.mark.k8s
@pytest.mark.asyncio
async def test_scale_cluster_while_create_snapshot_running(
    faker, namespace, kopf_runner, api_client
):
    # Given
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    await create_fake_cronjob(api_client, name, namespace.metadata.name)

    host, password = await start_cluster(name, namespace, core, coapi, 1)

    conn_factory = connection_factory(host, password)
    await create_test_sys_jobs_table(conn_factory)
    await insert_test_snapshot_job(conn_factory)

    # When
    await _scale_cluster(coapi, name, namespace, 2)

    # Then
    await assert_wait_for(
        True,
        _backup_cronjob_is_suspended,
        api_client,
        namespace.metadata.name,
        err_msg="Snapshot cronjob is not suspended",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        _is_blocked_on_running_snapshot,
        coapi,
        name,
        namespace.metadata.name,
        "snapshot is currently in progress",
        err_msg="Scaling was not blocked by a snapshot job",
        timeout=DEFAULT_TIMEOUT,
    )

    await clear_test_snapshot_jobs(conn_factory)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        2,
        err_msg="Cluster wasn't healthy after 2 minutes.",
        timeout=DEFAULT_TIMEOUT * 2,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Scaling has not finished",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        False,
        _backup_cronjob_is_suspended,
        api_client,
        namespace.metadata.name,
        err_msg="Snapshot cronjob has not been re-enabled.",
        timeout=DEFAULT_TIMEOUT,
    )


@pytest.mark.k8s
@pytest.mark.asyncio
async def test_scale_cluster_while_k8s_snapshot_job_running(
    faker, namespace, kopf_runner, api_client
):
    # Given
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(name, namespace, core, coapi, 1)

    conn_factory = connection_factory(host, password)
    await create_test_sys_jobs_table(conn_factory)
    await create_fake_snapshot_job(api_client, name, namespace.metadata.name)

    # When
    await _scale_cluster(coapi, name, namespace, 2)

    # Then
    await assert_wait_for(
        True,
        _is_blocked_on_running_snapshot,
        coapi,
        name,
        namespace.metadata.name,
        "A snapshot k8s job is currently running",
        err_msg="Scaling was not blocked by a snapshot job",
        timeout=DEFAULT_TIMEOUT,
    )

    await delete_fake_snapshot_job(api_client, name, namespace.metadata.name)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        2,
        err_msg="Cluster wasn't healthy after 3 minutes.",
        timeout=DEFAULT_TIMEOUT * 3,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Scaling has not finished",
        timeout=DEFAULT_TIMEOUT,
    )


async def _is_blocked_on_running_snapshot(
    coapi: CustomObjectsApi, name, namespace: str, expected_str: str
):
    cratedb = await coapi.get_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace,
        name=name,
    )

    before_cluster_update = cratedb["metadata"]["annotations"].get(
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update.before_cluster_update", None
    )
    if not before_cluster_update:
        return False

    for v in cratedb["metadata"]["annotations"].values():
        if expected_str in v:
            return True

    return False


async def _backup_cronjob_is_suspended(api_client, namespace: str):
    batch = BatchV1beta1Api(api_client)
    jobs = await batch.list_namespaced_cron_job(namespace)

    for job in jobs.items:
        return job.spec.suspend


async def _scale_cluster(
    coapi: CustomObjectsApi, name: str, namespace: V1Namespace, new_replicas: int
):
    patch_body = [
        {
            "op": "replace",
            "path": "/spec/nodes/data/0/replicas",
            "value": new_replicas,
        }
    ]
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=patch_body,
    )

    await asyncio.sleep(1.0)


async def does_backup_metrics_pod_exist(
    core: CoreV1Api, name: str, namespace: V1Namespace
) -> bool:
    backup_metrics_pods = await get_pods_in_deployment(core, namespace, name)
    backup_metrics_name = BACKUP_METRICS_DEPLOYMENT_NAME.format(name=name)
    return any(p["name"].startswith(backup_metrics_name) for p in backup_metrics_pods)


async def does_deployment_exist(apps: AppsV1Api, namespace: str, name: str) -> bool:
    deployments = await apps.list_namespaced_deployment(namespace=namespace)
    return name in (d.metadata.name for d in deployments.items)


async def do_crate_pods_exist(
    core: CoreV1Api, name: str, namespace: V1Namespace, node_name: str
) -> bool:
    crate_pods = await get_pods_in_statefulset(core, namespace, name, node_name)
    return len(crate_pods) > 0
