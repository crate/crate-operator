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

import sys

import pytest
from kubernetes_asyncio.client import (
    BatchV1beta1Api,
    CoreV1Api,
    CustomObjectsApi,
    V1Namespace,
)

from crate.operator.constants import (
    API_GROUP,
    KOPF_STATE_STORE_PREFIX,
    RESOURCE_CRATEDB,
)
from crate.operator.cratedb import connection_factory
from crate.operator.scale import parse_replicas

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
    start_cluster,
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


@pytest.mark.k8s
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "repl_hot_from,repl_hot_to",  # noqa
    [
        (1, 2),  # scale up from 1 to 2 data nodes
        (3, 2),  # scale down from 3 to 2 data nodes
    ],
)
async def test_scale_cluster(
    repl_hot_from,
    repl_hot_to,
    faker,
    namespace,
    cleanup_handler,
    kopf_runner,
    api_client,
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(
        name,
        namespace,
        cleanup_handler,
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


@pytest.mark.k8s
@pytest.mark.asyncio
async def test_scale_cluster_while_create_snapshot_running(
    faker, namespace, cleanup_handler, kopf_runner, api_client
):
    # Given
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    await create_fake_cronjob(api_client, name, namespace.metadata.name)

    host, password = await start_cluster(
        name, namespace, cleanup_handler, core, coapi, 1
    )

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
    faker, namespace, cleanup_handler, kopf_runner, api_client
):
    # Given
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(
        name, namespace, cleanup_handler, core, coapi, 1
    )

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

    ensure_no_backups = cratedb["metadata"]["annotations"].get(
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update.ensure_no_backups", None
    )
    if not ensure_no_backups:
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
