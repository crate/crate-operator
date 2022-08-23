# CrateDB Kubernetes Operator
# Copyright (C) 2021 Crate.IO GmbH
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
import logging
from typing import Set
from unittest import mock

import pytest
from change_compute import generate_body_patch
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi
from webhooks import WebhookChangeComputePayload

from crate.operator.constants import API_GROUP, RESOURCE_CRATEDB
from crate.operator.cratedb import connection_factory

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    cluster_routing_allocation_enable_equals,
    create_test_sys_jobs_table,
    is_cluster_healthy,
    is_kopf_handler_finished,
    start_cluster,
)


async def do_pods_exist(core: CoreV1Api, namespace: str, expected: Set[str]) -> bool:
    pods = await core.list_namespaced_pod(namespace=namespace)
    return expected.issubset({p.metadata.name for p in pods.items})


async def do_pod_ids_exist(core: CoreV1Api, namespace: str, pod_ids: Set[str]) -> bool:
    pods = await core.list_namespaced_pod(namespace=namespace)
    return bool(pod_ids.intersection({p.metadata.uid for p in pods.items}))


@pytest.mark.k8s
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "set_initial_request_resources, cpu_request, memory_request, cpu_limit,"
    "memory_limit",
    [
        # Test a change in cpu and memory with requests = limits
        (False, 1, "5Gi", 1, "5Gi"),
        # Test a change in cpu and memory from req = limits to req != limits
        (False, 1, "3Gi", 3, "5Gi"),
        # Test a change in cpu and memory from req != limits to req = limits
        (True, 1, "5Gi", 1, "5Gi"),
    ],
)
async def test_change_cpu_and_ram(
    set_initial_request_resources,
    cpu_request,
    memory_request,
    cpu_limit,
    memory_limit,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Tests that the cratedb resource changes in cpu/memory requests/limits are properly
    passed on to the statefulset.
    Note we cannot test the affinity in a test cluster.
    """
    body_changes = [
        {
            "op": "replace",
            "path": "/spec/nodes/data/0/resources/limits/cpu",
            "value": cpu_limit,
        },
        {
            "op": "replace",
            "path": "/spec/nodes/data/0/resources/limits/memory",
            "value": memory_limit,
        },
    ]
    if cpu_request != cpu_limit:
        body_changes.append(
            {
                "op": "replace",
                "path": "/spec/nodes/data/0/resources/requests/cpu",
                "value": cpu_request,
            },
        )
    if memory_request != memory_limit:
        body_changes.append(
            {
                "op": "replace",
                "path": "/spec/nodes/data/0/resources/requests/memory",
                "value": memory_request,
            },
        )

    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    if set_initial_request_resources:
        # Start a cluster with requests set to half the original limits
        initial_requests = {"cpu": 1, "memory": "2Gi"}
        host, password = await start_cluster(
            name, namespace, core, coapi, 3, "5.0.0", resource_requests=initial_requests
        )
    else:
        # Start a cluster without requests, just the standard limits of cpu=2 and
        # memory=4Gi per node
        host, password = await start_cluster(name, namespace, core, coapi, 3, "5.0.0")

    await assert_wait_for(
        True,
        do_pods_exist,
        core,
        namespace.metadata.name,
        {
            f"crate-data-hot-{name}-0",
            f"crate-data-hot-{name}-1",
            f"crate-data-hot-{name}-2",
        },
    )

    conn_factory = connection_factory(host, password)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        conn_factory,
        3,
        err_msg="Cluster wasn't healthy",
        timeout=DEFAULT_TIMEOUT,
    )

    await create_test_sys_jobs_table(conn_factory)

    pods = await core.list_namespaced_pod(namespace=namespace.metadata.name)
    original_pods = {p.metadata.uid for p in pods.items}

    for p in pods.items:
        for c in p.spec.containers:
            if c.name == "crate":
                assert c.resources.limits["cpu"] == str(2)
                assert c.resources.limits["memory"] == "4Gi"
                assert c.resources.requests["cpu"] == str(2)
                assert c.resources.requests["memory"] == "4Gi"

    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=body_changes,
    )

    await assert_wait_for(
        True,
        cluster_routing_allocation_enable_equals,
        connection_factory(host, password),
        "new_primaries",
        err_msg="Cluster routing allocation setting has not been updated",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.change_compute",
        err_msg="Plan change has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.restart",
        err_msg="Restart has not finished",
        timeout=DEFAULT_TIMEOUT * 15,
    )

    await assert_wait_for(
        False,
        do_pod_ids_exist,
        core,
        namespace.metadata.name,
        original_pods,
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.after_change_compute",
        err_msg="Plan change has not finished",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        3,
        err_msg="Cluster wasn't healthy",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        cluster_routing_allocation_enable_equals,
        connection_factory(host, password),
        "all",
        err_msg="Cluster routing allocation setting has not been updated",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    # Collect the pod info again to verify the changes have been applied
    pods = await core.list_namespaced_pod(namespace=namespace.metadata.name)
    assert len(pods.items) == 3
    for p in pods.items:
        for c in p.spec.containers:
            if c.name == "crate":
                assert c.resources.limits["cpu"] == str(cpu_limit)
                assert c.resources.limits["memory"] == memory_limit
                assert c.resources.requests["cpu"] == str(cpu_request)
                assert c.resources.requests["memory"] == memory_request


@pytest.mark.parametrize(
    "old_cpu_limit, old_memory_limit, old_cpu_request, old_memory_request, "
    "new_cpu_limit, new_memory_limit, new_cpu_request, new_memory_request",
    [
        # Test no requests
        (1, "2Gi", None, None, 3, "5Gi", None, None),
        # Test requests set
        (1, "2Gi", None, None, 3, "5Gi", 5, "8Gi"),
    ],
)
def test_generate_body_patch(
    old_cpu_limit,
    old_memory_limit,
    old_cpu_request,
    old_memory_request,
    new_cpu_limit,
    new_memory_limit,
    new_cpu_request,
    new_memory_request,
    faker,
):
    plan_change_data = WebhookChangeComputePayload(
        old_cpu_limit=old_cpu_limit,
        old_memory_limit=old_memory_limit,
        old_cpu_request=old_cpu_request,
        old_memory_request=old_memory_request,
        new_cpu_limit=new_cpu_limit,
        new_memory_limit=new_memory_limit,
        new_cpu_request=new_cpu_request,
        new_memory_request=new_memory_request,
    )

    name = faker.domain_word()
    with mock.patch("crate.operator.create.config.TESTING", False):
        body = generate_body_patch(name, plan_change_data, logging.getLogger(__name__))

    resources = body["spec"]["template"]["spec"]["containers"][0]["resources"]
    assert resources["limits"]["cpu"] == new_cpu_limit
    assert resources["limits"]["memory"] == new_memory_limit

    assert resources["requests"]["cpu"] == new_cpu_request or new_cpu_limit
    assert resources["requests"]["memory"] == new_memory_request or new_memory_limit

    if new_cpu_request or new_memory_request:
        assert body["spec"]["template"]["spec"]["affinity"].node_affinity is not None
    else:
        assert (
            body["spec"]["template"]["spec"]["affinity"].pod_anti_affinity is not None
        )