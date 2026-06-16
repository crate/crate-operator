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

import copy
import logging
from unittest import mock

import pytest
from kubernetes_asyncio.client import (
    AppsV1Api,
    CoreV1Api,
    CustomObjectsApi,
    V1NodeAffinity,
    V1PodAntiAffinity,
)

from crate.operator.change_compute import (
    change_cluster_compute,
    compute_payload_for_specs,
    generate_body_patch,
)
from crate.operator.constants import API_GROUP, RESOURCE_CRATEDB
from crate.operator.cratedb import connection_factory
from crate.operator.webhooks import (
    WebhookChangeComputePayload,
    WebhookEvent,
    WebhookStatus,
)

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    cluster_routing_allocation_enable_equals,
    create_test_sys_jobs_table,
    do_pod_ids_exist,
    do_pods_exist,
    is_cluster_healthy,
    is_kopf_handler_finished,
    require_connection,
    start_cluster,
    was_notification_sent,
)


def calculate_heap_size(memory_size_in_gb: int, heap_ratio: float) -> int:
    return int(memory_size_in_gb * 1024 * 1024 * 1024 * heap_ratio)


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client._send")
async def test_change_compute_from_request_to_limit(
    mock_send_notification,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Tests that the cratedb resource changes in cpu/memory requests/limits are properly
    passed on to the statefulset.
    The original cluster has requests and limits defined.
    The changes requested involve requests=limits.
    Note we cannot test the affinity in a test cluster.
    """
    cpu_limit = 1
    memory_limit = "5Gi"
    heap_ratio = 0.3
    expected_heap_size = calculate_heap_size(5, 0.3)

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
        {
            "op": "replace",
            "path": "/spec/nodes/data/0/resources/heapRatio",
            "value": heap_ratio,
        },
        # Make requests equal to limits
        {
            "op": "replace",
            "path": "/spec/nodes/data/0/resources/requests/cpu",
            "value": cpu_limit,
        },
        {
            "op": "replace",
            "path": "/spec/nodes/data/0/resources/requests/memory",
            "value": memory_limit,
        },
    ]

    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    # Start a cluster with requests set to half the original limits
    initial_requests = {"cpu": 1, "memory": "2Gi"}
    host, password = await start_cluster(
        name, namespace, core, coapi, 1, resource_requests=initial_requests
    )

    await assert_wait_for(
        True,
        do_pods_exist,
        core,
        namespace.metadata.name,
        {
            f"crate-data-hot-{name}-0",
        },
    )

    conn_factory = connection_factory(*require_connection(host, password))

    await assert_wait_for(
        True,
        is_cluster_healthy,
        conn_factory,
        1,
        err_msg="Cluster wasn't healthy",
        timeout=DEFAULT_TIMEOUT,
    )

    await create_test_sys_jobs_table(conn_factory)

    pods = await core.list_namespaced_pod(namespace=namespace.metadata.name)
    original_pods = {p.metadata.uid for p in pods.items}

    # Check limits and requests before changing them
    total_env_vars = 0
    for p in pods.items:
        for c in p.spec.containers:
            if c.name == "crate":
                assert c.resources.limits["cpu"] == str(2)
                assert c.resources.limits["memory"] == "4Gi"
                assert c.resources.requests["cpu"] == str(1)
                assert c.resources.requests["memory"] == str("2Gi")

                # Test the initial heap ratio
                total_env_vars = len(c.env)
                for env in c.env:
                    if env.name == "CRATE_HEAP_SIZE":
                        assert env.value == str(1024 * 1024 * 1024)

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
        connection_factory(*require_connection(host, password)),
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
        err_msg="Compute change has not finished",
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
        err_msg="Compute change has not finished",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(*require_connection(host, password)),
        1,
        err_msg="Cluster wasn't healthy",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        cluster_routing_allocation_enable_equals,
        connection_factory(*require_connection(host, password)),
        "all",
        err_msg="Cluster routing allocation setting has not been updated",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    # Collect the pod info again to verify the changes have been applied
    pods = await core.list_namespaced_pod(namespace=namespace.metadata.name)
    assert len(pods.items) == 1
    for p in pods.items:
        for c in p.spec.containers:
            if c.name == "crate":
                assert c.resources.limits["cpu"] == str(cpu_limit)
                assert c.resources.limits["memory"] == memory_limit
                assert c.resources.requests["cpu"] == str(cpu_limit)
                assert c.resources.requests["memory"] == memory_limit

                # Test the new heap ration has been applied
                for env in c.env:
                    if env.name == "CRATE_HEAP_SIZE":
                        assert env.value == str(expected_heap_size)
                assert total_env_vars == len(c.env)

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.after_cluster_update",
        err_msg="After Cluster Update has not finished",
        timeout=DEFAULT_TIMEOUT,
    )

    notification_success_call = mock.call(
        WebhookEvent.COMPUTE_CHANGED,
        WebhookStatus.SUCCESS,
        namespace.metadata.name,
        name,
        compute_changed_data=mock.ANY,
        unsafe=mock.ANY,
        retry=mock.ANY,
        logger=mock.ANY,
    )
    assert await was_notification_sent(
        mock_send_notification=mock_send_notification, call=notification_success_call
    ), "A success notification was expected but was not sent"


@pytest.mark.k8s
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old_cpu_limit, old_memory_limit, old_cpu_request, old_memory_request, "
    "new_cpu_limit, new_memory_limit, new_cpu_request, new_memory_request, "
    "old_nodepool, new_nodepool",
    [
        # Test no requests
        (1, "2Gi", None, None, 3, "5Gi", None, None, "dedicated", "dedicated"),
        # Test requests set
        (1, "2Gi", None, None, 3, "5Gi", 5, "8Gi", "shared", "shared"),
    ],
)
async def test_generate_body_patch(
    old_cpu_limit,
    old_memory_limit,
    old_cpu_request,
    old_memory_request,
    new_cpu_limit,
    new_memory_limit,
    new_cpu_request,
    new_memory_request,
    old_nodepool,
    new_nodepool,
    faker,
    kopf_runner,
    api_client,
    namespace,
):
    compute_change_data = WebhookChangeComputePayload(
        old_cpu_limit=old_cpu_limit,
        old_memory_limit=old_memory_limit,
        old_cpu_request=old_cpu_request,
        old_memory_request=old_memory_request,
        new_cpu_limit=new_cpu_limit,
        new_memory_limit=new_memory_limit,
        new_cpu_request=new_cpu_request,
        new_memory_request=new_memory_request,
        old_heap_ratio=0.25,
        new_heap_ratio=0.25,
        old_nodepool=old_nodepool,
        new_nodepool=new_nodepool,
    )

    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    apps = AppsV1Api(api_client)
    name = faker.domain_word()

    # Start a cluster with requests set to half the original limits
    crate_resources = {
        "limits": {"cpu": old_cpu_limit, "memory": old_memory_limit},
        "requests": {"cpu": old_cpu_request, "memory": old_memory_request},
    }
    host, password = await start_cluster(
        name, namespace, core, coapi, 1, resource_requests=crate_resources
    )

    with mock.patch("crate.operator.create.config.TESTING", False):
        body = await generate_body_patch(
            apps,
            name,
            namespace.metadata.name,
            compute_change_data,
            logging.getLogger(__name__),
        )

    resources = body["spec"]["template"]["spec"]["containers"][0]["resources"]
    command = body["spec"]["template"]["spec"]["containers"][0]["command"]

    assert f"-Cprocessors={new_cpu_limit}" in command

    assert resources["limits"]["cpu"] == new_cpu_limit
    assert resources["limits"]["memory"] == new_memory_limit

    assert resources["requests"]["cpu"] == new_cpu_request or new_cpu_limit
    assert resources["requests"]["memory"] == new_memory_request or new_memory_limit

    affinity = body["spec"]["template"]["spec"]["affinity"]
    tolerations = body["spec"]["template"]["spec"]["tolerations"]
    if new_cpu_request or new_memory_request:
        assert type(affinity.node_affinity) is V1NodeAffinity
        assert affinity.pod_anti_affinity == {"$patch": "delete"}
        assert len(tolerations) == 1
        assert tolerations[0].to_dict() == {
            "effect": "NoSchedule",
            "key": "cratedb",
            "operator": "Equal",
            "toleration_seconds": None,
            "value": "shared",
        }
    else:
        assert type(affinity.pod_anti_affinity) is V1PodAntiAffinity
        assert affinity.node_affinity == {"$patch": "delete"}
        assert len(tolerations) == 1
        assert tolerations[0].to_dict() == {
            "effect": "NoSchedule",
            "key": "cratedb",
            "operator": "Equal",
            "toleration_seconds": None,
            "value": "any",
        }


def _group_spec(cpu, memory, *, replicas=3, nodepool="dedicated", heap_ratio=0.25):
    return {
        "replicas": replicas,
        "nodepool": nodepool,
        "resources": {
            "limits": {"cpu": cpu, "memory": memory},
            "requests": {"cpu": cpu, "memory": memory},
            "heapRatio": heap_ratio,
        },
    }


class TestComputePayloadForSpecs:
    def test_reads_resources_from_the_given_spec(self):
        # Master resources are read independently from the master spec, so a
        # master payload reflects the master's (possibly smaller) compute.
        old_spec = _group_spec(16, "60000000000")
        new_spec = _group_spec(8, "30000000000")

        payload = compute_payload_for_specs(old_spec, new_spec)

        assert payload["old_cpu_limit"] == 16
        assert payload["new_cpu_limit"] == 8
        assert payload["new_memory_limit"] == "30000000000"
        assert payload["new_heap_ratio"] == 0.25
        assert payload["new_nodepool"] == "dedicated"


def _cluster_body(master_cpu, hot_cpu):
    return {
        "spec": {
            "nodes": {
                "master": _group_spec(master_cpu, "60000000000"),
                "data": [{"name": "hot", **_group_spec(hot_cpu, "30000000000")}],
            }
        }
    }


class TestChangeClusterCompute:
    @pytest.mark.asyncio
    @mock.patch(
        "crate.operator.change_compute.generate_body_patch",
        new_callable=mock.AsyncMock,
    )
    async def test_patches_only_the_master_when_only_master_changed(self, mock_patch):
        mock_patch.return_value = {"spec": {}}
        apps = mock.Mock()
        apps.patch_namespaced_stateful_set = mock.AsyncMock()
        old = _cluster_body(master_cpu=16, hot_cpu=32)
        new = copy.deepcopy(old)
        new["spec"]["nodes"]["master"]["resources"]["limits"]["cpu"] = 8

        await change_cluster_compute(apps, "ns", "c", old, new, mock.Mock())

        patched = [
            call.kwargs["name"]
            for call in apps.patch_namespaced_stateful_set.await_args_list
        ]
        assert patched == ["crate-master-c"]
        # The master StatefulSet name is passed through explicitly.
        assert mock_patch.await_args.kwargs["sts_name"] == "crate-master-c"

    @pytest.mark.asyncio
    @mock.patch(
        "crate.operator.change_compute.generate_body_patch",
        new_callable=mock.AsyncMock,
    )
    async def test_patches_only_the_data_group_when_only_data_changed(self, mock_patch):
        mock_patch.return_value = {"spec": {}}
        apps = mock.Mock()
        apps.patch_namespaced_stateful_set = mock.AsyncMock()
        old = _cluster_body(master_cpu=16, hot_cpu=32)
        new = copy.deepcopy(old)
        new["spec"]["nodes"]["data"][0]["resources"]["limits"]["cpu"] = 30

        await change_cluster_compute(apps, "ns", "c", old, new, mock.Mock())

        patched = [
            call.kwargs["name"]
            for call in apps.patch_namespaced_stateful_set.await_args_list
        ]
        assert patched == ["crate-data-hot-c"]

    @pytest.mark.asyncio
    @mock.patch(
        "crate.operator.change_compute.generate_body_patch",
        new_callable=mock.AsyncMock,
    )
    async def test_no_patch_when_nothing_changed(self, mock_patch):
        apps = mock.Mock()
        apps.patch_namespaced_stateful_set = mock.AsyncMock()
        body = _cluster_body(master_cpu=16, hot_cpu=32)

        await change_cluster_compute(apps, "ns", "c", body, body, mock.Mock())

        apps.patch_namespaced_stateful_set.assert_not_awaited()

    @pytest.mark.asyncio
    @mock.patch(
        "crate.operator.change_compute.generate_body_patch",
        new_callable=mock.AsyncMock,
    )
    async def test_legacy_cluster_without_masters(self, mock_patch):
        mock_patch.return_value = {"spec": {}}
        apps = mock.Mock()
        apps.patch_namespaced_stateful_set = mock.AsyncMock()
        old = {"spec": {"nodes": {"data": [{"name": "hot", **_group_spec(32, "30")}]}}}
        new = copy.deepcopy(old)
        new["spec"]["nodes"]["data"][0]["resources"]["limits"]["cpu"] = 30

        await change_cluster_compute(apps, "ns", "c", old, new, mock.Mock())

        patched = [
            call.kwargs["name"]
            for call in apps.patch_namespaced_stateful_set.await_args_list
        ]
        assert patched == ["crate-data-hot-c"]


class TestUpdateCprocessorCrateSettings:
    @pytest.mark.asyncio
    async def test_updates_cprocessors_for_the_given_statefulset(self):
        # A cpu change must rewrite -Cprocessors in the crate container command
        # of whichever StatefulSet is passed (here the master STS), leaving the
        # rest of the command untouched.
        from crate.operator.change_compute import update_cprocessor_crate_settings

        container = mock.Mock()
        container.name = "crate"
        container.command = [
            "/docker-entrypoint.sh",
            "crate",
            "-Cprocessors=16",
            "-Cnode.master=true",
        ]
        other = mock.Mock()
        other.name = "sql-exporter"
        other.command = ["-Cprocessors=999"]  # must not be touched
        sts = mock.Mock()
        sts.spec.template.spec.containers = [container, other]
        apps = mock.Mock()
        apps.read_namespaced_stateful_set = mock.AsyncMock(return_value=sts)

        updated = await update_cprocessor_crate_settings(
            apps=apps, namespace="ns", sts_name="crate-master-c", processors=8
        )

        assert "-Cprocessors=8" in updated
        assert "-Cprocessors=16" not in updated
        assert "-Cnode.master=true" in updated
        assert apps.read_namespaced_stateful_set.await_args.kwargs["name"] == (
            "crate-master-c"
        )

    @pytest.mark.asyncio
    async def test_rounds_cpu_up_to_match_create(self):
        # A fractional CPU limit must be rounded up, the same way create.py
        # bakes -Cprocessors, so the running and configured values agree.
        from crate.operator.change_compute import update_cprocessor_crate_settings

        container = mock.Mock()
        container.name = "crate"
        container.command = ["crate", "-Cprocessors=4"]
        sts = mock.Mock()
        sts.spec.template.spec.containers = [container]
        apps = mock.Mock()
        apps.read_namespaced_stateful_set = mock.AsyncMock(return_value=sts)

        updated = await update_cprocessor_crate_settings(
            apps=apps, namespace="ns", sts_name="crate-master-c", processors=1.5
        )

        assert "-Cprocessors=2" in updated


class TestGenerateChangeComputePayloadMaster:
    def test_includes_master_compute_when_masters_present(self):
        from crate.operator.change_compute import generate_change_compute_payload

        old = {"spec": {"nodes": {
            "master": _group_spec(16, "60000000000"),
            "data": [{"name": "hot", **_group_spec(32, "30000000000")}],
        }}}
        new = copy.deepcopy(old)
        new["spec"]["nodes"]["master"]["resources"]["limits"]["cpu"] = 8

        payload = generate_change_compute_payload(old, new)

        # Data fields stay as before; master compute is added additively.
        assert payload["new_cpu_limit"] == 32
        assert payload["old_master_cpu_limit"] == 16
        assert payload["new_master_cpu_limit"] == 8
        assert payload["new_master_memory_limit"] == "60000000000"

    def test_omits_master_fields_for_legacy_cluster(self):
        from crate.operator.change_compute import generate_change_compute_payload

        old = {"spec": {"nodes": {
            "data": [{"name": "hot", **_group_spec(32, "30000000000")}],
        }}}
        new = copy.deepcopy(old)
        new["spec"]["nodes"]["data"][0]["resources"]["limits"]["cpu"] = 30

        payload = generate_change_compute_payload(old, new)

        assert payload["new_cpu_limit"] == 30
        assert "old_master_cpu_limit" not in payload
        assert "new_master_cpu_limit" not in payload
