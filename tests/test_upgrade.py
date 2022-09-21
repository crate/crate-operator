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


import pytest
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi

from crate.operator.constants import API_GROUP, RESOURCE_CRATEDB
from crate.operator.cratedb import connection_factory
from crate.operator.create import get_statefulset_crate_command
from crate.operator.upgrade import upgrade_command

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    cluster_routing_allocation_enable_equals,
    create_test_sys_jobs_table,
    do_pod_ids_exist,
    do_pods_exist,
    is_cluster_healthy,
    is_kopf_handler_finished,
    start_cluster,
)


@pytest.mark.k8s
@pytest.mark.asyncio
async def test_upgrade_cluster(faker, namespace, kopf_runner, api_client):
    version_from = "4.8.2"
    version_to = "5.0.1"
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(name, namespace, core, coapi, 2, version_from)

    await assert_wait_for(
        True,
        do_pods_exist,
        core,
        namespace.metadata.name,
        {
            f"crate-data-hot-{name}-0",
            f"crate-data-hot-{name}-1",
        },
    )

    conn_factory = connection_factory(host, password)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        conn_factory,
        2,
        err_msg="Cluster wasn't healthy",
        timeout=DEFAULT_TIMEOUT,
    )

    await create_test_sys_jobs_table(conn_factory)

    pods = await core.list_namespaced_pod(namespace=namespace.metadata.name)
    original_pods = {p.metadata.uid for p in pods.items}
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "replace",
                "path": "/spec/cluster/version",
                "value": version_to,
            },
        ],
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
        False,
        do_pod_ids_exist,
        core,
        namespace.metadata.name,
        original_pods,
        timeout=DEFAULT_TIMEOUT * 15,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/upgrade.upgrade",
        err_msg="Upgrade has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/upgrade.restart",
        err_msg="Restart has not finished",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        2,
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


@pytest.mark.parametrize(
    "total_nodes, old_quorum, data_nodes, new_quorum",
    [(1, 1, 1, 1), (3, 2, 2, 2), (8, 5, 5, 3), (31, 16, 27, 14)],
)
def test_upgrade_sts_command(total_nodes, old_quorum, data_nodes, new_quorum):
    cmd = get_statefulset_crate_command(
        namespace="some-namespace",
        name="cluster1",
        master_nodes=[f"node-{i}" for i in range(total_nodes - data_nodes)],
        total_nodes_count=total_nodes,
        data_nodes_count=data_nodes,
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
    assert f"-Cgateway.recover_after_nodes={old_quorum}" in cmd
    assert f"-Cgateway.expected_nodes={total_nodes}" in cmd

    new_cmd = upgrade_command(cmd, data_nodes)
    assert f"-Cgateway.recover_after_data_nodes={new_quorum}" in new_cmd
    assert f"-Cgateway.expected_data_nodes={data_nodes}" in new_cmd
