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
from typing import Callable

import psycopg2
import pytest
from aiopg import Connection
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi

from crate.operator.constants import API_GROUP, BACKOFF_TIME, RESOURCE_CRATEDB
from crate.operator.cratedb import (
    connection_factory,
    get_healthiness,
    get_number_of_nodes,
)
from crate.operator.utils.kubeapi import get_public_host, get_system_user_password

from .utils import assert_wait_for


async def is_cluster_healthy(
    conn_factory: Callable[[], Connection], expected_num_nodes: int
):
    try:
        async with conn_factory() as conn:
            async with conn.cursor() as cursor:
                num_nodes = await get_number_of_nodes(cursor)
                healthines = await get_healthiness(cursor)
                return expected_num_nodes == num_nodes and healthines in {1, None}
    except psycopg2.DatabaseError:
        return False


@pytest.mark.k8s
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "repl_master_from,repl_master_to,repl_hot_from,repl_hot_to,repl_cold_from,repl_cold_to",  # noqa
    [
        (0, 0, 1, 2, 0, 0),  # scale up from 1 to 2 data nodes
        (0, 0, 3, 2, 0, 0),  # scale down from 3 to 2 data nodes
        # These can't be tested due to license violations:
        # License is violated - Statement not allowed. If expired or more cluster nodes
        # are in need, please request a new license and use 'SET LICENSE' statement to
        # register it. Alternatively, if the allowed number of nodes is exceeded, use
        # the 'ALTER CLUSTER DECOMMISSION' statement to downscale your cluster
        # (3, 4, 1, 1, 0, 0),  # scale master nodes from 3 to 4
        # (4, 3, 1, 1, 0, 0),  # scale master nodes from 4 to 3
        # (3, 3, 1, 2, 3, 2),  # scale up hot data nodes and down cold data nodes
    ],
)
async def test_scale_cluster(
    repl_master_from,
    repl_master_to,
    repl_hot_from,
    repl_hot_to,
    repl_cold_from,
    repl_cold_to,
    faker,
    namespace,
    cleanup_handler,
    kopf_runner,
):
    coapi = CustomObjectsApi()
    core = CoreV1Api()
    name = faker.domain_word()

    # Clean up persistent volume after the test
    cleanup_handler.append(
        core.delete_persistent_volume(name=f"temp-pv-{namespace.metadata.name}-{name}")
    )
    body = {
        "apiVersion": "cloud.crate.io/v1",
        "kind": "CrateDB",
        "metadata": {"name": name},
        "spec": {
            "cluster": {
                "imageRegistry": "crate",
                "name": "my-crate-cluster",
                "version": "4.1.5",
            },
            "nodes": {"data": []},
        },
    }
    if repl_master_from:
        body["spec"]["nodes"]["master"] = {
            "replicas": repl_master_from,
            "resources": {
                "cpus": 0.5,
                "memory": "1Gi",
                "heapRatio": 0.25,
                "disk": {"storageClass": "default", "size": "16GiB", "count": 1},
            },
        }
    body["spec"]["nodes"]["data"].append(
        {
            "name": "hot",
            "replicas": repl_hot_from,
            "resources": {
                "cpus": 0.5,
                "memory": "1Gi",
                "heapRatio": 0.25,
                "disk": {"storageClass": "default", "size": "16GiB", "count": 1},
            },
        },
    )
    if repl_cold_from:
        body["spec"]["nodes"]["data"].append(
            {
                "name": "cold",
                "replicas": repl_cold_from,
                "resources": {
                    "cpus": 0.5,
                    "memory": "1Gi",
                    "heapRatio": 0.25,
                    "disk": {"storageClass": "default", "size": "16GiB", "count": 1},
                },
            },
        )
    await coapi.create_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        body=body,
    )

    host = await asyncio.wait_for(
        get_public_host(core, namespace.metadata.name, name),
        timeout=BACKOFF_TIME * 5,  # It takes a while to retrieve an external IP on AKS.
    )
    password = await get_system_user_password(namespace.metadata.name, name, core)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        repl_master_from + repl_hot_from + repl_cold_from,
        err_msg="Cluster wasn't healthy after 5 minutes.",
        timeout=BACKOFF_TIME * 5,
    )

    patch_body = []
    if repl_master_from != repl_master_to:
        patch_body.append(
            {
                "op": "replace",
                "path": "/spec/nodes/master/replicas",
                "value": repl_master_to,
            }
        )
    if repl_hot_from != repl_hot_to:
        patch_body.append(
            {
                "op": "replace",
                "path": "/spec/nodes/data/0/replicas",
                "value": repl_hot_to,
            }
        )
    if repl_cold_from != repl_cold_to:
        patch_body.append(
            {
                "op": "replace",
                "path": "/spec/nodes/data/1/replicas",
                "value": repl_cold_to,
            }
        )
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=patch_body,
    )

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        repl_master_to + repl_hot_to + repl_cold_to,
        err_msg="Cluster wasn't healthy after 5 minutes.",
        timeout=BACKOFF_TIME * 5,
    )
