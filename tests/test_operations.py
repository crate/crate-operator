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
from typing import Callable, Set

import psycopg2
import pytest
from aiopg import Connection
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi

from crate.operator.constants import API_GROUP, BACKOFF_TIME, RESOURCE_CRATEDB
from crate.operator.cratedb import connection_factory, get_healthiness
from crate.operator.utils.kubeapi import get_public_host, get_system_user_password

from .utils import assert_wait_for


async def do_pods_exist(core: CoreV1Api, namespace: str, expected: Set[str]) -> bool:
    pods = await core.list_namespaced_pod(namespace=namespace)
    return expected.issubset({p.metadata.name for p in pods.items})


async def do_pod_ids_exist(core: CoreV1Api, namespace: str, pod_ids: Set[str]) -> bool:
    pods = await core.list_namespaced_pod(namespace=namespace)
    return bool(pod_ids.intersection({p.metadata.uid for p in pods.items}))


async def is_cluster_healthy(conn_factory: Callable[[], Connection]):
    try:
        async with conn_factory() as conn:
            async with conn.cursor() as cursor:
                healthines = await get_healthiness(cursor)
                return healthines in {1, None}
    except psycopg2.DatabaseError:
        return False


@pytest.mark.k8s
@pytest.mark.asyncio
async def test_restart_cluster(
    faker, namespace, cleanup_handler, kopf_runner, api_client
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    # Clean up persistent volume after the test
    cleanup_handler.append(
        core.delete_persistent_volume(name=f"temp-pv-{namespace.metadata.name}-{name}")
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
                    "version": "4.3.0",
                },
                "nodes": {
                    "data": [
                        {
                            "name": "hot",
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
                        },
                        {
                            "name": "cold",
                            "replicas": 2,
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
                        },
                    ],
                },
            },
        },
    )

    host = await asyncio.wait_for(
        get_public_host(core, namespace.metadata.name, name),
        timeout=BACKOFF_TIME * 5,  # It takes a while to retrieve an external IP on AKS.
    )

    password = await get_system_user_password(core, namespace.metadata.name, name)

    await assert_wait_for(
        True,
        do_pods_exist,
        core,
        namespace.metadata.name,
        {
            f"crate-data-hot-{name}-0",
            f"crate-data-cold-{name}-0",
            f"crate-data-cold-{name}-1",
        },
    )

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        err_msg="Cluster wasn't healthy after 5 minutes.",
        timeout=BACKOFF_TIME * 5,
    )

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
                "value": "4.3.1",
            },
        ],
    )

    await assert_wait_for(
        False,
        do_pod_ids_exist,
        core,
        namespace.metadata.name,
        original_pods,
        timeout=BACKOFF_TIME * 15,
    )
