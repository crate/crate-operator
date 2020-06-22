import asyncio
from typing import Callable, Set

import psycopg2
import pytest
from aiopg import Connection
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi

from crate.operator.constants import API_GROUP, BACKOFF_TIME, RESOURCE_CRATEDB
from crate.operator.cratedb import connection_factory, get_healthiness
from crate.operator.operations import restart_cluster
from crate.operator.utils.kubeapi import get_public_ip, get_system_user_password

from .utils import assert_wait_for


async def do_pods_exist(core: CoreV1Api, namespace: str, expected: Set[str]) -> bool:
    pods = await core.list_namespaced_pod(namespace=namespace)
    return expected.issubset({p.metadata.name for p in pods.items})


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
    faker, namespace, cleanup_handler, cratedb_crd, kopf_runner
):
    coapi = CustomObjectsApi()
    core = CoreV1Api()
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
                    "version": "4.1.5",
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

    ip_address = await asyncio.wait_for(
        get_public_ip(core, namespace.metadata.name, name),
        timeout=BACKOFF_TIME * 5,  # It takes a while to retrieve an external IP on AKS.
    )

    password = await get_system_user_password(namespace.metadata.name, name, core)

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
        connection_factory(ip_address, password),
        err_msg="Cluster wasn't healthy after 5 minutes.",
        timeout=BACKOFF_TIME * 5,
    )

    pods = await core.list_namespaced_pod(namespace=namespace.metadata.name)
    original_pods = {p.metadata.uid for p in pods.items}

    await asyncio.wait_for(
        restart_cluster(namespace.metadata.name, name, 3), BACKOFF_TIME * 15
    )

    pods = await core.list_namespaced_pod(namespace=namespace.metadata.name)
    new_pods = {p.metadata.uid for p in pods.items}

    assert original_pods.intersection(new_pods) == set()
