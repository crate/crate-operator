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

"""
End-to-end tests for dedicated master nodes.

These tests create, suspend, expand and delete *real* CrateDB clusters. Like
all ``k8s`` tests they only run against the contexts allowed by ``conftest``
(a ``crate-*`` context, e.g. CI's disposable cluster, or ``minikube``), and are
skipped entirely unless ``--kube-config`` is given.

Locally, run them against a kubeconfig that contains only minikube, e.g.::

    export KUBECONFIG=/tmp/minikube-only.kubeconfig
    minikube start --cpus 6 --memory 12288
    python -m pytest tests/test_master_nodes.py -m k8s -vvv \\
        --kube-config "$KUBECONFIG" --kube-context minikube
"""

import asyncio
from unittest import mock

import pytest
from kubernetes_asyncio.client import AppsV1Api, CoreV1Api, CustomObjectsApi

from crate.operator.constants import (
    API_GROUP,
    KOPF_STATE_STORE_PREFIX,
    RESOURCE_CRATEDB,
)
from crate.operator.cratedb import connection_factory
from crate.operator.operations import get_pods_in_statefulset
from crate.operator.utils.kubeapi import get_host
from crate.operator.webhooks import WebhookEvent
from tests.utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    create_test_sys_jobs_table,
    is_cluster_healthy,
    is_kopf_handler_finished,
    require_connection,
    start_cluster,
)

# Dedicated-master e2e. These run as part of the normal suite; mark them so the
# whole module can be selected on its own with ``pytest -m master`` (and
# excluded with ``-m "not master"`` if ever needed).
pytestmark = pytest.mark.master


# The CRD enforces an odd master count >= 3. Three small masters + one data
# node keeps a master cluster within a local minikube's resources while still
# exercising the masters-first ordering. cpu must be a number (not "500m").
MASTER_REPLICAS = 3
HOT_REPLICAS = 1
SMALL_RESOURCES = {
    "limits": {"cpu": 1, "memory": "2Gi"},
    "requests": {"cpu": 0.5, "memory": "1Gi"},
}


async def _master_sts(apps: AppsV1Api, namespace: str, name: str):
    return await apps.read_namespaced_stateful_set(
        namespace=namespace, name=f"crate-master-{name}"
    )


async def _master_sts_replicas(apps: AppsV1Api, namespace: str, name: str) -> int:
    return (await _master_sts(apps, namespace, name)).spec.replicas


async def _master_pods_gone(core: CoreV1Api, namespace: str, name: str) -> bool:
    return len(await get_pods_in_statefulset(core, namespace, name, "master")) == 0


async def _master_pods_present(core: CoreV1Api, namespace: str, name: str) -> bool:
    return len(await get_pods_in_statefulset(core, namespace, name, "master")) > 0


async def _data_nodes_ready(
    apps: AppsV1Api, namespace: str, name: str, expected: int
) -> bool:
    sts = await apps.read_namespaced_stateful_set(
        namespace=namespace, name=f"crate-data-hot-{name}"
    )
    return (sts.status.ready_replicas or 0) == expected


async def _data_service_target_pods(core: CoreV1Api, namespace: str, name: str):
    """Pod names currently backing the client-facing ``crate-<name>`` service."""
    ep = await core.read_namespaced_endpoints(f"crate-{name}", namespace)
    return {
        addr.target_ref.name
        for subset in (ep.subsets or [])
        for addr in (subset.addresses or [])
        if addr.target_ref is not None
    }


async def _data_service_excludes_masters(
    core: CoreV1Api, namespace: str, name: str
) -> bool:
    """True once the client service has endpoints, none of them master pods."""
    pods = await _data_service_target_pods(core, namespace, name)
    return bool(pods) and not any(p.startswith(f"crate-master-{name}-") for p in pods)


def _scale_data(coapi, name, namespace, replicas):
    return coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {"op": "replace", "path": "/spec/nodes/data/0/replicas", "value": replicas}
        ],
    )


def _patch(coapi, name, namespace, path, value):
    return coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[{"op": "replace", "path": path, "value": value}],
    )


def _scale_webhook_reported_master(mock_send, *, new_master_replicas) -> bool:
    for call in mock_send.await_args_list:
        if (
            call.args[2] == WebhookEvent.SCALE
            and call.args[3].get("new_master_replicas") == new_master_replicas
        ):
            return True
    return False


def _compute_webhook_reported_master(mock_send) -> bool:
    for call in mock_send.await_args_list:
        if (
            call.args[2] == WebhookEvent.COMPUTE_CHANGED
            and call.args[3].get("new_master_cpu_limit") is not None
        ):
            return True
    return False


@pytest.mark.k8s
@pytest.mark.asyncio
async def test_create_cluster_with_dedicated_masters(
    faker, namespace, kopf_runner, api_client
):
    """A cluster with dedicated masters forms successfully: the master STS is
    created and ready, and the cluster reports masters + data nodes healthy
    (which only happens if masters come up before data)."""
    apps = AppsV1Api(api_client)
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(
        name,
        namespace,
        core,
        coapi,
        hot_nodes=HOT_REPLICAS,
        master_nodes=MASTER_REPLICAS,
        resource_requests=SMALL_RESOURCES,
    )

    assert await _master_sts_replicas(apps, namespace.metadata.name, name) == (
        MASTER_REPLICAS
    )
    assert await _master_pods_present(core, namespace.metadata.name, name)

    # Healthy with the *combined* node count -> masters joined the cluster.
    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(*require_connection(host, password)),
        MASTER_REPLICAS + HOT_REPLICAS,
        err_msg="Master cluster wasn't healthy.",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    # The client-facing service must route to data nodes only: dedicated
    # masters are deliberately kept out of the load-balancer pool.
    await assert_wait_for(
        True,
        _data_service_excludes_masters,
        core,
        namespace.metadata.name,
        name,
        err_msg="dedicated master pods must be excluded from the client service",
        timeout=DEFAULT_TIMEOUT,
    )


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_suspend_resume_cluster_with_masters(
    mock_send, faker, namespace, kopf_runner, api_client
):
    """Suspend scales masters to 0 (after data); resume brings them back (before
    data). The SCALE webhook reports the effective master count: 0 on suspend,
    restored on resume (B1)."""
    apps = AppsV1Api(api_client)
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(
        name,
        namespace,
        core,
        coapi,
        hot_nodes=HOT_REPLICAS,
        master_nodes=MASTER_REPLICAS,
        resource_requests=SMALL_RESOURCES,
    )
    # before_cluster_update checks for running snapshots via the configured
    # JOBS_TABLE, so it must exist or every update operation stalls.
    await create_test_sys_jobs_table(
        connection_factory(*require_connection(host, password))
    )

    # --- Suspend ---
    await _scale_data(coapi, name, namespace, 0)
    await asyncio.sleep(1.0)
    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Suspend has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )
    # Masters scaled to 0 and their pods are gone.
    assert await _master_sts_replicas(apps, namespace.metadata.name, name) == 0
    await assert_wait_for(
        True,
        _master_pods_gone,
        core,
        namespace.metadata.name,
        name,
        err_msg="Master pods still present after suspend.",
        timeout=DEFAULT_TIMEOUT,
    )
    # B1: billing sees the masters as effectively 0 while suspended.
    assert _scale_webhook_reported_master(mock_send, new_master_replicas=0)

    # --- Resume ---
    await _scale_data(coapi, name, namespace, HOT_REPLICAS)
    await asyncio.sleep(1.0)
    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Resume has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )
    assert await _master_sts_replicas(apps, namespace.metadata.name, name) == (
        MASTER_REPLICAS
    )
    host = await get_host(core, namespace.metadata.name, name)
    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(*require_connection(host, password)),
        MASTER_REPLICAS + HOT_REPLICAS,
        err_msg="Cluster wasn't healthy after resume.",
        timeout=DEFAULT_TIMEOUT * 5,
    )
    # B1: masters restored on resume.
    assert _scale_webhook_reported_master(
        mock_send, new_master_replicas=MASTER_REPLICAS
    )


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_change_compute_on_master(
    mock_send, faker, namespace, kopf_runner, api_client
):
    """Changing a master's CPU patches the master StatefulSet (and -Cprocessors)
    and emits master compute in the CHANGE_COMPUTE webhook (B2)."""
    apps = AppsV1Api(api_client)
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(
        name,
        namespace,
        core,
        coapi,
        hot_nodes=HOT_REPLICAS,
        master_nodes=MASTER_REPLICAS,
        resource_requests=SMALL_RESOURCES,
    )
    # before_cluster_update checks for running snapshots via the configured
    # JOBS_TABLE, so it must exist or the compute change stalls.
    await create_test_sys_jobs_table(
        connection_factory(*require_connection(host, password))
    )

    await _patch(coapi, name, namespace, "/spec/nodes/master/resources/limits/cpu", 2)
    await asyncio.sleep(1.0)
    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Master compute change has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    sts = await _master_sts(apps, namespace.metadata.name, name)
    crate_container = next(
        c for c in sts.spec.template.spec.containers if c.name == "crate"
    )
    assert crate_container.resources.limits["cpu"] == str(2)
    assert "-Cprocessors=2" in crate_container.command

    # B2: master compute is present in the CHANGE_COMPUTE webhook.
    assert _compute_webhook_reported_master(mock_send)


@pytest.mark.k8s
@pytest.mark.asyncio
async def test_legacy_cluster_without_masters_regression(
    faker, namespace, kopf_runner, api_client
):
    """Regression: a cluster without dedicated masters still creates, suspends
    and resumes correctly (no master logic kicks in)."""
    apps = AppsV1Api(api_client)
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(
        name,
        namespace,
        core,
        coapi,
        hot_nodes=HOT_REPLICAS,
        resource_requests=SMALL_RESOURCES,
    )
    await create_test_sys_jobs_table(
        connection_factory(*require_connection(host, password))
    )

    # No master StatefulSet exists for a legacy cluster.
    stss = await apps.list_namespaced_stateful_set(namespace=namespace.metadata.name)
    assert f"crate-master-{name}" not in {s.metadata.name for s in stss.items}

    # Suspend / resume round-trip still works.
    await _scale_data(coapi, name, namespace, 0)
    await asyncio.sleep(1.0)
    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Suspend has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )
    await _scale_data(coapi, name, namespace, HOT_REPLICAS)
    await asyncio.sleep(1.0)
    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Resume has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )
    await assert_wait_for(
        True,
        _data_nodes_ready,
        apps,
        namespace.metadata.name,
        name,
        HOT_REPLICAS,
        err_msg="Data nodes were not scaled back up after resume.",
        timeout=DEFAULT_TIMEOUT * 5,
    )
