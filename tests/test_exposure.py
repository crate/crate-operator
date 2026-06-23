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

from typing import Optional
from unittest import mock

import pytest
from kubernetes_asyncio.client import ApiException, CoreV1Api, CustomObjectsApi

from crate.operator.constants import (
    API_GROUP,
    KOPF_STATE_STORE_PREFIX,
    RESOURCE_CRATEDB,
)

from .utils import (
    CLUSTER_CREATE_TIMEOUT,
    DEFAULT_TIMEOUT,
    assert_wait_for,
    is_kopf_handler_finished,
    start_cluster,
    wait_for_kopf_handler,
)

pytestmark = [pytest.mark.k8s, pytest.mark.asyncio]


async def middleware_exists(
    custom_api: CustomObjectsApi, namespace: str, name: str
) -> bool:
    try:
        await custom_api.get_namespaced_custom_object(
            group="traefik.io",
            version="v1alpha1",
            namespace=namespace,
            plural="middlewaretcps",
            name=f"cratedb-allow-{name}",
        )
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        raise


async def ingress_route_exists(
    custom_api: CustomObjectsApi, namespace: str, name: str, suffix: str
) -> bool:
    try:
        await custom_api.get_namespaced_custom_object(
            group="traefik.io",
            version="v1alpha1",
            namespace=namespace,
            plural="ingressroutetcps",
            name=f"crate-{suffix}-{name}",
        )
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        raise


async def ingress_route_has_middleware(
    custom_api: CustomObjectsApi, namespace: str, name: str, suffix: str
) -> bool:
    try:
        irt = await custom_api.get_namespaced_custom_object(
            group="traefik.io",
            version="v1alpha1",
            namespace=namespace,
            plural="ingressroutetcps",
            name=f"crate-{suffix}-{name}",
        )
        routes = irt.get("spec", {}).get("routes", [])
        for route in routes:
            if "middlewares" in route:
                return True
        return False
    except ApiException as e:
        if e.status == 404:
            return False
        raise


async def get_service(core: CoreV1Api, namespace: str, name: str):
    try:
        return await core.read_namespaced_service(f"crate-{name}", namespace)
    except ApiException as e:
        if e.status == 404:
            return None
        raise


async def service_type(core: CoreV1Api, namespace: str, name: str) -> Optional[str]:
    svc = await get_service(core, namespace, name)
    return svc.spec.type if svc else None


async def service_is_clusterip(core: CoreV1Api, namespace: str, name: str) -> bool:
    svc = await get_service(core, namespace, name)
    return svc is not None and svc.spec.type == "ClusterIP"


async def service_is_loadbalancer(core: CoreV1Api, namespace: str, name: str) -> bool:
    svc = await get_service(core, namespace, name)
    return svc is not None and svc.spec.type == "LoadBalancer"


@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_create_traefik_with_cidrs(
    mock_send_notification: mock.AsyncMock,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Test that a Traefik‑exposed cluster with non‑empty allowedCIDRs creates a
    ClusterIP service, a MiddlewareTCP, and IngressRouteTCPs that reference the
    middleware.
    """
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    cidrs = ["192.168.1.0/24", "10.0.0.0/8"]

    await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
        wait_for_healthy=False,
        wait_for_lb=False,
        additional_cluster_spec={
            "exposure": "traefik",
            "externalDNS": f"{name}.example.com",
            "allowedCIDRs": cidrs,
        },
    )

    await wait_for_kopf_handler(
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_create",
        timeout=CLUSTER_CREATE_TIMEOUT,
    )

    # Service should be ClusterIP
    await assert_wait_for(
        True,
        service_is_clusterip,
        core,
        namespace.metadata.name,
        name,
        err_msg="Service was not ClusterIP",
        timeout=DEFAULT_TIMEOUT,
    )

    # Middleware should exist
    await assert_wait_for(
        True,
        middleware_exists,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="Middleware was not created",
        timeout=DEFAULT_TIMEOUT,
    )

    # IngressRoutes should exist and have middleware reference
    for suffix in ("pg", "http"):
        await assert_wait_for(
            True,
            ingress_route_exists,
            coapi,
            namespace.metadata.name,
            name,
            suffix,
            err_msg="IngressRoute was not created",
            timeout=DEFAULT_TIMEOUT,
        )
        await assert_wait_for(
            True,
            ingress_route_has_middleware,
            coapi,
            namespace.metadata.name,
            name,
            suffix,
            err_msg="IngressRoute does not have middleware reference",
            timeout=DEFAULT_TIMEOUT,
        )


@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_create_traefik_without_cidrs(
    mock_send_notification: mock.AsyncMock,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Test that a Traefik‑exposed cluster with empty allowedCIDRs creates a
    ClusterIP service, no MiddlewareTCP, and IngressRouteTCPs without a
    middleware reference.
    """
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
        wait_for_healthy=False,
        wait_for_lb=False,
        additional_cluster_spec={
            "exposure": "traefik",
            "externalDNS": f"{name}.example.com",
            "allowedCIDRs": [],  # empty list
        },
    )

    await wait_for_kopf_handler(
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_create",
        timeout=CLUSTER_CREATE_TIMEOUT,
    )

    # Service ClusterIP
    await assert_wait_for(
        True,
        service_is_clusterip,
        core,
        namespace.metadata.name,
        name,
        err_msg="Service was not ClusterIP",
        timeout=DEFAULT_TIMEOUT,
    )

    # No middleware
    await assert_wait_for(
        False,
        middleware_exists,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="Middleware was created",
        timeout=DEFAULT_TIMEOUT,
    )

    # IngressRoutes exist but have no middleware
    for suffix in ("pg", "http"):
        await assert_wait_for(
            True,
            ingress_route_exists,
            coapi,
            namespace.metadata.name,
            name,
            suffix,
            err_msg="IngressRoute was not created",
            timeout=DEFAULT_TIMEOUT,
        )
        await assert_wait_for(
            False,
            ingress_route_has_middleware,
            coapi,
            namespace.metadata.name,
            name,
            suffix,
            err_msg="IngressRoute has middleware reference",
            timeout=DEFAULT_TIMEOUT,
        )


@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_update_cidrs_traefik_empty_to_nonempty(
    mock_send_notification: mock.AsyncMock,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Test that updating allowedCIDRs from empty to non‑empty on a Traefik
    cluster creates the missing MiddlewareTCP and adds the middleware
    reference to the IngressRouteTCPs.
    """
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    initial_cidrs: list[str] = []
    updated_cidrs: list[str] = ["192.168.1.0/24"]

    await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
        wait_for_healthy=False,
        wait_for_lb=False,
        additional_cluster_spec={
            "exposure": "traefik",
            "externalDNS": f"{name}.example.com",
            "allowedCIDRs": initial_cidrs,
        },
    )

    await wait_for_kopf_handler(
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_create",
        timeout=CLUSTER_CREATE_TIMEOUT,
    )

    # Service should be ClusterIP
    await assert_wait_for(
        True,
        service_is_clusterip,
        core,
        namespace.metadata.name,
        name,
        err_msg="Service was not ClusterIP",
        timeout=DEFAULT_TIMEOUT,
    )

    # Initially no middleware
    await assert_wait_for(
        False,
        middleware_exists,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="Middleware was created",
        timeout=DEFAULT_TIMEOUT,
    )

    # Update CIDRs
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "replace",
                "path": "/spec/cluster/allowedCIDRs",
                "value": updated_cidrs,
            }
        ],
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/service_cidr_changes/spec.cluster.allowedCIDRs",
        timeout=DEFAULT_TIMEOUT,
    )

    # Middleware should be created
    await assert_wait_for(
        True,
        middleware_exists,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="Middleware was not created",
        timeout=DEFAULT_TIMEOUT,
    )

    # IngressRoutes should now have middleware reference
    for suffix in ("pg", "http"):
        await assert_wait_for(
            True,
            ingress_route_has_middleware,
            coapi,
            namespace.metadata.name,
            name,
            suffix,
            err_msg="IngressRoute does not have middleware reference",
            timeout=DEFAULT_TIMEOUT,
        )


@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_update_cidrs_traefik_nonempty_to_empty(
    mock_send_notification: mock.AsyncMock,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Test that updating allowedCIDRs from non‑empty to empty on a Traefik
    cluster deletes the MiddlewareTCP and removes the middleware reference
    from the IngressRouteTCPs.
    """
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    initial_cidrs: list[str] = ["192.168.1.0/24"]
    updated_cidrs: list[str] = []

    await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
        wait_for_healthy=False,
        wait_for_lb=False,
        additional_cluster_spec={
            "exposure": "traefik",
            "externalDNS": f"{name}.example.com",
            "allowedCIDRs": initial_cidrs,
        },
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_create.traefik_resources",
        timeout=DEFAULT_TIMEOUT,
    )

    await wait_for_kopf_handler(
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_create",
        timeout=CLUSTER_CREATE_TIMEOUT,
    )

    # Initially middleware exists
    await assert_wait_for(
        True,
        middleware_exists,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="Middleware was not created",
        timeout=DEFAULT_TIMEOUT,
    )

    # Update CIDRs to empty
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "replace",
                "path": "/spec/cluster/allowedCIDRs",
                "value": updated_cidrs,
            }
        ],
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/service_cidr_changes/spec.cluster.allowedCIDRs",
        timeout=DEFAULT_TIMEOUT,
    )

    # Middleware should be deleted
    await assert_wait_for(
        False,
        middleware_exists,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="Middleware was not deleted",
        timeout=DEFAULT_TIMEOUT,
    )

    # IngressRoutes should have no middleware reference
    for suffix in ("pg", "http"):
        await assert_wait_for(
            False,
            ingress_route_has_middleware,
            coapi,
            namespace.metadata.name,
            name,
            suffix,
            err_msg="IngressRoute still has middleware reference",
            timeout=DEFAULT_TIMEOUT,
        )


@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_change_exposure_loadbalancer_to_traefik(
    mock_send_notification: mock.AsyncMock,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Test changing exposure from loadbalancer to traefik: the service becomes
    ClusterIP, and Traefik resources (middleware + ingress routes) are created.
    """
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    cidrs = ["10.0.0.0/8"]

    # Create with default exposure (loadbalancer)
    await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
        wait_for_healthy=False,
        additional_cluster_spec={
            "externalDNS": f"{name}.example.com",
            "allowedCIDRs": cidrs,
        },
    )

    await wait_for_kopf_handler(
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_create.bootstrap",
        timeout=CLUSTER_CREATE_TIMEOUT,
    )

    # Initially service is LoadBalancer, no Traefik resources
    await assert_wait_for(
        True,
        service_is_loadbalancer,
        core,
        namespace.metadata.name,
        name,
        err_msg="Service was not LoadBalancer",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        False,
        middleware_exists,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="Middleware was created",
        timeout=DEFAULT_TIMEOUT,
    )

    # Change exposure to traefik
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "replace",
                "path": "/spec/cluster/exposure",
                "value": "traefik",
            }
        ],
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        timeout=DEFAULT_TIMEOUT,
    )

    # Now service should be ClusterIP, middleware and ingress routes exist
    await assert_wait_for(
        True,
        service_is_clusterip,
        core,
        namespace.metadata.name,
        name,
        err_msg="Service was not ClusterIP",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        True,
        middleware_exists,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="Middleware was not created",
        timeout=DEFAULT_TIMEOUT,
    )
    for suffix in ("pg", "http"):
        await assert_wait_for(
            True,
            ingress_route_exists,
            coapi,
            namespace.metadata.name,
            name,
            suffix,
            err_msg="IngressRoute was not created",
            timeout=DEFAULT_TIMEOUT,
        )


@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_change_exposure_traefik_to_loadbalancer(
    mock_send_notification: mock.AsyncMock,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Test changing exposure from traefik to loadbalancer: the service becomes
    LoadBalancer, and all Traefik resources (middleware + ingress routes) are deleted.
    """
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
        wait_for_healthy=False,
        wait_for_lb=False,
        additional_cluster_spec={
            "exposure": "traefik",
            "externalDNS": f"{name}.example.com",
        },
    )

    await wait_for_kopf_handler(
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_create",
        timeout=CLUSTER_CREATE_TIMEOUT,
    )

    # Initially Traefik resources exist
    await assert_wait_for(
        True,
        service_is_clusterip,
        core,
        namespace.metadata.name,
        name,
        err_msg="Service was not ClusterIP",
        timeout=DEFAULT_TIMEOUT,
    )
    for suffix in ("pg", "http"):
        await assert_wait_for(
            True,
            ingress_route_exists,
            coapi,
            namespace.metadata.name,
            name,
            suffix,
            err_msg="IngressRoute was not created",
            timeout=DEFAULT_TIMEOUT,
        )

    # Change exposure to loadbalancer
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "replace",
                "path": "/spec/cluster/exposure",
                "value": "loadbalancer",
            }
        ],
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        timeout=DEFAULT_TIMEOUT,
    )

    # Service becomes LoadBalancer, Traefik resources deleted
    await assert_wait_for(
        False,
        middleware_exists,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="Middleware was not deleted",
        timeout=DEFAULT_TIMEOUT,
    )
    for suffix in ("pg", "http"):
        await assert_wait_for(
            False,
            ingress_route_exists,
            coapi,
            namespace.metadata.name,
            name,
            suffix,
            err_msg="IngressRoute was not deleted",
            timeout=DEFAULT_TIMEOUT,
        )
    await assert_wait_for(
        True,
        service_is_loadbalancer,
        core,
        namespace.metadata.name,
        name,
        err_msg="Service was not LoadBalancer",
        timeout=DEFAULT_TIMEOUT,
    )
