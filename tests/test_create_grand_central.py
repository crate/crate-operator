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
from typing import Set
from unittest import mock

import pytest
from kubernetes_asyncio.client import (
    ApiException,
    AppsV1Api,
    CoreV1Api,
    CustomObjectsApi,
    NetworkingV1Api,
)

from crate.operator.constants import (
    API_GROUP,
    GC_USER_SECRET_NAME,
    GC_USERNAME,
    GRAND_CENTRAL_PROMETHEUS_PORT,
    GRAND_CENTRAL_RESOURCE_PREFIX,
    KOPF_STATE_STORE_PREFIX,
    RESOURCE_CRATEDB,
)
from crate.operator.cratedb import connection_factory
from crate.operator.utils.formatting import b64decode
from tests.test_bootstrap import does_user_exist

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    do_pods_exist,
    is_cluster_healthy,
    is_kopf_handler_finished,
    require_connection,
    start_cluster,
    wait_for_kopf_handler,
)

_GC_TRAEFIK_MIDDLEWARES = (
    "grand-central-cors",
    "grand-central-compress-js",
    "grand-central-buffering",
    "grand-central-ip-allowlist",
)


async def _start_gc_on_cluster(coapi, name, namespace_name, mock_bootstrap=None):
    """Patch grandCentral onto a running cluster and wait for the handler."""
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace_name,
        name=name,
        body=[
            {
                "op": "add",
                "path": "/spec/grandCentral",
                "value": {
                    "backendEnabled": True,
                    "backendImage": "cloud.registry.cr8.net/crate/grand-central:latest",
                    "apiUrl": "https://my-cratedb-api.cloud/",
                    "jwkUrl": "https://my-cratedb-api.cloud/api/v2/meta/jwk/",
                },
            }
        ],
    )


@pytest.mark.k8s
@pytest.mark.asyncio
async def test_create_grand_central(faker, namespace, kopf_runner, api_client):
    apps = AppsV1Api(api_client)
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    networking = NetworkingV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
        additional_cluster_spec={
            "externalDNS": "my-crate-cluster.aks1.eastus.azure.cratedb-dev.net."
        },
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

    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "add",
                "path": "/spec/grandCentral",
                "value": {
                    "backendEnabled": True,
                    "backendImage": "cloud.registry.cr8.net/crate/grand-central:latest",
                    "apiUrl": "https://my-cratedb-api.cloud/",
                    "jwkUrl": "https://my-cratedb-api.cloud/api/v2/meta/jwk/",
                },
            },
        ],
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/grand_central_create.spec.grandCentral",
        err_msg="Create handler has not finished",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        does_deployment_exist,
        apps,
        namespace.metadata.name,
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
    )
    deploy = await apps.read_namespaced_deployment(
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}", namespace.metadata.name
    )
    assert (
        deploy.spec.template.spec.containers[0].image
        == "cloud.registry.cr8.net/crate/grand-central:latest"
    )
    na = deploy.spec.template.spec.affinity.node_affinity
    selector = na.required_during_scheduling_ignored_during_execution
    terms = selector.node_selector_terms[0]
    expressions = terms.match_expressions
    assert [e.to_dict() for e in expressions] == [
        {
            "key": "cratedb",
            "operator": "In",
            "values": ["shared"],
        }
    ]
    await assert_wait_for(
        True,
        do_services_exist,
        core,
        namespace.metadata.name,
        {f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}"},
    )

    await assert_wait_for(
        True,
        does_ingress_exist,
        networking,
        namespace.metadata.name,
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
    )
    ingress = await networking.read_namespaced_ingress(
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}", namespace.metadata.name
    )
    assert (
        ingress.metadata.annotations["external-dns.alpha.kubernetes.io/hostname"]
        == "my-crate-cluster.gc.aks1.eastus.azure.cratedb-dev.net"
    )
    assert (
        ingress.metadata.annotations[
            "nginx.ingress.kubernetes.io/cors-allow-credentials"
        ]
        == "true"
    )
    assert (
        ingress.metadata.annotations["nginx.ingress.kubernetes.io/enable-cors"]
        == "true"
    )
    assert (
        ingress.metadata.annotations["nginx.ingress.kubernetes.io/cors-allow-origin"]
        == "$http_origin"
    )
    assert (
        ingress.metadata.annotations["nginx.ingress.kubernetes.io/cors-allow-methods"]
        == "GET,POST,PUT,PATCH,OPTIONS,DELETE"
    )
    assert (
        ingress.metadata.annotations["nginx.ingress.kubernetes.io/cors-allow-headers"]
        == "Content-Type,Authorization"
    )
    assert (
        ingress.metadata.annotations["nginx.ingress.kubernetes.io/cors-max-age"]
        == "7200"
    )

    await assert_wait_for(
        True,
        does_secret_exist,
        core,
        namespace.metadata.name,
        GC_USER_SECRET_NAME.format(name=name),
    )
    secrets = (await core.list_namespaced_secret(namespace.metadata.name)).items
    secret_pw = next(
        filter(
            lambda x: x.metadata.name == GC_USER_SECRET_NAME.format(name=name), secrets
        )
    )

    gc_admin_pw = b64decode(secret_pw.data["password"])

    await assert_wait_for(
        True,
        does_user_exist,
        host,
        gc_admin_pw,
        GC_USERNAME,
        timeout=DEFAULT_TIMEOUT * 3,
    )

    assert deploy.spec.template.metadata.annotations["prometheus.io/scrape"] == "true"
    assert deploy.spec.template.metadata.annotations["prometheus.io/port"] == str(
        GRAND_CENTRAL_PROMETHEUS_PORT
    )
    app_prometheus_port = next(
        (
            port.container_port
            for port in deploy.spec.template.spec.containers[0].ports
            if port.name == "prometheus"
        ),
        None,
    )
    assert app_prometheus_port == GRAND_CENTRAL_PROMETHEUS_PORT

    service = await core.read_namespaced_service(
        namespace=namespace.metadata.name,
        name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
    )
    svc_prometheus_port = next(
        (port for port in service.spec.ports if port.name == "prometheus"),
        None,
    )
    assert svc_prometheus_port
    assert svc_prometheus_port.port == GRAND_CENTRAL_PROMETHEUS_PORT
    assert svc_prometheus_port.target_port == GRAND_CENTRAL_PROMETHEUS_PORT


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_create_grand_central_traefik(
    mock_send_notification,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Creating a cluster with exposure=traefik and then enabling grandCentral
    should produce an HTTPRoute and all four Traefik Middlewares - no Ingress.
    """
    apps = AppsV1Api(api_client)
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    networking = NetworkingV1Api(api_client)
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
    )

    await _start_gc_on_cluster(coapi, name, namespace.metadata.name)

    # Deployment and Service are always created regardless of exposure
    await assert_wait_for(
        True,
        does_deployment_exist,
        apps,
        namespace.metadata.name,
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
    )
    await assert_wait_for(
        True,
        do_services_exist,
        core,
        namespace.metadata.name,
        {f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}"},
    )

    # HTTPRoute must exist
    await assert_wait_for(
        True,
        does_gc_httproute_exist,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="GC HTTPRoute was not created",
        timeout=DEFAULT_TIMEOUT,
    )

    # All four Middlewares must exist
    for mw in _GC_TRAEFIK_MIDDLEWARES:
        await assert_wait_for(
            True,
            does_gc_middleware_exist,
            coapi,
            namespace.metadata.name,
            mw,
            err_msg=f"Middleware '{mw}' was not created",
            timeout=DEFAULT_TIMEOUT,
        )

    cors_headers = await get_gc_cors_headers(
        coapi,
        namespace.metadata.name,
    )

    assert cors_headers["accessControlAllowMethods"] == [
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "OPTIONS",
        "DELETE",
    ]
    assert cors_headers["accessControlAllowHeaders"] == [
        "Content-Type",
        "Authorization",
    ]
    assert cors_headers["accessControlAllowCredentials"] is True
    assert cors_headers["accessControlMaxAge"] == 7200
    assert cors_headers["accessControlAllowOriginList"] == [
        "*"
    ], "Expected default origin list ['*'] when no cors setting is configured"

    # No nginx Ingress should be created
    await assert_wait_for(
        False,
        does_ingress_exist,
        networking,
        namespace.metadata.name,
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
        err_msg="Unexpected Ingress was created for traefik exposure",
        timeout=DEFAULT_TIMEOUT,
    )


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_gc_cors_origin_list_traefik(
    mock_send_notification,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    When spec.cluster.settings.http.cors.allow-origin contains a
    comma-separated list of origins, the grand-central-cors Middleware must
    split them into individual accessControlAllowOriginList entries rather
    than setting a single comma-joined string, which browsers reject.
    """
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    allowed_origins = [
        "https://console.cratedb-dev.cloud",
        "http://localhost:8000",
    ]

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
            "settings": {
                "http.cors.allow-origin": ",".join(allowed_origins),
            },
        },
    )
    await wait_for_kopf_handler(
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_create",
    )
    await _start_gc_on_cluster(coapi, name, namespace.metadata.name)

    await assert_wait_for(
        True,
        does_gc_middleware_exist,
        coapi,
        namespace.metadata.name,
        "grand-central-cors",
        err_msg="grand-central-cors Middleware was not created",
        timeout=DEFAULT_TIMEOUT,
    )

    cors_headers = await get_gc_cors_headers(coapi, namespace.metadata.name)

    assert (
        cors_headers["accessControlAllowOriginList"] == allowed_origins
    ), "Multi-origin CORS setting was not split into individual list entries"
    assert cors_headers["accessControlAllowCredentials"] is True


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
@mock.patch("crate.operator.grand_central.bootstrap_gc_admin_user")
async def test_gc_change_exposure_loadbalancer_to_traefik(
    mock_bootstrap,
    mock_send_notification,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Switching a cluster with GC from loadbalancer to traefik should delete
    the Ingress and create the HTTPRoute + Middlewares.
    """
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    networking = NetworkingV1Api(api_client)
    name = faker.domain_word()

    await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
        wait_for_healthy=False,
        additional_cluster_spec={
            "externalDNS": f"{name}.example.com",
        },
    )
    await wait_for_kopf_handler(
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_create",
        timeout=DEFAULT_TIMEOUT * 5,
    )
    await _start_gc_on_cluster(coapi, name, namespace.metadata.name)

    # Ingress exists before the switch
    await assert_wait_for(
        True,
        does_ingress_exist,
        networking,
        namespace.metadata.name,
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
        err_msg="Initial Ingress was not created",
        timeout=DEFAULT_TIMEOUT,
    )

    # Switch to traefik
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[{"op": "replace", "path": "/spec/cluster/exposure", "value": "traefik"}],
    )
    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Exposure change handler did not finish",
        timeout=DEFAULT_TIMEOUT * 2,
    )

    # Ingress gone, HTTPRoute + Middlewares present
    await assert_wait_for(
        False,
        does_ingress_exist,
        networking,
        namespace.metadata.name,
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
        err_msg="Ingress was not deleted after switching to traefik",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        True,
        does_gc_httproute_exist,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="GC HTTPRoute was not created after switching to traefik",
        timeout=DEFAULT_TIMEOUT,
    )
    for mw in _GC_TRAEFIK_MIDDLEWARES:
        await assert_wait_for(
            True,
            does_gc_middleware_exist,
            coapi,
            namespace.metadata.name,
            mw,
            err_msg=f"Middleware '{mw}' was not created after switching to traefik",
            timeout=DEFAULT_TIMEOUT,
        )


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
@mock.patch("crate.operator.grand_central.bootstrap_gc_admin_user")
async def test_gc_change_exposure_traefik_to_loadbalancer(
    mock_bootstrap,
    mock_send_notification,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Switching a cluster with GC from traefik to loadbalancer should delete
    the HTTPRoute + Middlewares and create the Ingress.
    """
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    networking = NetworkingV1Api(api_client)
    name = faker.domain_word()
    cidrs = ["10.0.0.0/8", "192.168.1.0/24"]

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
    )
    await _start_gc_on_cluster(coapi, name, namespace.metadata.name)

    # HTTPRoute exists before the switch
    await assert_wait_for(
        True,
        does_gc_httproute_exist,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="Initial HTTPRoute was not created",
        timeout=DEFAULT_TIMEOUT,
    )

    # Switch to loadbalancer
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {"op": "replace", "path": "/spec/cluster/exposure", "value": "loadbalancer"}
        ],
    )
    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Exposure change handler did not finish",
        timeout=DEFAULT_TIMEOUT * 2,
    )

    # HTTPRoute + Middlewares gone, Ingress present
    await assert_wait_for(
        False,
        does_gc_httproute_exist,
        coapi,
        namespace.metadata.name,
        name,
        err_msg="HTTPRoute was not deleted after switching to loadbalancer",
        timeout=DEFAULT_TIMEOUT,
    )
    for mw in _GC_TRAEFIK_MIDDLEWARES:
        await assert_wait_for(
            False,
            does_gc_middleware_exist,
            coapi,
            namespace.metadata.name,
            mw,
            err_msg=f"Middleware '{mw}' was not deleted after switching to loadbalancer",  # noqa
            timeout=DEFAULT_TIMEOUT,
        )
    await assert_wait_for(
        True,
        does_ingress_exist,
        networking,
        namespace.metadata.name,
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
        err_msg="Ingress was not created after switching to loadbalancer",
        timeout=DEFAULT_TIMEOUT,
    )
    ingress = await networking.read_namespaced_ingress(
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}", namespace.metadata.name
    )
    whitelist = ingress.metadata.annotations.get(
        "nginx.ingress.kubernetes.io/whitelist-source-range"
    )
    assert whitelist is not None, "whitelist-source-range annotation is missing"
    assert set(whitelist.split(",")) == set(
        cidrs
    ), f"Expected CIDRs {cidrs}, got {whitelist}"


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_gc_cidr_update_traefik(
    mock_send_notification,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    """
    Updating allowedCIDRs on a traefik cluster with GC should patch the
    ip-allowlist Middleware's sourceRange accordingly.
    """
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    initial_cidrs = ["10.0.0.0/8"]
    updated_cidrs = ["10.0.0.0/8", "192.168.1.0/24"]

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
    )
    await _start_gc_on_cluster(coapi, name, namespace.metadata.name)

    # ip-allowlist starts with initial_cidrs
    await assert_wait_for(
        True,
        does_gc_middleware_exist,
        coapi,
        namespace.metadata.name,
        "grand-central-ip-allowlist",
        err_msg="ip-allowlist Middleware was not created",
        timeout=DEFAULT_TIMEOUT,
    )
    cidrs = await get_gc_ip_allowlist_cidrs(coapi, namespace.metadata.name)
    assert set(cidrs) == set(initial_cidrs), f"Expected {initial_cidrs}, got {cidrs}"

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
        err_msg="CIDR update handler did not finish",
        timeout=DEFAULT_TIMEOUT,
    )

    # ip-allowlist now reflects updated_cidrs
    async def _cidrs_updated(coapi, namespace):
        cidrs = await get_gc_ip_allowlist_cidrs(coapi, namespace)
        return set(cidrs) == set(updated_cidrs)

    await assert_wait_for(
        True,
        _cidrs_updated,
        coapi,
        namespace.metadata.name,
        err_msg=f"ip-allowlist sourceRange was not updated to {updated_cidrs}",
        timeout=DEFAULT_TIMEOUT,
    )


async def does_deployment_exist(apps: AppsV1Api, namespace: str, name: str) -> bool:
    deployments = await apps.list_namespaced_deployment(namespace=namespace)
    return name in (d.metadata.name for d in deployments.items)


async def do_services_exist(
    core: CoreV1Api, namespace: str, expected: Set[str]
) -> bool:
    services = await core.list_namespaced_service(namespace)
    return expected.issubset({s.metadata.name for s in services.items})


async def does_ingress_exist(
    networking: NetworkingV1Api, namespace: str, name: str
) -> bool:
    ingresses = await networking.list_namespaced_ingress(namespace=namespace)
    return name in (i.metadata.name for i in ingresses.items)


async def does_secret_exist(core: CoreV1Api, namespace: str, name: str) -> bool:
    secrets = await core.list_namespaced_secret(namespace)
    return name in (s.metadata.name for s in secrets.items)


async def does_gc_httproute_exist(
    coapi: CustomObjectsApi, namespace: str, name: str
) -> bool:
    try:
        await coapi.get_namespaced_custom_object(
            group="gateway.networking.k8s.io",
            version="v1",
            namespace=namespace,
            plural="httproutes",
            name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
        )
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        raise


async def does_gc_middleware_exist(
    coapi: CustomObjectsApi, namespace: str, middleware_name: str
) -> bool:
    try:
        await coapi.get_namespaced_custom_object(
            group="traefik.io",
            version="v1alpha1",
            namespace=namespace,
            plural="middlewares",
            name=middleware_name,
        )
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        raise


async def get_gc_cors_headers(coapi: CustomObjectsApi, namespace: str) -> dict:
    try:
        mw = await coapi.get_namespaced_custom_object(
            group="traefik.io",
            version="v1alpha1",
            namespace=namespace,
            plural="middlewares",
            name="grand-central-cors",
        )
        return mw.get("spec", {}).get("headers", {})
    except ApiException as e:
        if e.status == 404:
            return {}
        raise


async def get_gc_ip_allowlist_cidrs(coapi: CustomObjectsApi, namespace: str) -> list:
    try:
        mw = await coapi.get_namespaced_custom_object(
            group="traefik.io",
            version="v1alpha1",
            namespace=namespace,
            plural="middlewares",
            name="grand-central-ip-allowlist",
        )
        return mw.get("spec", {}).get("ipAllowList", {}).get("sourceRange", [])
    except ApiException as e:
        if e.status == 404:
            return []
        raise


async def gc_deployment_replicas(apps: AppsV1Api, namespace: str, name: str) -> int:
    try:
        d = await apps.read_namespaced_deployment(
            f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}", namespace
        )
        return d.spec.replicas
    except ApiException as e:
        if e.status == 404:
            return -1
        raise
