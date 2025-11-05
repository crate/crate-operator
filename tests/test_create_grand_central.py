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

import pytest
from kubernetes_asyncio.client import (
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
    start_cluster,
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

    conn_factory = connection_factory(host, password)

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
