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
from kubernetes_asyncio.client import AppsV1Api, CoreV1Api, CustomObjectsApi

from crate.operator.constants import (
    API_GROUP,
    GRAND_CENTRAL_RESOURCE_PREFIX,
    RESOURCE_CRATEDB,
)

from .utils import DEFAULT_TIMEOUT, assert_wait_for, start_cluster

pytestmark = [pytest.mark.k8s, pytest.mark.asyncio]


async def test_update_grand_central(
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    apps = AppsV1Api(api_client)
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    await start_cluster(
        name,
        namespace,
        core,
        coapi,
        1,
        additional_cluster_spec={
            "externalDNS": "my-crate-cluster.aks1.eastus.azure.cratedb-dev.net."
        },
        grand_central_spec={
            "backendEnabled": True,
            "backendImage": "cloud.registry.cr8.net/crate/grand-central:0.6.1",
            "apiUrl": "https://my-cratedb-api.cloud/",
            "jwkUrl": "https://my-cratedb-api.cloud/api/v2/meta/jwk/",
        },
    )

    await assert_wait_for(
        True,
        _is_container_image_updated,
        apps,
        name,
        namespace.metadata.name,
        "cloud.registry.cr8.net/crate/grand-central:0.6.1",
        err_msg="Container image has not been updated.",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        _is_init_container_image_updated,
        apps,
        name,
        namespace.metadata.name,
        "cloud.registry.cr8.net/crate/grand-central:0.6.1",
        err_msg="InitContainer image has not been updated.",
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
                "op": "replace",
                "path": "/spec/grandCentral/backendImage",
                "value": "cloud.registry.cr8.net/crate/grand-central:latest",
            },
        ],
    )

    await assert_wait_for(
        True,
        _is_container_image_updated,
        apps,
        name,
        namespace.metadata.name,
        "cloud.registry.cr8.net/crate/grand-central:latest",
        err_msg="Container image has not been updated.",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        _is_init_container_image_updated,
        apps,
        name,
        namespace.metadata.name,
        "cloud.registry.cr8.net/crate/grand-central:latest",
        err_msg="InitContainer image has not been updated.",
        timeout=DEFAULT_TIMEOUT,
    )


async def _is_container_image_updated(apps, name, namespace, expected_value):
    deploy = await apps.read_namespaced_deployment(
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}", namespace
    )
    return deploy.spec.template.spec.containers[0].image == expected_value


async def _is_init_container_image_updated(apps, name, namespace, expected_value):
    deploy = await apps.read_namespaced_deployment(
        f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}", namespace
    )
    return deploy.spec.template.spec.init_containers[0].image == expected_value
