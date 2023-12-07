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

import logging
from typing import Any, List, Optional

import kopf
from kubernetes_asyncio.client import (
    AppsV1Api,
    CoreV1Api,
    NetworkingV1Api,
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1EnvVar,
    V1EnvVarSource,
    V1HTTPGetAction,
    V1HTTPIngressPath,
    V1HTTPIngressRuleValue,
    V1Ingress,
    V1IngressBackend,
    V1IngressRule,
    V1IngressServiceBackend,
    V1IngressSpec,
    V1IngressTLS,
    V1LabelSelector,
    V1LocalObjectReference,
    V1ObjectMeta,
    V1OwnerReference,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Probe,
    V1ResourceRequirements,
    V1SecretKeySelector,
    V1Service,
    V1ServiceBackendPort,
    V1ServicePort,
    V1ServiceSpec,
)
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.constants import LABEL_NAME
from crate.operator.utils import crate
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.kubeapi import call_kubeapi
from crate.operator.utils.typing import LabelType


def get_grand_central_deployment(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    spec: kopf.Spec,
) -> V1Deployment:
    env = [
        V1EnvVar(
            name="CRATEDB_CENTER_CRATEDB_ADDRESS", value=f"crate-discovery-{name}"
        ),
        V1EnvVar(
            name="CRATEDB_CENTER_VERIFY_SSL",
            value="false",
        ),
        V1EnvVar(name="CRATEDB_CENTER_CLUSTER_ID", value=name),
        V1EnvVar(
            name="CRATEDB_CENTER_JWK_ENDPOINT",
            value=spec["grandCentral"]["jwkUrl"],
        ),
        V1EnvVar(
            name="CRATEDB_CENTER_CRATEDB_PASSWORD",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    key="password", name=f"user-password-{name}-0"
                ),
            ),
        ),
    ]
    return V1Deployment(
        metadata=V1ObjectMeta(
            name=f"grand-central-{name}",
            labels=labels,
            owner_references=owner_references,
        ),
        spec=V1DeploymentSpec(
            replicas=1,
            selector=V1LabelSelector(
                match_labels={
                    LABEL_NAME: f"grand-central-{name}",
                }
            ),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(
                    labels=labels,
                    name=f"grand-central-{name}",
                ),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            env=env,
                            image=spec["grandCentral"]["backendImage"],
                            image_pull_policy="Never",
                            name="grand-central-api",
                            ports=[
                                V1ContainerPort(
                                    container_port=5050,
                                    name="grand-central",
                                )
                            ],
                            resources=V1ResourceRequirements(
                                limits={
                                    "cpu": 2,
                                    "memory": "1Gi",
                                },
                                requests={"cpu": "500m", "memory": "1Gi"},
                            ),
                            liveness_probe=V1Probe(
                                http_get=V1HTTPGetAction(path="/api/health", port=5050),
                                initial_delay_seconds=60,
                                period_seconds=10,
                            ),
                        )
                    ],
                    image_pull_secrets=image_pull_secrets,
                    restart_policy="Always",
                ),
            ),
        ),
    )


def get_grand_central_service(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
) -> V1Service:
    return V1Service(
        metadata=V1ObjectMeta(
            name=f"grand-central-{name}",
            labels=labels,
            owner_references=owner_references,
        ),
        spec=V1ServiceSpec(
            type="ClusterIP",
            ports=[
                V1ServicePort(name="api", port=5050, target_port=5050),
            ],
            selector={LABEL_NAME: f"grand-central-{name}"},
        ),
    )


def get_grand_central_ingress(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    hostname: str,
) -> V1Ingress:
    return V1Ingress(
        metadata=V1ObjectMeta(
            name=f"grand-central-{name}",
            labels=labels,
            owner_references=owner_references,
            annotations={
                "external-dns.alpha.kubernetes.io/hostname": hostname,
                "nginx.ingress.kubernetes.io/proxy-body-size": "1Gi",
                "nginx.ingress.kubernetes.io/configuration-snippet": (
                    """
                    gzip on;
                    gzip_types
                        application/javascript
                        text/javascript;
                    more_set_headers "X-XSS-Protection: 1;mode=block"
                                    "X-Frame-Options: DENY"
                                    "X-Content-Type-Options: nosniff"
                                    "Access-Control-Allow-Origin: $http_origin"
                                    "Access-Control-Allow-Headers: Content-Type"
                                    "Access-Control-Allow-Credentials: true"
                                    "Referrer-Policy: strict-origin-when-cross-origin"
                                    ;
                    """
                ),
                "nginx.ingress.kubernetes.io/proxy-buffer-size": "64k",
                "nginx.ingress.kubernetes.io/ssl-redirect": "true",
            },
        ),
        spec=V1IngressSpec(
            rules=[
                V1IngressRule(
                    host=hostname,
                    http=V1HTTPIngressRuleValue(
                        paths=[
                            V1HTTPIngressPath(
                                path="/api",
                                path_type="ImplementationSpecific",
                                backend=V1IngressBackend(
                                    service=V1IngressServiceBackend(
                                        port=V1ServiceBackendPort(
                                            number=5050,
                                        ),
                                        name=f"grand-central-{name}",
                                    )
                                ),
                            )
                        ]
                    ),
                ),
                V1IngressRule(
                    host=hostname,
                    http=V1HTTPIngressRuleValue(
                        paths=[
                            V1HTTPIngressPath(
                                path="/socket.io",
                                path_type="ImplementationSpecific",
                                backend=V1IngressBackend(
                                    service=V1IngressServiceBackend(
                                        port=V1ServiceBackendPort(
                                            number=5050,
                                        ),
                                        name=f"cratedb-center-{name}",
                                    )
                                ),
                            )
                        ]
                    ),
                ),
            ],
            tls=[
                V1IngressTLS(
                    hosts=[hostname],
                    secret_name="keystore-certificate-grandcentral-wildcard",
                )
            ],
        ),
    )


async def create_grand_central_backend(
    owner_references: Optional[List[V1OwnerReference]],
    namespace: str,
    name: str,
    spec: kopf.Spec,
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    labels: LabelType,
    hostname: str,
    logger: logging.Logger,
) -> None:
    async with ApiClient() as api_client:
        apps = AppsV1Api(api_client)
        core = CoreV1Api(api_client)
        networking = NetworkingV1Api(api_client)

        await call_kubeapi(
            apps.create_namespaced_deployment,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_grand_central_deployment(
                owner_references,
                name,
                labels,
                image_pull_secrets,
                spec,
            ),
        )
        await call_kubeapi(
            core.create_namespaced_service,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_grand_central_service(owner_references, name, labels),
        )
        await call_kubeapi(
            networking.create_namespaced_ingress,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_grand_central_ingress(owner_references, name, labels, hostname),
        )


async def update_grand_central_deployment_image(
    namespace: str, name: str, image: str, logger: logging.Logger
):
    async with ApiClient() as api_client:
        apps = AppsV1Api(api_client)
        deployment: V1Deployment = await apps.read_namespaced_deployment(
            namespace=namespace, name=f"grand-central-{name}"
        )
        api_container: V1Container = next(
            container
            for container in deployment.spec.template.spec.containers
            if container.name == "grand-central-api"
        )
        api_container.image = image

        await apps.patch_namespaced_deployment(
            namespace=namespace, name=f"grand-central-{name}", body=deployment
        )


class CreateGrandCentralBackendSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_create_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        spec: kopf.Spec,
        owner_references: Optional[List[V1OwnerReference]],
        image_pull_secrets: Optional[List[V1LocalObjectReference]],
        grand_central_labels: LabelType,
        grand_central_hostname: str,
        logger: logging.Logger,
        **_kwargs: Any,
    ):
        await create_grand_central_backend(
            owner_references,
            namespace,
            name,
            spec,
            image_pull_secrets,
            grand_central_labels,
            grand_central_hostname,
            logger,
        )
