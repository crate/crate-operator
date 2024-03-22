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
from typing import Any, Dict, List, Optional

import kopf
from kubernetes_asyncio.client import (
    AppsV1Api,
    CoreV1Api,
    NetworkingV1Api,
    V1Affinity,
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1DeploymentStrategy,
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
    V1NodeAffinity,
    V1NodeSelector,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
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
    V1Toleration,
)

from crate.operator.bootstrap import bootstrap_gc_admin_user
from crate.operator.config import config
from crate.operator.constants import (
    GC_USERNAME,
    GRAND_CENTRAL_BACKEND_API_PORT,
    GRAND_CENTRAL_PROMETHEUS_PORT,
    GRAND_CENTRAL_RESOURCE_PREFIX,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_PART_OF,
    SHARED_NODE_SELECTOR_KEY,
    SHARED_NODE_SELECTOR_VALUE,
    SHARED_NODE_TOLERATION_EFFECT,
    SHARED_NODE_TOLERATION_KEY,
    SHARED_NODE_TOLERATION_VALUE,
)
from crate.operator.create import get_gc_user_secret, get_owner_references
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.kubeapi import call_kubeapi
from crate.operator.utils.secrets import get_image_pull_secrets
from crate.operator.utils.typing import LabelType


def get_grand_central_labels(name: str, meta: kopf.Meta) -> Dict[str, Any]:
    labels = {
        LABEL_MANAGED_BY: "crate-operator",
        LABEL_NAME: f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
        LABEL_PART_OF: "cratedb",
        LABEL_COMPONENT: "grand-central",
    }
    labels.update(meta.get("labels", {}))

    return labels


def get_grand_central_deployment(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    spec: kopf.Spec,
) -> V1Deployment:
    env = [
        V1EnvVar(name="GRAND_CENTRAL_CRATEDB_USERNAME", value=GC_USERNAME),
        V1EnvVar(
            name="GRAND_CENTRAL_CRATEDB_ADDRESS",
            value=f"https://crate-discovery-{name}:4200",
        ),
        V1EnvVar(
            name="GRAND_CENTRAL_USE_SSL",
            value="true",
        ),
        V1EnvVar(
            name="GRAND_CENTRAL_VERIFY_SSL",
            value="false",
        ),
        V1EnvVar(name="GRAND_CENTRAL_CLUSTER_ID", value=name),
        V1EnvVar(
            name="GRAND_CENTRAL_JWK_ENDPOINT",
            value=spec["grandCentral"]["jwkUrl"],
        ),
        V1EnvVar(
            name="GRAND_CENTRAL_API_URL",
            value=spec["grandCentral"]["apiUrl"],
        ),
        V1EnvVar(
            name="GRAND_CENTRAL_CRATEDB_PASSWORD",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    key="password", name=f"user-gc-{name}"
                ),
            ),
        ),
        V1EnvVar(name="GRAND_CENTRAL_SENTRY_DSN", value=config.GC_SENTRY_DSN),
    ]
    annotations = {
        "prometheus.io/port": str(GRAND_CENTRAL_PROMETHEUS_PORT),
        "prometheus.io/scrape": "true",
    }
    return V1Deployment(
        metadata=V1ObjectMeta(
            name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
            labels=labels,
            owner_references=owner_references,
        ),
        spec=V1DeploymentSpec(
            replicas=1,
            selector=V1LabelSelector(
                match_labels={
                    LABEL_NAME: f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
                }
            ),
            strategy=V1DeploymentStrategy(type="Recreate"),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(
                    annotations=annotations,
                    labels=labels,
                    name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
                ),
                spec=V1PodSpec(
                    init_containers=[
                        V1Container(
                            env=env,
                            image=spec["grandCentral"]["backendImage"],
                            image_pull_policy="IfNotPresent",
                            name="wait-for-crate",
                            command=["./wait-for-cratedb.py"],
                        )
                    ],
                    containers=[
                        V1Container(
                            env=env,
                            image=spec["grandCentral"]["backendImage"],
                            image_pull_policy="IfNotPresent",
                            name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-api",
                            ports=[
                                V1ContainerPort(
                                    container_port=GRAND_CENTRAL_BACKEND_API_PORT,
                                    name="grand-central",
                                ),
                                V1ContainerPort(
                                    container_port=GRAND_CENTRAL_PROMETHEUS_PORT,
                                    name="prometheus",
                                ),
                            ],
                            resources=V1ResourceRequirements(
                                limits={
                                    "cpu": 2,
                                    "memory": "150Mi",
                                },
                                requests={"cpu": "100m", "memory": "150Mi"},
                            ),
                            liveness_probe=V1Probe(
                                http_get=V1HTTPGetAction(
                                    path="/api/health",
                                    port=GRAND_CENTRAL_BACKEND_API_PORT,
                                ),
                                initial_delay_seconds=180,
                                period_seconds=10,
                            ),
                        )
                    ],
                    affinity=V1Affinity(
                        node_affinity=V1NodeAffinity(
                            required_during_scheduling_ignored_during_execution=V1NodeSelector(  # noqa
                                node_selector_terms=[
                                    V1NodeSelectorTerm(
                                        match_expressions=[
                                            V1NodeSelectorRequirement(
                                                key=SHARED_NODE_SELECTOR_KEY,
                                                operator="In",
                                                values=[SHARED_NODE_SELECTOR_VALUE],
                                            )
                                        ]
                                    )
                                ]
                            )
                        )
                    ),
                    tolerations=[
                        V1Toleration(
                            effect=SHARED_NODE_TOLERATION_EFFECT,
                            key=SHARED_NODE_TOLERATION_KEY,
                            operator="Equal",
                            value=SHARED_NODE_TOLERATION_VALUE,
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
            name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
            labels=labels,
            owner_references=owner_references,
        ),
        spec=V1ServiceSpec(
            type="ClusterIP",
            ports=[
                V1ServicePort(
                    name="api",
                    port=GRAND_CENTRAL_BACKEND_API_PORT,
                    target_port=GRAND_CENTRAL_BACKEND_API_PORT,
                ),
                V1ServicePort(
                    name="prometheus",
                    port=GRAND_CENTRAL_PROMETHEUS_PORT,
                    target_port=GRAND_CENTRAL_PROMETHEUS_PORT,
                ),
            ],
            selector={LABEL_NAME: f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}"},
        ),
    )


async def read_grand_central_ingress(namespace: str, name: str) -> Optional[V1Ingress]:
    async with GlobalApiClient() as api_client:
        networking = NetworkingV1Api(api_client)

        ingresses = await networking.list_namespaced_ingress(namespace=namespace)
        return next(
            (
                ing
                for ing in ingresses.items
                if ing.metadata.name == f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}"
            ),
            None,
        )


async def read_grand_central_deployment(
    namespace: str, name: str
) -> Optional[V1Deployment]:
    async with GlobalApiClient() as api_client:
        apps = AppsV1Api(api_client)
        deployments = await apps.list_namespaced_deployment(namespace=namespace)
        return next(
            (
                deploy
                for deploy in deployments.items
                if deploy.metadata.name == f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}"
            ),
            None,
        )


def get_grand_central_ingress(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    hostname: str,
) -> V1Ingress:
    return V1Ingress(
        metadata=V1ObjectMeta(
            name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
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
                                    "Access-Control-Allow-Headers: Content-Type,Authorization"
                                    "Access-Control-Allow-Credentials: true"
                                    "Access-Control-Max-Age: 7200"
                                    "Access-Control-Allow-Methods: GET,POST,PUT,PATCH,OPTIONS,DELETE"
                                    "Referrer-Policy: strict-origin-when-cross-origin"
                                    ;
                    """  # noqa
                ),
                "nginx.ingress.kubernetes.io/proxy-buffer-size": "64k",
                "nginx.ingress.kubernetes.io/ssl-redirect": "true",
            },
        ),
        spec=V1IngressSpec(
            ingress_class_name="nginx",
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
                                            number=GRAND_CENTRAL_BACKEND_API_PORT,
                                        ),
                                        name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
                                    )
                                ),
                            ),
                            V1HTTPIngressPath(
                                path="/socket.io",
                                path_type="ImplementationSpecific",
                                backend=V1IngressBackend(
                                    service=V1IngressServiceBackend(
                                        port=V1ServiceBackendPort(
                                            number=GRAND_CENTRAL_BACKEND_API_PORT,
                                        ),
                                        name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
                                    )
                                ),
                            ),
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
    namespace: str,
    name: str,
    spec: kopf.Spec,
    meta: kopf.Meta,
    logger: logging.Logger,
) -> None:
    image_pull_secrets = get_image_pull_secrets()
    owner_references = get_owner_references(name, meta)
    cluster_name = spec["cluster"]["name"]
    external_dns = spec["cluster"]["externalDNS"]
    hostname = external_dns.replace(cluster_name, f"{cluster_name}.gc").rstrip(".")
    labels = get_grand_central_labels(name, meta)

    async with GlobalApiClient() as api_client:
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


async def create_grand_central_user(
    namespace: str,
    name: str,
    meta: kopf.Meta,
    logger: logging.Logger,
):
    owner_references = get_owner_references(name, meta)
    labels = get_grand_central_labels(name, meta)

    async with GlobalApiClient() as api_client:
        core = CoreV1Api(api_client)

        await call_kubeapi(
            core.create_namespaced_secret,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_gc_user_secret(owner_references, name, labels),
        )

        await bootstrap_gc_admin_user(core, namespace, name)


async def update_grand_central_deployment_image(
    namespace: str, name: str, image: str, logger: logging.Logger
):
    async with GlobalApiClient() as api_client:
        apps = AppsV1Api(api_client)
        # This also runs on creation events, so we need to double check that the
        # deployment exists before attempting to do anything.
        deployments = await apps.list_namespaced_deployment(namespace=namespace)
        deployment = next(
            (
                deploy
                for deploy in deployments.items
                if deploy.metadata.name == f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}"
            ),
            None,
        )
        if not deployment:
            return

        api_container: V1Container = next(
            container
            for container in deployment.spec.template.spec.containers
            if container.name == f"{GRAND_CENTRAL_RESOURCE_PREFIX}-api"
        )
        api_container.image = image

        await apps.patch_namespaced_deployment(
            namespace=namespace,
            name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
            body=deployment,
        )
