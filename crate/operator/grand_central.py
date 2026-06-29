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
    ApiException,
    AppsV1Api,
    CoreV1Api,
    CustomObjectsApi,
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
    GC_USER_SECRET_NAME,
    GC_USERNAME,
    GRAND_CENTRAL_BACKEND_API_PORT,
    GRAND_CENTRAL_INIT_CONTAINER,
    GRAND_CENTRAL_PROMETHEUS_PORT,
    GRAND_CENTRAL_RESOURCE_PREFIX,
    LABEL_NAME,
    SHARED_NODE_SELECTOR_KEY,
    SHARED_NODE_SELECTOR_VALUE,
    SHARED_NODE_TOLERATION_EFFECT,
    SHARED_NODE_TOLERATION_KEY,
    SHARED_NODE_TOLERATION_VALUE,
)
from crate.operator.create import (
    build_cratedb_labels,
    get_gc_user_secret,
    get_owner_references,
)
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.kubeapi import call_kubeapi
from crate.operator.utils.secrets import get_image_pull_secrets
from crate.operator.utils.typing import LabelType

# Gateway API / Traefik CRD constants
_GATEWAY_API_GROUP = "gateway.networking.k8s.io"
_GATEWAY_API_VERSION = "v1"
_HTTPROUTE_PLURAL = "httproutes"

_GC_GATEWAY_NAME: str = "traefik"
_GC_GATEWAY_NAMESPACE: str = "traefik"
_GC_GATEWAY_SECTION_NAME: str = "websecure"

_TRAEFIK_GROUP = "traefik.io"
_TRAEFIK_VERSION = "v1alpha1"
_MIDDLEWARE_PLURAL = "middlewares"

_MIDDLEWARE_COMPRESS_JS = "grand-central-compress-js"
_MIDDLEWARE_BUFFERING = "grand-central-buffering"
_MIDDLEWARE_IP_ALLOWLIST = "grand-central-ip-allowlist"
_MIDDLEWARE_CORS = "grand-central-cors"
_OPEN_CIDR = ["0.0.0.0/0", "::/0"]


def get_grand_central_labels(name: str, meta: kopf.Meta) -> Dict[str, Any]:
    return build_cratedb_labels(
        name=name,
        meta=meta,
        component="grand-central",
        label_name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
    )


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
                    key="password", name=GC_USER_SECRET_NAME.format(name=name)
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
                            name=GRAND_CENTRAL_INIT_CONTAINER,
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
                                    "cpu": "500m",
                                    "memory": "256Mi",
                                },
                                requests={"cpu": "64m", "memory": "256Mi"},
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


def _build_response_header_filter() -> Dict[str, Any]:
    """
    Build a Gateway API ResponseHeaderModifier filter that sets security
    headers on all grand-central HTTPRoute rules.
    """
    return {
        "type": "ResponseHeaderModifier",
        "responseHeaderModifier": {
            "set": [
                {"name": "X-Frame-Options", "value": "DENY"},
                {"name": "X-Content-Type-Options", "value": "nosniff"},
                {
                    "name": "Referrer-Policy",
                    "value": "strict-origin-when-cross-origin",
                },
            ]
        },
    }


def _middleware_ref(name: str) -> Dict[str, Any]:
    """
    Build a Gateway API ExtensionRef filter pointing to a Traefik Middleware.

    :param name: The name of the Traefik Middleware resource to reference.
    """
    return {
        "type": "ExtensionRef",
        "extensionRef": {
            "group": _TRAEFIK_GROUP,
            "kind": "Middleware",
            "name": name,
        },
    }


def get_grand_central_httproute(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    hostname: str,
    spec: kopf.Spec,
) -> Dict[str, Any]:
    """
    Build the HTTPRoute manifest for grand-central.

    Creates two rules: one matching ``/api`` (with JS compression, buffering,
    and IP allowlist middlewares) and one matching ``/socket.io`` (with
    buffering and IP allowlist only). Both rules set security and CORS response
    headers via a ResponseHeaderModifier filter.

    :param owner_references: Owner references to set on the resource.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param labels: Kubernetes labels to apply to the resource.
    :param hostname: The external hostname the HTTPRoute should match.
    :param spec: The ``spec`` section of the CrateDB custom resource.
    """
    header_filter = _build_response_header_filter()
    service_name = f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}"

    raw_owner_refs = []
    if owner_references:
        for ref in owner_references:
            raw_owner_refs.append(
                {
                    "apiVersion": ref.api_version,
                    "blockOwnerDeletion": ref.block_owner_deletion,
                    "controller": ref.controller,
                    "kind": ref.kind,
                    "name": ref.name,
                    "uid": ref.uid,
                }
            )

    return {
        "apiVersion": f"{_GATEWAY_API_GROUP}/{_GATEWAY_API_VERSION}",
        "kind": "HTTPRoute",
        "metadata": {
            "name": service_name,
            "labels": labels,
            "ownerReferences": raw_owner_refs,
        },
        "spec": {
            "hostnames": [hostname],
            "parentRefs": [
                {
                    "name": _GC_GATEWAY_NAME,
                    "namespace": _GC_GATEWAY_NAMESPACE,
                    "sectionName": _GC_GATEWAY_SECTION_NAME,
                }
            ],
            "rules": [
                {
                    "matches": [{"path": {"type": "PathPrefix", "value": "/api"}}],
                    "filters": [
                        # 1. GUARD: reject disallowed IPs before doing any other work
                        _middleware_ref(_MIDDLEWARE_IP_ALLOWLIST),
                        # 2. SANITIZE/TRANSFORM: security headers, CORS, buffering
                        header_filter,
                        _middleware_ref(_MIDDLEWARE_CORS),
                        _middleware_ref(_MIDDLEWARE_BUFFERING),
                        # 3. OPTIMIZE: compression last
                        _middleware_ref(_MIDDLEWARE_COMPRESS_JS),
                    ],
                    "backendRefs": [
                        {"name": service_name, "port": GRAND_CENTRAL_BACKEND_API_PORT}
                    ],
                },
                {
                    "matches": [
                        {"path": {"type": "PathPrefix", "value": "/socket.io"}}
                    ],
                    "filters": [
                        # 1. GUARD: reject disallowed IPs before doing any other work
                        _middleware_ref(_MIDDLEWARE_IP_ALLOWLIST),
                        # 2. SANITIZE/TRANSFORM: security headers, CORS, buffering
                        header_filter,
                        _middleware_ref(_MIDDLEWARE_CORS),
                        _middleware_ref(_MIDDLEWARE_BUFFERING),
                    ],
                    "backendRefs": [
                        {"name": service_name, "port": GRAND_CENTRAL_BACKEND_API_PORT}
                    ],
                },
            ],
        },
    }


def _build_middleware_base(
    name: str,
    middleware_name: str,
    labels: LabelType,
    owner_references: Optional[List[V1OwnerReference]],
) -> Dict[str, Any]:
    """
    Build the common metadata scaffold for a Traefik Middleware resource.

    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param middleware_name: The Kubernetes resource name for this Middleware.
    :param labels: Kubernetes labels to apply to the resource.
    :param owner_references: Owner references to set on the resource.
    """
    raw_owner_refs = []
    if owner_references:
        for ref in owner_references:
            raw_owner_refs.append(
                {
                    "apiVersion": ref.api_version,
                    "blockOwnerDeletion": ref.block_owner_deletion,
                    "controller": ref.controller,
                    "kind": ref.kind,
                    "name": ref.name,
                    "uid": ref.uid,
                }
            )
    return {
        "apiVersion": f"{_TRAEFIK_GROUP}/{_TRAEFIK_VERSION}",
        "kind": "Middleware",
        "metadata": {
            "name": middleware_name,
            "labels": labels,
            "ownerReferences": raw_owner_refs,
        },
    }


def get_grand_central_middleware_compress_js(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
) -> Dict[str, Any]:
    """
    Build the ``grand-central-compress-js`` Traefik Middleware manifest.

    Configures Traefik to gzip-compress ``application/javascript`` and
    ``text/javascript`` responses, replacing the nginx ``gzip`` configuration
    snippet from the legacy Ingress.

    :param owner_references: Owner references to set on the resource.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param labels: Kubernetes labels to apply to the resource.
    """
    body = _build_middleware_base(
        name, _MIDDLEWARE_COMPRESS_JS, labels, owner_references
    )
    body["spec"] = {
        "compress": {
            "includedContentTypes": [
                "application/javascript",
                "text/javascript",
            ]
        }
    }
    return body


def get_grand_central_middleware_buffering(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
) -> Dict[str, Any]:
    """
    Build the ``grand-central-buffering`` Traefik Middleware manifest.

    Configures request/response buffering with a 1 GiB maximum request body,
    replacing the nginx ``proxy-body-size: 1G`` annotation from the legacy
    Ingress.

    :param owner_references: Owner references to set on the resource.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param labels: Kubernetes labels to apply to the resource.
    """
    body = _build_middleware_base(name, _MIDDLEWARE_BUFFERING, labels, owner_references)
    body["spec"] = {
        "buffering": {
            "maxRequestBodyBytes": 1_073_741_824,
            "memRequestBodyBytes": 2_097_152,
        }
    }
    return body


def get_grand_central_middleware_ip_allowlist(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    cidrs: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Build the ``grand-central-ip-allowlist`` Traefik Middleware manifest.

    Restricts access to the listed CIDR ranges, replacing the nginx
    ``whitelist-source-range`` annotation from the legacy Ingress. When
    ``cidrs`` is empty or ``None``, all traffic is allowed (``0.0.0.0/0``
    and ``::/0``).

    :param owner_references: Owner references to set on the resource.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param labels: Kubernetes labels to apply to the resource.
    :param cidrs: Optional list of CIDR ranges to allow. Defaults to open
        access when not provided.
    """
    body = _build_middleware_base(
        name, _MIDDLEWARE_IP_ALLOWLIST, labels, owner_references
    )
    body["spec"] = {
        "ipAllowList": {
            "sourceRange": cidrs if cidrs else _OPEN_CIDR,
        }
    }
    return body


def get_grand_central_middleware_cors(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    spec: kopf.Spec,
) -> Dict[str, Any]:
    """
    Build the ``grand-central-cors`` Traefik Middleware manifest.

    The CrateDB setting accepts a comma-separated list of origins; these are
    split into individual entries. Falls back to ``["*"]`` when the setting
    is absent.

    :param owner_references: Owner references to set on the resource.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param labels: Kubernetes labels to apply to the resource.
    :param spec: The ``spec`` section of the CrateDB custom resource, used to
        read ``cluster.settings.http.cors.allow-origin``.
    """
    raw_origin = (
        spec["cluster"].get("settings", {}).get("http.cors.allow-origin") or "*"
    )
    origin_list = [o.strip() for o in raw_origin.split(",") if o.strip()]

    body = _build_middleware_base(name, _MIDDLEWARE_CORS, labels, owner_references)
    body["spec"] = {
        "headers": {
            "accessControlAllowOriginList": origin_list,
            "accessControlAllowCredentials": True,
            "accessControlAllowMethods": [
                "GET",
                "POST",
                "PUT",
                "PATCH",
                "OPTIONS",
                "DELETE",
            ],
            "accessControlAllowHeaders": ["Content-Type", "Authorization"],
            "accessControlMaxAge": 7200,
        }
    }
    return body


async def update_grand_central_ip_allowlist(
    namespace: str,
    name: str,
    cidrs: List[str],
    logger: logging.Logger,
) -> None:
    """
    Patch the ``grand-central-ip-allowlist`` Middleware in place with a new
    set of CIDR ranges.

    When ``cidrs`` is empty, the middleware is set to allow all traffic
    (``0.0.0.0/0`` and ``::/0``) rather than blocking everything.

    :param namespace: The Kubernetes namespace where the Middleware resides.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param cidrs: New list of CIDR ranges. Pass an empty list to remove all
        restrictions.
    :param logger: Logger for operation tracking.
    """
    source_range = cidrs if cidrs else _OPEN_CIDR
    async with GlobalApiClient() as api_client:
        custom = CustomObjectsApi(api_client)
        await custom.patch_namespaced_custom_object(
            group=_TRAEFIK_GROUP,
            version=_TRAEFIK_VERSION,
            namespace=namespace,
            plural=_MIDDLEWARE_PLURAL,
            name=_MIDDLEWARE_IP_ALLOWLIST,
            body={"spec": {"ipAllowList": {"sourceRange": source_range}}},
            _content_type="application/merge-patch+json",
        )


async def delete_grand_central_ingress(
    namespace: str, name: str, logger: logging.Logger
) -> None:
    """
    Delete the nginx Ingress resource for grand-central.

    A 404 response is treated as success so that the function is safe to call
    even when the Ingress has already been removed.

    :param namespace: The Kubernetes namespace where the Ingress resides.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param logger: Logger for operation tracking.
    """
    async with GlobalApiClient() as api_client:
        networking = NetworkingV1Api(api_client)
        try:
            await networking.delete_namespaced_ingress(
                name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
                namespace=namespace,
            )
            logger.info(f"Deleted Ingress {GRAND_CENTRAL_RESOURCE_PREFIX}-{name}")
        except ApiException as e:
            if e.status == 404:
                logger.info(
                    f"Ingress {GRAND_CENTRAL_RESOURCE_PREFIX}-{name} not found "
                    "during deletion - already absent, continuing."
                )
            else:
                raise


async def delete_grand_central_traefik_resources(
    namespace: str, name: str, logger: logging.Logger
) -> None:
    """
    Delete the HTTPRoute and all three Traefik Middlewares for grand-central.

    Resources are deleted in order: HTTPRoute first, then ``grand-central-cors``,
    ``grand-central-compress-js``, ``grand-central-buffering``, and
    ``grand-central-ip-allowlist``. A 404 response for any resource is
    treated as success.

    :param namespace: The Kubernetes namespace where the resources reside.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param logger: Logger for operation tracking.
    """
    async with GlobalApiClient() as api_client:
        custom = CustomObjectsApi(api_client)

        # HTTPRoute
        try:
            await custom.delete_namespaced_custom_object(
                group=_GATEWAY_API_GROUP,
                version=_GATEWAY_API_VERSION,
                namespace=namespace,
                plural=_HTTPROUTE_PLURAL,
                name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
            )
        except ApiException as e:
            if e.status == 404:
                logger.info(
                    f"HTTPRoute {GRAND_CENTRAL_RESOURCE_PREFIX}-{name} not found "
                    "during deletion - already absent, continuing."
                )
            else:
                raise

        # Middlewares
        for mw_name in (
            _MIDDLEWARE_CORS,
            _MIDDLEWARE_COMPRESS_JS,
            _MIDDLEWARE_BUFFERING,
            _MIDDLEWARE_IP_ALLOWLIST,
        ):
            try:
                await custom.delete_namespaced_custom_object(
                    group=_TRAEFIK_GROUP,
                    version=_TRAEFIK_VERSION,
                    namespace=namespace,
                    plural=_MIDDLEWARE_PLURAL,
                    name=mw_name,
                )
            except ApiException as e:
                if e.status == 404:
                    logger.info(
                        f"Middleware {mw_name} not found "
                        "during deletion - already absent, continuing."
                    )
                else:
                    raise
        logger.info(f"Deleted GC HTTPRoute + Middlewares for {name}")


async def read_grand_central_httproute(
    namespace: str, name: str
) -> Optional[Dict[str, Any]]:
    """
    Return the HTTPRoute object for grand-central, or ``None`` if it does not
    exist or cannot be read.

    :param namespace: The Kubernetes namespace to look up the HTTPRoute in.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    """
    async with GlobalApiClient() as api_client:
        custom = CustomObjectsApi(api_client)
        try:
            return await custom.get_namespaced_custom_object(
                group=_GATEWAY_API_GROUP,
                version=_GATEWAY_API_VERSION,
                namespace=namespace,
                plural=_HTTPROUTE_PLURAL,
                name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
            )
        except Exception:
            return None


def get_grand_central_exposure(spec: kopf.Spec) -> str:
    """
    Resolve the effective exposure mode for grand-central.

    :param spec: The ``spec`` section of the CrateDB custom resource.
    """
    explicit = (spec.get("grandCentral") or {}).get("exposure")
    if explicit:
        return explicit
    return spec.get("cluster", {}).get("exposure", "loadbalancer")


def grand_central_uses_traefik(spec: kopf.Spec) -> bool:
    """
    Return whether grand-central should be exposed via Traefik.

    :param spec: The ``spec`` section of the CrateDB custom resource.
    """
    return get_grand_central_exposure(spec) == "traefik"


async def create_grand_central_exposure(
    namespace: str,
    name: str,
    spec: kopf.Spec,
    meta: kopf.Meta,
    logger: logging.Logger,
    use_traefik: bool = False,
) -> None:
    """
    Create only the routing resources for grand-central (HTTPRoute and
    Traefik Middlewares, or nginx Ingress), without touching the Deployment
    or Service.

    This is used when resuming a suspended cluster or when switching the
    ``spec.cluster.exposure`` field, where the Deployment and Service already
    exist and only the routing layer needs to be (re-)created.

    :param namespace: The Kubernetes namespace to create resources in.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param spec: The ``spec`` section of the CrateDB custom resource.
    :param meta: The ``metadata`` section of the CrateDB custom resource.
    :param logger: Logger for operation tracking.
    :param use_traefik: When ``True``, create an HTTPRoute and Traefik
        Middlewares. When ``False`` (default), create an nginx Ingress.
    """
    owner_references = get_owner_references(name, meta)
    cluster_name = spec["cluster"]["name"]
    external_dns = spec["cluster"]["externalDNS"]
    hostname = external_dns.replace(cluster_name, f"{cluster_name}.gc").rstrip(".")
    labels = get_grand_central_labels(name, meta)
    cidrs = spec["cluster"].get("allowedCIDRs", None)

    async with GlobalApiClient() as api_client:
        if use_traefik:
            custom = CustomObjectsApi(api_client)
            for mw_body in (
                get_grand_central_middleware_cors(owner_references, name, labels, spec),
                get_grand_central_middleware_compress_js(
                    owner_references, name, labels
                ),
                get_grand_central_middleware_buffering(owner_references, name, labels),
                get_grand_central_middleware_ip_allowlist(
                    owner_references, name, labels, cidrs
                ),
            ):
                await call_kubeapi(
                    custom.create_namespaced_custom_object,
                    logger,
                    continue_on_conflict=True,
                    group=_TRAEFIK_GROUP,
                    version=_TRAEFIK_VERSION,
                    namespace=namespace,
                    plural=_MIDDLEWARE_PLURAL,
                    body=mw_body,
                )
            await call_kubeapi(
                custom.create_namespaced_custom_object,
                logger,
                continue_on_conflict=True,
                group=_GATEWAY_API_GROUP,
                version=_GATEWAY_API_VERSION,
                namespace=namespace,
                plural=_HTTPROUTE_PLURAL,
                body=get_grand_central_httproute(
                    owner_references, name, labels, hostname, spec
                ),
            )
        else:
            networking = NetworkingV1Api(api_client)
            await call_kubeapi(
                networking.create_namespaced_ingress,
                logger,
                continue_on_conflict=True,
                namespace=namespace,
                body=get_grand_central_ingress(
                    owner_references, name, labels, hostname, spec, cidrs
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
    spec: kopf.Spec,
    cidrs: Optional[List[str]] = None,
) -> V1Ingress:
    allow_origin = (
        spec["cluster"].get("settings", {}).get("http.cors.allow-origin")
        or "$http_origin"
    )
    annotations = {
        "external-dns.alpha.kubernetes.io/hostname": hostname,
        "nginx.ingress.kubernetes.io/proxy-body-size": "1G",
        "nginx.ingress.kubernetes.io/configuration-snippet": (
            """
            gzip on;
            gzip_types
                application/javascript
                text/javascript;
            more_set_headers "X-XSS-Protection: 1;mode=block"
                            "X-Frame-Options: DENY"
                            "X-Content-Type-Options: nosniff"
                            "Referrer-Policy: strict-origin-when-cross-origin"
                            ;
            """  # noqa
        ),
        "nginx.ingress.kubernetes.io/proxy-buffer-size": "64k",
        "nginx.ingress.kubernetes.io/ssl-redirect": "true",
        "nginx.ingress.kubernetes.io/enable-cors": "true",
        "nginx.ingress.kubernetes.io/cors-allow-credentials": "true",
        "nginx.ingress.kubernetes.io/cors-allow-origin": allow_origin,
        "nginx.ingress.kubernetes.io/cors-allow-methods": (
            "GET,POST,PUT,PATCH,OPTIONS,DELETE"
        ),
        "nginx.ingress.kubernetes.io/cors-allow-headers": (
            "Content-Type,Authorization"
        ),
        "nginx.ingress.kubernetes.io/cors-max-age": "7200",
    }
    if cidrs:
        annotations["nginx.ingress.kubernetes.io/whitelist-source-range"] = ",".join(
            cidrs
        )
    return V1Ingress(
        metadata=V1ObjectMeta(
            name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
            labels=labels,
            owner_references=owner_references,
            annotations=annotations,
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
    use_traefik: bool = False,
) -> None:
    image_pull_secrets = get_image_pull_secrets()
    owner_references = get_owner_references(name, meta)
    labels = get_grand_central_labels(name, meta)

    async with GlobalApiClient() as api_client:
        apps = AppsV1Api(api_client)
        core = CoreV1Api(api_client)

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

    await create_grand_central_exposure(
        namespace=namespace,
        name=name,
        spec=spec,
        meta=meta,
        logger=logger,
        use_traefik=use_traefik,
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
        deployment: V1Deployment = next(
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

        if deployment.spec.template.spec.init_containers:
            init_container: V1Container = next(
                container
                for container in deployment.spec.template.spec.init_containers
                if container.name == GRAND_CENTRAL_INIT_CONTAINER
            )
            init_container.image = image

        await apps.patch_namespaced_deployment(
            namespace=namespace,
            name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
            body=deployment,
        )
