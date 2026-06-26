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
    ApiException,
    CoreV1Api,
    CustomObjectsApi,
    V1OwnerReference,
)

from crate.operator.constants import Port
from crate.operator.create import (
    _lb_annotations_to_add,
    _lb_annotations_to_remove,
    get_owner_references,
)
from crate.operator.grand_central import (
    create_grand_central_exposure,
    delete_grand_central_ingress,
    delete_grand_central_traefik_resources,
    grand_central_uses_traefik,
    read_grand_central_deployment,
)
from crate.operator.utils import crate
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.kubeapi import call_kubeapi, get_cratedb_resource


async def create_traefik_resources(
    owner_references: Optional[List[V1OwnerReference]],
    namespace: str,
    name: str,
    dns_record: Optional[str],
    source_ranges: Optional[List[str]],
    http_port: int,
    postgres_port: int,
    logger: logging.Logger,
) -> None:
    """
    Create MiddlewareTCP and IngressRouteTCP resources for Traefik exposure.

    If source_ranges is non‑empty, a MiddlewareTCP with IP allowlist is created
    and referenced in the IngressRouteTCP routes. Otherwise, the IngressRouteTCP
    routes are created without any middleware (no IP restriction).

    :param owner_references: Owner references to set on the created resources.
    :param namespace: Kubernetes namespace where the resources will be created.
    :param name: Name of the CrateDB cluster (used in resource names).
    :param dns_record: External DNS hostname.
    :param source_ranges: List of CIDR ranges for IP allowlist.
    :param http_port: Port number for HTTP traffic (default 4200).
    :param postgres_port: Port number for PostgreSQL traffic (default 5432).
    :param logger: Logger for operation tracking.
    """
    if not dns_record:
        raise kopf.PermanentError("externalDNS is required when exposure=traefik")

    async with GlobalApiClient() as api_client:
        custom_api = CustomObjectsApi(api_client)

        owner_refs_serialized = (
            [api_client.sanitize_for_serialization(owner) for owner in owner_references]
            if owner_references
            else []
        )

        has_ip_restriction = source_ranges and len(source_ranges) > 0
        middleware_name = None
        resource_labels = {
            "app.kubernetes.io/component": "cratedb",
            "app.kubernetes.io/managed-by": "crate-operator",
            "app.kubernetes.io/name": name,
            "app.kubernetes.io/part-of": "cratedb",
        }

        # Create MiddlewareTCP only if IP restriction is desired
        if has_ip_restriction:
            middleware_name = f"cratedb-allow-{name}"
            middleware_body = {
                "apiVersion": "traefik.io/v1alpha1",
                "kind": "MiddlewareTCP",
                "metadata": {
                    "name": middleware_name,
                    "namespace": namespace,
                    "labels": resource_labels,
                    "ownerReferences": owner_refs_serialized,
                },
                "spec": {"ipAllowList": {"sourceRange": source_ranges}},
            }
            await call_kubeapi(
                custom_api.create_namespaced_custom_object,
                logger,
                continue_on_conflict=True,
                group="traefik.io",
                version="v1alpha1",
                namespace=namespace,
                plural="middlewaretcps",
                body=middleware_body,
            )
        else:
            logger.info("No IP restriction – skipping MiddlewareTCP creation")

        def build_route(port: int) -> dict:
            route = {
                "match": f"HostSNI(`{dns_record}`)",
                "services": [{"name": f"crate-{name}", "port": port}],
            }
            if has_ip_restriction:
                route["middlewares"] = [{"name": middleware_name}]
            return route

        pg_ingress_body = {
            "apiVersion": "traefik.io/v1alpha1",
            "kind": "IngressRouteTCP",
            "metadata": {
                "name": f"crate-pg-{name}",
                "namespace": namespace,
                "labels": resource_labels,
                "ownerReferences": owner_refs_serialized,
            },
            "spec": {
                "entryPoints": ["postgres"],
                "routes": [build_route(postgres_port)],
                "tls": {"passthrough": True},
            },
        }
        await call_kubeapi(
            custom_api.create_namespaced_custom_object,
            logger,
            continue_on_conflict=True,
            group="traefik.io",
            version="v1alpha1",
            namespace=namespace,
            plural="ingressroutetcps",
            body=pg_ingress_body,
        )

        http_ingress_body = {
            "apiVersion": "traefik.io/v1alpha1",
            "kind": "IngressRouteTCP",
            "metadata": {
                "name": f"crate-http-{name}",
                "namespace": namespace,
                "labels": resource_labels,
                "ownerReferences": owner_refs_serialized,
            },
            "spec": {
                "entryPoints": ["web-4200"],
                "routes": [build_route(http_port)],
                "tls": {"passthrough": True},
            },
        }
        await call_kubeapi(
            custom_api.create_namespaced_custom_object,
            logger,
            continue_on_conflict=True,
            group="traefik.io",
            version="v1alpha1",
            namespace=namespace,
            plural="ingressroutetcps",
            body=http_ingress_body,
        )


async def delete_traefik_resources(
    namespace: str,
    name: str,
) -> None:
    """
    Delete all Traefik resources owned by this CrateDB cluster.

    Deletes the MiddlewareTCP and both IngressRouteTCP resources.

    :param namespace: Kubernetes namespace where the resources reside.
    :param name: Name of the CrateDB cluster.
    :param logger: Logger for operation tracking.
    """
    async with GlobalApiClient() as api_client:
        custom_api = CustomObjectsApi(api_client)
        # Delete MiddlewareTCP
        try:
            await custom_api.delete_namespaced_custom_object(
                group="traefik.io",
                version="v1alpha1",
                namespace=namespace,
                plural="middlewaretcps",
                name=f"cratedb-allow-{name}",
            )
        except ApiException as e:
            if e.status != 404:
                raise
        # Delete IngressRouteTCP for PG
        try:
            await custom_api.delete_namespaced_custom_object(
                group="traefik.io",
                version="v1alpha1",
                namespace=namespace,
                plural="ingressroutetcps",
                name=f"crate-pg-{name}",
            )
        except ApiException as e:
            if e.status != 404:
                raise
        # Delete IngressRouteTCP for HTTP
        try:
            await custom_api.delete_namespaced_custom_object(
                group="traefik.io",
                version="v1alpha1",
                namespace=namespace,
                plural="ingressroutetcps",
                name=f"crate-http-{name}",
            )
        except ApiException as e:
            if e.status != 404:
                raise


async def delete_service(
    core: CoreV1Api,
    namespace: str,
    name: str,
) -> None:
    """
    Delete the main data service (crate-<name>).

    :param core: Kubernetes CoreV1Api client.
    :param namespace: Kubernetes namespace where the service resides.
    :param name: Name of the CrateDB cluster.
    :param logger: Logger for operation tracking.
    """
    try:
        await core.delete_namespaced_service(
            name=f"crate-{name}",
            namespace=namespace,
        )
    except ApiException as e:
        if e.status != 404:
            raise


async def delete_ingress_route_tcps(
    namespace: str, name: str, logger: logging.Logger
) -> None:
    """
    Delete only the IngressRouteTCP resources (keep middleware if any).

    This is used when updating CIDRs from empty to non‑empty, to recreate
    the ingress routes with the new middleware reference.

    :param namespace: Kubernetes namespace where the resources reside.
    :param name: Name of the CrateDB cluster.
    :param logger: Logger for operation tracking.
    """
    async with GlobalApiClient() as api_client:
        custom_api = CustomObjectsApi(api_client)
        for suffix in ["pg", "http"]:
            ingress_name = f"crate-{suffix}-{name}"
            try:
                await custom_api.delete_namespaced_custom_object(
                    group="traefik.io",
                    version="v1alpha1",
                    namespace=namespace,
                    plural="ingressroutetcps",
                    name=ingress_name,
                )
                logger.info(f"Deleted IngressRouteTCP {ingress_name}")
            except ApiException as e:
                if e.status != 404:
                    raise


async def delete_middleware_tcp(
    namespace: str, name: str, logger: logging.Logger
) -> None:
    """
    Delete only the MiddlewareTCP resource.

    :param namespace: Kubernetes namespace where the resource resides.
    :param name: Name of the CrateDB cluster.
    :param logger: Logger for operation tracking.
    """
    async with GlobalApiClient() as api_client:
        custom_api = CustomObjectsApi(api_client)
        middleware_name = f"cratedb-allow-{name}"
        try:
            await custom_api.delete_namespaced_custom_object(
                group="traefik.io",
                version="v1alpha1",
                namespace=namespace,
                plural="middlewaretcps",
                name=middleware_name,
            )
            logger.info(f"Deleted MiddlewareTCP {middleware_name}")
        except ApiException as e:
            if e.status != 404:
                raise


async def _remove_middleware_from_ingress_routes(
    namespace: str, name: str, logger: logging.Logger, custom_api: CustomObjectsApi
) -> None:
    """
    Patch both IngressRouteTCP resources to remove the 'middlewares' field.

    This is called when allowedCIDRs becomes empty and a middleware existed,
    to remove the reference from the ingress routes.

    :param namespace: Kubernetes namespace where the resources reside.
    :param name: Name of the CrateDB cluster.
    :param logger: Logger for operation tracking.
    :param custom_api: Instance of CustomObjectsApi to perform the patch.
    """
    ingress_names = [f"crate-pg-{name}", f"crate-http-{name}"]
    for ingress_name in ingress_names:
        try:
            irt = await custom_api.get_namespaced_custom_object(
                group="traefik.io",
                version="v1alpha1",
                namespace=namespace,
                plural="ingressroutetcps",
                name=ingress_name,
            )
            # Remove middlewares from each route
            routes = irt.get("spec", {}).get("routes", [])
            modified = False
            for route in routes:
                if "middlewares" in route:
                    del route["middlewares"]
                    modified = True
            if modified:
                patch_body = [
                    {"op": "replace", "path": "/spec/routes", "value": routes}
                ]
                await call_kubeapi(
                    custom_api.patch_namespaced_custom_object,
                    logger,
                    group="traefik.io",
                    version="v1alpha1",
                    namespace=namespace,
                    plural="ingressroutetcps",
                    name=ingress_name,
                    body=patch_body,
                    _content_type="application/json-patch+json",
                )
                logger.info(
                    f"Removed middleware reference from IngressRouteTCP {ingress_name}"
                )
        except ApiException as e:
            if e.status != 404:
                raise


async def update_traefik_ip_restriction(
    namespace: str,
    name: str,
    new_cidrs: List[str],
    logger: logging.Logger,
) -> None:
    """
    Create, patch, or delete Traefik MiddlewareTCP based on new CIDRs.
    Also ensures IngressRouteTCPs reference the middleware if it exists.

    Handles four cases:
    - non‑empty -> non‑empty: patch existing middleware.
    - non‑empty -> empty: delete middleware and remove reference from ingress routes.
    - empty -> non‑empty: create middleware and recreate ingress routes to reference it.
    - empty -> empty: do nothing.

    :param namespace: Kubernetes namespace where the resources reside.
    :param name: Name of the CrateDB cluster.
    :param new_cidrs: New list of CIDR ranges (may be empty).
    :param logger: Logger for operation tracking.
    """
    async with GlobalApiClient() as api_client:
        custom_api = CustomObjectsApi(api_client)
        middleware_name = f"cratedb-allow-{name}"
        need_middleware = bool(new_cidrs)

        middleware_exists = False
        try:
            await custom_api.get_namespaced_custom_object(
                group="traefik.io",
                version="v1alpha1",
                namespace=namespace,
                plural="middlewaretcps",
                name=middleware_name,
            )
            middleware_exists = True
        except ApiException as e:
            if e.status != 404:
                raise

        if need_middleware and not middleware_exists:
            # Create middleware and recreate ingress routes to reference it
            logger.info(
                f"Creating MiddlewareTCP {middleware_name} with CIDRs {new_cidrs}"
            )
            cratedb = await get_cratedb_resource(namespace, name)
            spec = cratedb["spec"]
            dns_record = spec.get("cluster", {}).get("externalDNS")
            if not dns_record:
                logger.error("Cannot create middleware: externalDNS missing")
                return
            ports_spec = spec.get("ports", {})
            http_port = ports_spec.get("http", Port.HTTP.value)
            postgres_port = ports_spec.get("postgres", Port.POSTGRES.value)
            owner_references = get_owner_references(name, cratedb["metadata"])

            # Recreate both ingress routes (they will include the middleware)
            await delete_ingress_route_tcps(namespace, name, logger)
            await create_traefik_resources(
                owner_references,
                namespace,
                name,
                dns_record,
                new_cidrs,
                http_port,
                postgres_port,
                logger,
            )
        elif need_middleware and middleware_exists:
            # Patch existing middleware
            logger.info(
                f"Patching MiddlewareTCP {middleware_name} with new CIDRs {new_cidrs}"
            )
            patch_body = {"spec": {"ipAllowList": {"sourceRange": new_cidrs}}}
            await call_kubeapi(
                custom_api.patch_namespaced_custom_object,
                logger,
                group="traefik.io",
                version="v1alpha1",
                namespace=namespace,
                plural="middlewaretcps",
                name=middleware_name,
                body=patch_body,
                _content_type="application/merge-patch+json",
            )
        elif not need_middleware and middleware_exists:
            # Delete middleware and remove reference from ingress routes
            logger.info(f"Deleting MiddlewareTCP {middleware_name} (no CIDRs)")
            await delete_middleware_tcp(namespace, name, logger)
            await _remove_middleware_from_ingress_routes(
                namespace, name, logger, custom_api
            )
        else:
            logger.info("No change: CIDRs empty and no middleware exists")


async def patch_service_exposure(
    core: CoreV1Api,
    namespace: str,
    name: str,
    new_exposure: str,
    source_ranges: Optional[List[str]],
    dns_record: Optional[str],
    additional_annotations: Optional[dict],
    logger: logging.Logger,
) -> None:
    """
    Patch the existing crate-<name> service in-place to switch between
    LoadBalancer and ClusterIP.
    """
    service_name = f"crate-{name}"
    use_traefik = new_exposure == "traefik"

    new_annotations: dict[str, Optional[str]] = {}
    if use_traefik:
        # When switching to traefik, explicitly null out all LB-specific annotations
        # so they don't linger on the ClusterIP service.
        new_annotations.update(_lb_annotations_to_remove())
    else:
        new_annotations.update(_lb_annotations_to_add(dns_record))

    if additional_annotations:
        new_annotations.update(additional_annotations)

    # When switching away from LoadBalancer, explicitly null out LB-specific
    # fields - Kubernetes requires them to be absent for ClusterIP.
    spec_patch: dict[str, Any] = {}
    if use_traefik:
        spec_patch = {
            "type": "ClusterIP",
            "externalTrafficPolicy": None,
            "loadBalancerSourceRanges": None,
        }
    else:
        spec_patch = {
            "type": "LoadBalancer",
            "externalTrafficPolicy": "Local",
            "loadBalancerSourceRanges": source_ranges if source_ranges else None,
        }

    patch_body = {
        "metadata": {"annotations": new_annotations},
        "spec": spec_patch,
    }

    logger.info(f"Patching service {service_name} to exposure={new_exposure}")
    await core.patch_namespaced_service(
        name=service_name,
        namespace=namespace,
        body=patch_body,
    )


async def migrate_grand_central_exposure(
    namespace: str,
    name: str,
    spec: kopf.Spec,
    meta: kopf.Meta,
    old_use_traefik: bool,
    new_use_traefik: bool,
    logger: logging.Logger,
) -> None:
    """
    Migrate grand-central routing resources from one exposure to the other.

    Creates the routing resources for ``new_use_traefik`` and deletes the
    resources belonging to the old exposure. Does nothing if grand-central is
    not deployed for this cluster.

    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param spec: The ``spec`` section of the CrateDB custom resource.
    :param meta: The ``metadata`` section of the CrateDB custom resource.
    :param old_use_traefik: Whether grand-central was previously on Traefik.
    :param new_use_traefik: Whether grand-central should now be on Traefik.
    :param logger: Logger for operation tracking.
    """
    gc_deployment = await read_grand_central_deployment(namespace, name)
    if not gc_deployment:
        logger.info("Grand-central not deployed; skipping GC exposure migration")
        return

    await create_grand_central_exposure(
        namespace=namespace,
        name=name,
        spec=spec,
        meta=meta,
        logger=logger,
        use_traefik=new_use_traefik,
    )

    if old_use_traefik:
        await delete_grand_central_traefik_resources(namespace, name, logger)
    else:
        await delete_grand_central_ingress(namespace, name, logger)


class CreateTraefikResourcesSubHandler(StateBasedSubHandler):
    """
    Creates Traefik resources during cluster creation.
    """

    @crate.on.error(error_handler=crate.send_create_failed_notification)
    async def handle(
        self,
        namespace: str,
        name: str,
        owner_references: Optional[List[V1OwnerReference]],
        dns_record: Optional[str],
        source_ranges: Optional[List[str]],
        http_port: int,
        postgres_port: int,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        await create_traefik_resources(
            owner_references,
            namespace,
            name,
            dns_record,
            source_ranges,
            http_port,
            postgres_port,
            logger,
        )


class ChangeExposureSubHandler(StateBasedSubHandler):
    """
    Handles changes to spec.cluster.exposure between 'loadbalancer' and 'traefik'.
    Deletes old resources and creates new ones accordingly.
    """

    @crate.on.error(error_handler=crate.send_update_failed_notification)
    async def handle(
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        new_exposure = body["spec"]["cluster"].get("exposure", "loadbalancer")
        old_exposure = old["spec"]["cluster"].get("exposure", "loadbalancer")

        if old_exposure == new_exposure:
            logger.info(f"Exposure unchanged: {new_exposure}")
            return

        logger.info(f"Changing exposure from {old_exposure} to {new_exposure}")

        spec = body["spec"]
        ports_spec = spec.get("ports", {})
        http_port = ports_spec.get("http", Port.HTTP.value)
        postgres_port = ports_spec.get("postgres", Port.POSTGRES.value)
        dns_record = spec.get("cluster", {}).get("externalDNS")
        source_ranges = spec["cluster"].get("allowedCIDRs", None)
        additional_annotations = (
            spec.get("cluster", {}).get("service", {}).get("annotations", {})
        )

        owner_references = get_owner_references(name, body["metadata"])

        async with GlobalApiClient() as api_client:
            core = CoreV1Api(api_client)

            await patch_service_exposure(
                core,
                namespace,
                name,
                new_exposure,
                source_ranges,
                dns_record,
                additional_annotations,
                logger,
            )

            if old_exposure == "traefik":
                await delete_traefik_resources(namespace, name)

        if new_exposure == "traefik":
            await create_traefik_resources(
                owner_references,
                namespace,
                name,
                dns_record,
                source_ranges,
                http_port,
                postgres_port,
                logger,
            )

        gc_exposure_explicit = bool((spec.get("grandCentral") or {}).get("exposure"))
        if gc_exposure_explicit:
            logger.info(
                "Grand-central has its own exposure setting; "
                "not migrating it on cluster exposure change"
            )
        else:
            await migrate_grand_central_exposure(
                namespace,
                name,
                spec,
                meta=body["metadata"],
                old_use_traefik=(old_exposure == "traefik"),
                new_use_traefik=(new_exposure == "traefik"),
                logger=logger,
            )


class ChangeGrandCentralExposureSubHandler(StateBasedSubHandler):
    """
    Handles changes to ``spec.grandCentral.exposure`` between 'nginx' and
    'traefik', migrating only the grand-central routing resources and leaving
    the CrateDB service untouched.
    """

    @crate.on.error(error_handler=crate.send_update_failed_notification)
    async def handle(
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        old_use_traefik = grand_central_uses_traefik(old["spec"])
        new_use_traefik = grand_central_uses_traefik(body["spec"])

        if old_use_traefik == new_use_traefik:
            logger.info("Grand-central exposure unchanged")
            return

        logger.info(
            "Changing grand-central exposure "
            f"(traefik={old_use_traefik} -> traefik={new_use_traefik})"
        )

        await migrate_grand_central_exposure(
            namespace,
            name,
            body["spec"],
            meta=body["metadata"],
            old_use_traefik=old_use_traefik,
            new_use_traefik=new_use_traefik,
            logger=logger,
        )
