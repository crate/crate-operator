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

import kopf
from kopf import DiffItem
from kubernetes_asyncio.client import CoreV1Api, NetworkingV1Api

from crate.operator.constants import GRAND_CENTRAL_RESOURCE_PREFIX
from crate.operator.exposure import update_traefik_ip_restriction
from crate.operator.grand_central import (
    grand_central_uses_traefik,
    read_grand_central_httproute,
    read_grand_central_ingress,
    update_grand_central_ip_allowlist,
)
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.kubeapi import get_cratedb_resource
from crate.operator.utils.notifications import send_operation_progress_notification
from crate.operator.webhooks import WebhookAction, WebhookOperation, WebhookStatus


async def update_service_allowed_cidrs(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    logger: logging.Logger,
):
    change: DiffItem = diff[0]
    new_cidrs = change.new or []
    logger.info(f"Updating source ranges to {change.new}")

    await send_operation_progress_notification(
        namespace=namespace,
        name=name,
        message="Updating IP Network Whitelist.",
        logger=logger,
        status=WebhookStatus.IN_PROGRESS,
        operation=WebhookOperation.UPDATE,
        action=WebhookAction.ALLOWED_CIDR_UPDATE,
    )

    async with GlobalApiClient() as api_client:
        core = CoreV1Api(api_client)
        networking = NetworkingV1Api(api_client)
        cratedb = await get_cratedb_resource(namespace, name)
        exposure = (
            cratedb.get("spec", {}).get("cluster", {}).get("exposure", "loadbalancer")
        )

        # This also runs on creation events, so we want to double check that the service
        # exists before attempting to do anything.
        # Update the main data service if it's a LoadBalancer
        services = await core.list_namespaced_service(namespace=namespace)
        service = next(
            (svc for svc in services.items if svc.metadata.name == f"crate-{name}"),
            None,
        )

        # Only patch loadBalancerSourceRanges if the service is of type LoadBalancer.
        # For ClusterIP this field is forbidden and will cause a 422.
        if service and service.spec.type == "LoadBalancer":
            await core.patch_namespaced_service(
                name=f"crate-{name}",
                namespace=namespace,
                body={"spec": {"loadBalancerSourceRanges": new_cidrs}},
            )
        else:
            logger.info(
                f"Skipping loadBalancerSourceRanges patch: service 'crate-{name}' "
                f"is of type '{service.spec.type if service else 'missing'}'."
            )

        if exposure == "traefik":
            await update_traefik_ip_restriction(namespace, name, new_cidrs, logger)

        # Grand-central IP allowlist follows its own exposure, which
        # may differ from the CrateDB service exposure.
        if grand_central_uses_traefik(cratedb["spec"]):
            httproute = await read_grand_central_httproute(
                namespace=namespace, name=name
            )
            if httproute:
                await update_grand_central_ip_allowlist(
                    namespace=namespace,
                    name=name,
                    cidrs=new_cidrs,
                    logger=logger,
                )
        else:
            # grand-central is on nginx Ingress (e.g. cluster on Traefik but
            # GC explicitly on nginx) - patch its whitelist annotation instead.
            ingress = await read_grand_central_ingress(namespace=namespace, name=name)

            if ingress:
                await networking.patch_namespaced_ingress(
                    name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
                    namespace=namespace,
                    body={
                        "metadata": {
                            "annotations": {
                                "nginx.ingress.kubernetes.io/whitelist-source-range": ",".join(  # noqa
                                    new_cidrs
                                )
                            }
                        }
                    },
                )

    await send_operation_progress_notification(
        namespace=namespace,
        name=name,
        message="IP Network Whitelist updated successfully.",
        logger=logger,
        status=WebhookStatus.SUCCESS,
        operation=WebhookOperation.UPDATE,
        action=WebhookAction.ALLOWED_CIDR_UPDATE,
    )
