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
from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.utils.notifications import send_operation_progress_notification
from crate.operator.webhooks import WebhookOperation, WebhookStatus


async def update_service_allowed_cidrs(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    logger: logging.Logger,
):
    change: DiffItem = diff[0]
    logger.info(f"Updating load balancer source ranges to {change.new}")

    await send_operation_progress_notification(
        namespace=namespace,
        name=name,
        message="Updating IP Network Whitelist.",
        logger=logger,
        status=WebhookStatus.IN_PROGRESS,
        operation=WebhookOperation.UPDATE,
    )

    async with ApiClient() as api_client:
        core = CoreV1Api(api_client)
        # This also runs on creation events, so we want to double check that the service
        # exists before attempting to do anything.
        services = await core.list_namespaced_service(namespace=namespace)
        service = next(
            (svc for svc in services.items if svc.metadata.name == f"crate-{name}"),
            None,
        )
        if not service:
            return

        await core.patch_namespaced_service(
            name=f"crate-{name}",
            namespace=namespace,
            body={"spec": {"loadBalancerSourceRanges": change.new}},
        )

    await send_operation_progress_notification(
        namespace=namespace,
        name=name,
        message="IP Network Whitelist updated successfully.",
        logger=logger,
        status=WebhookStatus.SUCCESS,
        operation=WebhookOperation.UPDATE,
    )
