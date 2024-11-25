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
from typing import Any, List

import kopf
from kubernetes_asyncio.client import AppsV1Api

from crate.operator.config import config
from crate.operator.create import (
    get_statefulset_affinity,
    get_statefulset_env_crate_heap,
    get_tolerations,
)
from crate.operator.utils import crate
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.kubeapi import get_cratedb_resource
from crate.operator.webhooks import (
    WebhookChangeComputePayload,
    WebhookEvent,
    WebhookStatus,
)


class ChangeComputeSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(config.CLUSTER_UPDATE_TIMEOUT))
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        webhook_payload = generate_change_compute_payload(old, body)
        async with GlobalApiClient() as api_client:
            apps = AppsV1Api(api_client)
            await change_cluster_compute(apps, namespace, name, webhook_payload, logger)

        await self.send_notification_now(
            logger,
            WebhookEvent.COMPUTE_CHANGED,
            webhook_payload,
            WebhookStatus.IN_PROGRESS,
        )


class AfterChangeComputeSubHandler(StateBasedSubHandler):
    """
    A handler which depends on``restart`` having finished successfully and sends a
    success notification of the change compute process.
    """

    @crate.on.error(error_handler=crate.send_update_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        self.schedule_notification(
            WebhookEvent.COMPUTE_CHANGED,
            generate_change_compute_payload(old, body),
            WebhookStatus.SUCCESS,
        )


def generate_change_compute_payload(old, body):
    old_data = old["spec"]["nodes"]["data"][0]
    new_data = body["spec"]["nodes"]["data"][0]
    old_resources = old_data.get("resources", {})
    new_resources = new_data.get("resources", {})
    return WebhookChangeComputePayload(
        old_cpu_limit=old_resources.get("limits", {}).get("cpu"),
        old_memory_limit=old_resources.get("limits", {}).get("memory"),
        old_cpu_request=old_resources.get("requests", {}).get("cpu"),
        old_memory_request=old_resources.get("requests", {}).get("memory"),
        old_heap_ratio=old_resources.get("heapRatio"),
        old_nodepool=old_data.get("nodepool"),
        new_cpu_limit=new_resources.get("limits", {}).get("cpu"),
        new_memory_limit=new_resources.get("limits", {}).get("memory"),
        new_cpu_request=new_resources.get("requests", {}).get("cpu"),
        new_memory_request=new_resources.get("requests", {}).get("memory"),
        new_heap_ratio=new_resources.get("heapRatio"),
        new_nodepool=new_data.get("nodepool"),
    )


async def update_cprocessor_crate_settings(
    apps: AppsV1Api,
    namespace: str,
    sts_name: str,
    processors: int,
) -> List[str]:
    """
    Call the Kubernetes API, update the -Cprocessors value in the crate
    container command, and return the updated command list.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param sts_name: The name of the Kubernetes StatefulSet to update.
    :param processors: The new number of processors.
    :return: The updated command list.
    """
    stateful_set = await apps.read_namespaced_stateful_set(
        namespace=namespace, name=sts_name
    )

    for container in stateful_set.spec.template.spec.containers:
        if container.name == "crate":
            updated_command = []
            for cmd in container.command:
                if cmd.startswith("-Cprocessors=") and processors is not None:
                    updated_command.append(f"-Cprocessors={processors}")
                else:
                    updated_command.append(cmd)
            container.command = updated_command
            return container.command

    raise ValueError("Container 'crate' not found in the StatefulSet.")


async def change_cluster_compute(
    apps: AppsV1Api,
    namespace: str,
    name: str,
    compute_change_data: WebhookChangeComputePayload,
    logger: logging.Logger,
):
    """
    Patches the statefulset with the new cpu and memory requests and limits.
    """
    body = await generate_body_patch(apps, name, namespace, compute_change_data, logger)

    # Note only the stateful set is updated. Pods will become updated on restart
    sts_name = f"crate-data-hot-{name}"
    await apps.patch_namespaced_stateful_set(
        namespace=namespace,
        name=sts_name,
        body=body,
    )
    logger.info("updated the statefulset with name %s with body: %s", sts_name, body)
    pass


async def generate_body_patch(
    apps: AppsV1Api,
    name: str,
    namespace: str,
    compute_change_data: WebhookChangeComputePayload,
    logger: logging.Logger,
) -> dict:
    """
    Generates a dict representing the patch that will be applied to the statefulset.
    That patch modifies cpu/memory requests/limits based on compute_change_data.
    It also patches affinity as needed based on the existence or not of requests data.
    """

    crd = await get_cratedb_resource(namespace, name)
    if crd is None:
        raise ValueError(f"CRD {name} not found in namespace {namespace}")

    try:
        sts_name = f"crate-data-{crd['spec']['nodes']['data'][0]['name']}-{name}"
    except (KeyError, IndexError) as e:
        logger.error(f"Failed to construct sts_name: {e}")
        raise ValueError(f"Failed to construct sts_name: {e}")

    # sts_name = sts_name + f"-{name}"

    updated_command = await update_cprocessor_crate_settings(
        apps=apps,
        namespace=namespace,
        sts_name=sts_name,
        # sts_name=f"crate-data-hot-{name}",
        processors=compute_change_data["new_cpu_limit"],
    )

    node_spec = {
        "name": "crate",
        "command": updated_command,
        "env": [
            get_statefulset_env_crate_heap(
                memory=compute_change_data["new_memory_limit"],
                heap_ratio=compute_change_data["new_heap_ratio"],
            )
        ],
        "resources": {
            "limits": {
                "cpu": compute_change_data["new_cpu_limit"],
                "memory": compute_change_data["new_memory_limit"],
            },
            "requests": {
                "cpu": compute_change_data.get(
                    "new_cpu_request",
                    compute_change_data["new_cpu_limit"],
                ),
                "memory": compute_change_data.get(
                    "new_memory_request",
                    compute_change_data["new_memory_limit"],
                ),
            },
        },
    }

    body = {
        "spec": {
            "template": {
                "spec": {
                    "affinity": get_statefulset_affinity(
                        name,
                        logger,
                        {
                            **node_spec,
                            "nodepool": compute_change_data.get("new_nodepool"),
                        },
                    ),
                    "tolerations": get_tolerations(
                        name,
                        logger,
                        {
                            **node_spec,
                            "nodepool": compute_change_data.get("new_nodepool"),
                        },
                    ),
                    "containers": [node_spec],
                }
            }
        }
    }

    return body
