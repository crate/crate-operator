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
import math
from typing import Any, List, Optional

import kopf
from kubernetes_asyncio.client import AppsV1Api

from crate.operator.config import config
from crate.operator.create import (
    get_statefulset_affinity,
    get_statefulset_env_crate_heap,
    get_tolerations,
)
from crate.operator.operations import iter_node_groups
from crate.operator.utils import crate
from crate.operator.utils.crd import has_compute_changed
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
            await change_cluster_compute(apps, namespace, name, old, body, logger)

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


def compute_payload_for_specs(old_spec, new_spec) -> WebhookChangeComputePayload:
    """
    Build a compute-change payload from a single node group's old/new specs.
    The same StatefulSet patch is derived from this payload for masters and
    data groups alike (resources are read per group, since master CPU/mem may
    differ from data).
    """
    old_resources = old_spec.get("resources", {})
    new_resources = new_spec.get("resources", {})
    return WebhookChangeComputePayload(
        old_cpu_limit=old_resources.get("limits", {}).get("cpu"),
        old_memory_limit=old_resources.get("limits", {}).get("memory"),
        old_cpu_request=old_resources.get("requests", {}).get("cpu"),
        old_memory_request=old_resources.get("requests", {}).get("memory"),
        old_heap_ratio=old_resources.get("heapRatio"),
        old_nodepool=old_spec.get("nodepool"),
        new_cpu_limit=new_resources.get("limits", {}).get("cpu"),
        new_memory_limit=new_resources.get("limits", {}).get("memory"),
        new_cpu_request=new_resources.get("requests", {}).get("cpu"),
        new_memory_request=new_resources.get("requests", {}).get("memory"),
        new_heap_ratio=new_resources.get("heapRatio"),
        new_nodepool=new_spec.get("nodepool"),
    )


def _master_compute_fields(old_master, new_master) -> dict:
    """
    The dedicated-master compute fields for the CHANGE_COMPUTE webhook, keyed
    with the ``*_master_*`` names. Empty for clusters without dedicated masters.
    """
    if old_master is None or new_master is None:
        return {}
    p = compute_payload_for_specs(old_master, new_master)
    return {
        "old_master_cpu_limit": p["old_cpu_limit"],
        "old_master_memory_limit": p["old_memory_limit"],
        "old_master_cpu_request": p["old_cpu_request"],
        "old_master_memory_request": p["old_memory_request"],
        "old_master_heap_ratio": p["old_heap_ratio"],
        "old_master_nodepool": p["old_nodepool"],
        "new_master_cpu_limit": p["new_cpu_limit"],
        "new_master_memory_limit": p["new_memory_limit"],
        "new_master_cpu_request": p["new_cpu_request"],
        "new_master_memory_request": p["new_memory_request"],
        "new_master_heap_ratio": p["new_heap_ratio"],
        "new_master_nodepool": p["new_nodepool"],
    }


def generate_change_compute_payload(old, body):
    # Reports the first data group's compute plus, additively, the dedicated
    # masters' compute (so billing sees master compute changes too).
    payload = compute_payload_for_specs(
        old["spec"]["nodes"]["data"][0], body["spec"]["nodes"]["data"][0]
    )
    payload.update(
        _master_compute_fields(
            old["spec"]["nodes"].get("master"),
            body["spec"]["nodes"].get("master"),
        )
    )
    return payload


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
                    # Round up to match create.py, which bakes -Cprocessors as
                    # the rounded-up CPU limit.
                    updated_command.append(
                        f"-Cprocessors={math.ceil(float(processors))}"
                    )
                else:
                    updated_command.append(cmd)
            container.command = updated_command
            return container.command

    raise ValueError("Container 'crate' not found in the StatefulSet.")


async def change_cluster_compute(
    apps: AppsV1Api,
    namespace: str,
    name: str,
    old: kopf.Body,
    body: kopf.Body,
    logger: logging.Logger,
):
    """
    Patch the StatefulSet of every node group whose compute changed -- the
    dedicated masters and/or any data group -- each with its own resources.
    Pods pick up the new cpu/memory on the subsequent restart.
    """
    old_groups = {g.name: g for g in iter_node_groups(old["spec"]["nodes"])}

    for group in iter_node_groups(body["spec"]["nodes"]):
        old_group = old_groups.get(group.name)
        if old_group is None or not has_compute_changed(old_group.spec, group.spec):
            continue

        sts_name = group.statefulset_name(name)
        compute_change_data = compute_payload_for_specs(old_group.spec, group.spec)
        patch = await generate_body_patch(
            apps, name, namespace, compute_change_data, logger, sts_name=sts_name
        )
        # Note only the stateful set is updated. Pods are updated on restart.
        await apps.patch_namespaced_stateful_set(
            namespace=namespace, name=sts_name, body=patch
        )
        logger.info("updated the statefulset %s with body: %s", sts_name, patch)


async def generate_body_patch(
    apps: AppsV1Api,
    name: str,
    namespace: str,
    compute_change_data: WebhookChangeComputePayload,
    logger: logging.Logger,
    sts_name: Optional[str] = None,
) -> dict:
    """
    Generates a dict representing the patch that will be applied to the statefulset.
    That patch modifies cpu/memory requests/limits based on compute_change_data.
    It also patches affinity as needed based on the existence or not of requests data.

    :param sts_name: The StatefulSet to patch. Defaults to the first data
        group's StatefulSet when omitted (callers patching the master or a
        specific data group pass it explicitly).
    """

    if sts_name is None:
        crd = await get_cratedb_resource(namespace, name)
        if crd is None:
            raise ValueError(f"CRD {name} not found in namespace {namespace}")

        try:
            sts_name = f"crate-data-{crd['spec']['nodes']['data'][0]['name']}-{name}"
        except (KeyError, IndexError) as e:
            logger.error(f"Failed to construct sts_name: {e}")
            raise ValueError(f"Failed to construct sts_name: {e}")

    updated_command = await update_cprocessor_crate_settings(
        apps=apps,
        namespace=namespace,
        sts_name=sts_name,
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
