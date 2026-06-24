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
from typing import Any, Dict, List, Tuple

import kopf
from kubernetes_asyncio.client import CoreV1Api

from crate.operator.config import config
from crate.operator.constants import DATA_PVC_NAME_PREFIX
from crate.operator.operations import get_pvcs_in_namespace
from crate.operator.utils import crate
from crate.operator.utils.formatting import convert_to_bytes
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.notifications import send_operation_progress_notification
from crate.operator.webhooks import (
    WebhookAction,
    WebhookEvent,
    WebhookFeedbackPayload,
    WebhookOperation,
    WebhookStatus,
    WebhookStorageGroupPayload,
)


def collect_disk_expansions(
    diff: kopf.Diff, logger: logging.Logger
) -> List[Tuple[str, Any]]:
    """
    Build one ``(node_label, new_size)`` pair per node group whose disk size
    changed. Data groups are a list (diffed as a whole); the dedicated masters
    are a single dict, so kopf reports their disk size at the leaf.

    :param diff: The kopf diff for the update.
    """
    expansions: List[Tuple[str, Any]] = []
    for operation, field_path, old_value, new_value in diff:
        if field_path == ("spec", "nodes", "data"):
            for old_spec, new_spec in zip(old_value, new_value):
                old_size = old_spec["resources"]["disk"]["size"]
                new_size = new_spec["resources"]["disk"]["size"]
                if old_size != new_size:
                    expansions.append((new_spec["name"], new_size))
        elif field_path == ("spec", "nodes", "master", "resources", "disk", "size"):
            expansions.append(("master", new_value))
        else:
            logger.info("Ignoring operation %s on field %s", operation, field_path)
    return expansions


async def expand_volume(
    core: CoreV1Api,
    namespace: str,
    name: str,
    expansions: List[Tuple[str, Any]],
    logger: logging.Logger,
):
    """
    Expand the disk volumes of one or more node groups.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param expansions: One ``(node_label, new_size)`` pair per node group whose
        disk size changed. ``node_label`` is the group's node-name label
        (``"master"`` for the dedicated masters, or a data group's name);
        ``new_size`` is the target disk size (a size string or byte count).
    """
    await send_operation_progress_notification(
        namespace=namespace,
        name=name,
        message="Resizing volume(s).",
        logger=logger,
        status=WebhookStatus.IN_PROGRESS,
        operation=WebhookOperation.UPDATE,
        action=WebhookAction.EXPAND_STORAGE,
    )

    pvc_storage = {}
    for node_label, new_size in expansions:
        new_storage = convert_to_bytes(new_size)
        all_pvcs = await get_pvcs_in_namespace(core, namespace, name, node_label)
        for pvc in all_pvcs:
            if not pvc["name"].startswith(DATA_PVC_NAME_PREFIX):
                continue
            current_pvc = await core.read_namespaced_persistent_volume_claim(
                name=pvc["name"],
                namespace=namespace,
            )
            current_storage = convert_to_bytes(
                current_pvc.spec.resources.requests["storage"]
            )
            if current_storage != new_storage:
                body: Dict[str, Any] = {
                    "spec": {
                        "resources": {"requests": {"storage": str(int(new_storage))}},
                    }
                }
                logger.info(f"Patch PVC {pvc['name']} with body {body}")
                await core.patch_namespaced_persistent_volume_claim(
                    name=pvc["name"],
                    namespace=namespace,
                    body=body,
                )
            storage_status = convert_to_bytes(current_pvc.status.capacity["storage"])
            logger.info(
                f"PVC {pvc['name']} storage current/new "
                f"{int(storage_status)}/{int(new_storage)}"
            )
            conditions = current_pvc.status.conditions or []
            # We only need the resize accepted, not physically complete -
            # ``Resizing/FileSystemResizePending`` (or capacity already grown)
            # is enough. The online resize finishes on its own, waiting would
            # time out on slow backends.
            accepted = {"Resizing", "FileSystemResizePending"}
            if int(storage_status) == int(new_storage) or any(
                cond.type in accepted for cond in conditions
            ):
                pvc_storage[pvc["name"]] = {"in_progress": False}
            else:
                pvc_storage[pvc["name"]] = {"in_progress": True}
    # If the expansion has not been accepted for at least one PVC yet, retry.
    # A StorageClass that does not support expansion never produces a resize
    # condition, so this still fails after the timeout is reached.
    if pending_pvc := next(
        (
            name
            for name, value in pvc_storage.items()
            if value.get("in_progress") is True
        ),
        None,
    ):
        raise kopf.TemporaryError(
            f"Resizing is still in progress for PVC {pending_pvc}. Retrying... ",
            delay=15,
        )


class ExpandVolumeSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=90 if config.TESTING else config.EXPAND_VOLUME_TIMEOUT)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        spec: kopf.Spec,
        old: kopf.Body,
        new: kopf.Body,
        diff: kopf.Diff,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        expansions = collect_disk_expansions(diff, logger)

        if expansions:
            async with GlobalApiClient() as api_client:
                core = CoreV1Api(api_client)

                await expand_volume(core, namespace, name, expansions, logger)

        # schedule success notification and send it after the cluster
        # has been restarted successfully. The per-group new sizes let billing
        # attribute storage per node group (master, hot, ...).
        self.schedule_notification(
            WebhookEvent.FEEDBACK,
            WebhookFeedbackPayload(
                message="The cluster storage has been resized successfully.",
                operation=WebhookOperation.UPDATE,
                action=WebhookAction.EXPAND_STORAGE,
                disk_sizes=[
                    WebhookStorageGroupPayload(
                        name=node_label, new_disk_size=str(new_size)
                    )
                    for node_label, new_size in expansions
                ],
            ),
            WebhookStatus.SUCCESS,
        )
