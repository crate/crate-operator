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
from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.config import config
from crate.operator.constants import DATA_NODE_NAME, DATA_PVC_NAME_PREFIX
from crate.operator.operations import get_pvcs_in_namespace
from crate.operator.utils import crate
from crate.operator.utils.formatting import convert_to_bytes
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.notifications import send_operation_progress_notification
from crate.operator.webhooks import (
    WebhookEvent,
    WebhookFeedbackPayload,
    WebhookOperation,
    WebhookStatus,
)


async def expand_volume(
    core: CoreV1Api,
    namespace: str,
    name: str,
    data_diff_items: kopf.Diff,
    logger: logging.Logger,
):
    """
    Expand a cluster's disk size according to the given ``data_diff_items``.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param old: The old resource body.
    :param data_diff_items: A list of changes made to the individual
        data node specifications.
    """
    all_pvcs = await get_pvcs_in_namespace(core, namespace, name, DATA_NODE_NAME)
    pvc_storage = {}

    await send_operation_progress_notification(
        namespace=namespace,
        name=name,
        message="Resizing volume(s).",
        logger=logger,
        status=WebhookStatus.IN_PROGRESS,
        operation=WebhookOperation.UPDATE,
    )

    for pvc in all_pvcs:
        if not pvc["name"].startswith(DATA_PVC_NAME_PREFIX):
            continue
        current_pvc = await core.read_namespaced_persistent_volume_claim(
            name=pvc["name"],
            namespace=namespace,
        )
        if data_diff_items:
            for _, field_path, old_storage, new_storage in data_diff_items:
                current_storage = convert_to_bytes(
                    current_pvc.spec.resources.requests["storage"]
                )
                new_storage = convert_to_bytes(new_storage)
                if current_storage != new_storage:
                    body: Dict[str, Any] = {
                        "spec": {
                            "resources": {
                                "requests": {"storage": str(int(new_storage))}
                            },
                        }
                    }
                    logger.info(f"Patch PVC {pvc['name']} with body {body}")
                    await core.patch_namespaced_persistent_volume_claim(
                        name=pvc["name"],
                        namespace=namespace,
                        body=body,
                    )
                storage_status = convert_to_bytes(
                    current_pvc.status.capacity["storage"]
                )
                logger.info(
                    f"PVC {pvc['name']} storage current/new "
                    f"{int(storage_status)}/{int(new_storage)}"
                )
                conditions = current_pvc.status.conditions or []
                if int(storage_status) == int(new_storage) or any(
                    cond.type == "FileSystemResizePending" for cond in conditions
                ):
                    pvc_storage[pvc["name"]] = {
                        "in_progress": False,
                    }
                else:
                    pvc_storage[pvc["name"]] = {
                        "in_progress": True,
                    }
    # If resizing for at least one of the PVCs is not finished, we try again.
    # Or assume that the StorageClass does not support expansion and fail
    # after the timeout is reached.
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
        expand_data_diff_items: Optional[List[kopf.DiffItem]] = None

        for operation, field_path, old_value, new_value in diff:
            if field_path == ("spec", "nodes", "data"):
                expand_data_diff_items = []
                for node_spec_idx in range(len(old_value)):
                    old_spec = old_value[node_spec_idx]
                    new_spec = new_value[node_spec_idx]

                    expand_data_diff_items.append(
                        kopf.DiffItem(
                            kopf.DiffOperation.CHANGE,
                            (str(node_spec_idx), "resources", "disk", "size"),
                            old_spec["resources"]["disk"]["size"],
                            new_spec["resources"]["disk"]["size"],
                        )
                    )
            else:
                logger.info("Ignoring operation %s on field %s", operation, field_path)

        if expand_data_diff_items:
            async with ApiClient() as api_client:
                core = CoreV1Api(api_client)

                await expand_volume(
                    core,
                    namespace,
                    name,
                    kopf.Diff(expand_data_diff_items),
                    logger,
                )

        # schedule success notification and send it after the cluster
        # has been restarted successfully.
        self.schedule_notification(
            WebhookEvent.FEEDBACK,
            WebhookFeedbackPayload(
                message="The cluster storage has been resized successfully.",
                operation=WebhookOperation.UPDATE,
            ),
            WebhookStatus.SUCCESS,
        )
