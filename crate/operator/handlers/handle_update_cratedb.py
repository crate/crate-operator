# CrateDB Kubernetes Operator
# Copyright (C) 2021 Crate.IO GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import hashlib
import logging
from typing import Any, List, Optional

import kopf
from kubernetes_asyncio.client import ApiClient, AppsV1Api, CoreV1Api

from crate.operator.config import config
from crate.operator.constants import CLUSTER_UPDATE_ID
from crate.operator.expand_volume import (
    EXPAND_REPLICAS_IN_PROGRESS_MSG,
    ExpandVolumeSubHandler,
)
from crate.operator.operations import (
    AfterClusterUpdateSubHandler,
    BeforeClusterUpdateSubHandler,
    RestartSubHandler,
    suspend_or_start_cluster,
)
from crate.operator.scale import SUSPEND_IN_PROGRESS_MSG, ScaleSubHandler
from crate.operator.upgrade import AfterUpgradeSubHandler, UpgradeSubHandler
from crate.operator.utils import crate
from crate.operator.utils.kopf import StateBasedSubHandler


async def update_cratedb(
    namespace: str,
    name: str,
    patch: kopf.Patch,
    status: kopf.Status,
    diff: kopf.Diff,
):
    """
    Handle cluster updates.

    This is done as a chain of sub-handlers that depend on the previous ones completing.
    The state of each handler is stored in the status field of the CrateDB
    custom resource. Since the status field persists between runs of this handler
    (even for unrelated runs), we calculate and store a hash of what changed as well.
    This hash is then used by the sub-handlers to work out which run they are part of.

    i.e., consider this status:

    ::

        status:
          cluster_update:
            ref: 24b527bf0eada363bf548f19b98dd9cb
          cluster_update/after_cluster_update:
            ref: 24b527bf0eada363bf548f19b98dd9cb
            success: true
          cluster_update/before_cluster_update:
            ref: 24b527bf0eada363bf548f19b98dd9cb
            success: true
          cluster_update/scale:
            ref: 24b527bf0eada363bf548f19b98dd9cb
            success: true


    here ``status.cluster_update.ref`` is the hash of the last diff that was being acted
    upon. Since kopf *does not clean up statuses*, when we start a new run we check if
    the hash matches - if not, it means we can disregard any refs that are not for this
    run.
    """
    context = status.get(CLUSTER_UPDATE_ID)
    hash = hashlib.md5(str(diff).encode("utf-8")).hexdigest()
    if not context:
        context = {"ref": hash}
    elif context.get("ref", "") != hash:
        context["ref"] = hash

    # Determines whether the before_cluster_update and after_cluster_update handlers
    # will be registered
    do_before_update = True
    do_after_update = True

    do_upgrade = False
    do_restart = False
    do_scale = False
    do_suspend = False
    do_resume = False
    do_expand_volume = False

    for _, field_path, old_spec, new_spec in diff:
        if field_path in {
            ("spec", "cluster", "imageRegistry"),
            ("spec", "cluster", "version"),
        }:
            do_upgrade = True
            do_restart = True
        elif field_path == ("spec", "nodes", "master", "replicas"):
            do_scale = True
        elif field_path == ("spec", "nodes", "data"):
            for node_spec_idx in range(len(old_spec)):
                old_spec = old_spec[node_spec_idx]
                new_spec = new_spec[node_spec_idx]

                if old_spec.get("replicas") != new_spec.get("replicas"):
                    # When resuming the cluster do not register before_update
                    if old_spec.get("replicas") == 0:
                        do_resume = True
                        do_before_update = False
                    # When suspending the cluster do not register after_update
                    elif new_spec.get("replicas") == 0:
                        do_suspend = True
                        do_after_update = False
                    else:
                        do_scale = True
                elif old_spec.get("resources", {}).get("disk", {}).get(
                    "size"
                ) != new_spec.get("resources", {}).get("disk", {}).get("size"):
                    do_expand_volume = True

    if not do_upgrade and not do_restart and not do_scale and not do_expand_volume:
        return

    depends_on = []

    if do_before_update:
        depends_on.append(f"{CLUSTER_UPDATE_ID}/before_cluster_update")
        kopf.register(
            fn=BeforeClusterUpdateSubHandler(namespace, name, hash, context)(),
            id="before_cluster_update",
        )

    if do_upgrade:
        kopf.register(
            fn=UpgradeSubHandler(
                namespace, name, hash, context, depends_on=depends_on.copy()
            )(),
            id="upgrade",
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/upgrade")
    if do_restart:
        kopf.register(
            fn=RestartSubHandler(
                namespace, name, hash, context, depends_on=depends_on.copy()
            )(),
            id="restart",
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/restart")
        if do_upgrade:
            kopf.register(
                fn=AfterUpgradeSubHandler(
                    namespace, name, hash, context, depends_on=depends_on.copy()
                )(),
                id="after_upgrade",
            )
            depends_on.append(f"{CLUSTER_UPDATE_ID}/after_upgrade")

    if do_scale:
        kopf.register(
            fn=ScaleSubHandler(
                namespace,
                name,
                hash,
                context,
                depends_on=depends_on.copy(),
            )(),
            id="scale",
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/scale")
    if do_suspend:
        kopf.register(
            fn=SuspendClusterSubHandler(
                SUSPEND_IN_PROGRESS_MSG,
                namespace,
                name,
                hash,
                context,
                depends_on=depends_on.copy(),
            )(),
            id="suspend_cluster",
        )
    if do_resume:
        kopf.register(
            fn=StartClusterSubHandler(
                namespace,
                name,
                hash,
                context,
                depends_on=depends_on.copy(),
            )(),
            id="resume_cluster",
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/resume_cluster")
    if do_expand_volume:
        # Volume expansion and cluster suspension can run in parallel. Scaling the
        # cluster back up needs to wait until both operations are finished.
        kopf.register(
            fn=ExpandVolumeSubHandler(
                namespace, name, hash, context, depends_on=depends_on.copy()
            )(),
            id="expand_volume",
        )
        kopf.register(
            fn=SuspendClusterSubHandler(
                EXPAND_REPLICAS_IN_PROGRESS_MSG,
                namespace,
                name,
                hash,
                context,
                depends_on=depends_on.copy(),
            )(),
            id="suspend_cluster",
        )
        depends_on.extend(
            [
                f"{CLUSTER_UPDATE_ID}/suspend_cluster",
                f"{CLUSTER_UPDATE_ID}/expand_volume",
            ]
        )
        kopf.register(
            fn=StartClusterSubHandler(
                namespace,
                name,
                hash,
                context,
                depends_on=depends_on.copy(),
                run_on_dep_failures=True,
            )(),
            id="start_cluster",
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/start_cluster")

    if do_after_update:
        kopf.register(
            fn=AfterClusterUpdateSubHandler(
                namespace,
                name,
                hash,
                context,
                depends_on=depends_on.copy(),
                run_on_dep_failures=True,
            )(),
            id="after_cluster_update",
        )

    patch.status[CLUSTER_UPDATE_ID] = context


class StartClusterSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(config.SCALING_TIMEOUT))
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        spec: kopf.Spec,
        old: kopf.Body,
        diff: kopf.Diff,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        scale_data_diff_items: Optional[List[kopf.DiffItem]] = None

        for operation, field_path, old_value, new_value in diff:
            if field_path == ("spec", "nodes", "data"):
                scale_data_diff_items = []
                for node_spec_idx in range(len(old_value)):
                    new_spec = new_value[node_spec_idx]

                    scale_data_diff_items.append(
                        kopf.DiffItem(
                            kopf.DiffOperation.CHANGE,
                            (str(node_spec_idx), "replicas"),
                            0,
                            new_spec["replicas"],
                        )
                    )
            else:
                logger.info("Ignoring operation %s on field %s", operation, field_path)

        if scale_data_diff_items:
            async with ApiClient() as api_client:
                apps = AppsV1Api(api_client)
                core = CoreV1Api(api_client)

                await suspend_or_start_cluster(
                    apps,
                    core,
                    namespace,
                    name,
                    old,
                    kopf.Diff(scale_data_diff_items),
                    logger,
                    None,
                )

        await self.send_notifications(logger)


class SuspendClusterSubHandler(StateBasedSubHandler):
    in_progress_message: str

    def __init__(
        self,
        in_progress_msg: str,
        namespace: str,
        name: str,
        hash: str,
        context: dict,
        depends_on=None,
        run_on_dep_failures=False,
    ):
        self.in_progress_message = in_progress_msg
        super().__init__(
            namespace,
            name,
            hash,
            context,
            depends_on=depends_on,
            run_on_dep_failures=run_on_dep_failures,
        )

    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(config.SCALING_TIMEOUT))
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        spec: kopf.Spec,
        old: kopf.Body,
        diff: kopf.Diff,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        scale_data_diff_items: Optional[List[kopf.DiffItem]] = None

        for operation, field_path, old_value, new_value in diff:
            if field_path == ("spec", "nodes", "data"):
                scale_data_diff_items = []
                for node_spec_idx in range(len(old_value)):
                    old_spec = old_value[node_spec_idx]

                    # scale all data nodes to 0 replicas
                    scale_data_diff_items.append(
                        kopf.DiffItem(
                            kopf.DiffOperation.CHANGE,
                            (str(node_spec_idx), "replicas"),
                            old_spec["replicas"],
                            0,
                        )
                    )
            else:
                logger.info("Ignoring operation %s on field %s", operation, field_path)

        if scale_data_diff_items:
            async with ApiClient() as api_client:
                apps = AppsV1Api(api_client)
                core = CoreV1Api(api_client)

                await suspend_or_start_cluster(
                    apps,
                    core,
                    namespace,
                    name,
                    old,
                    kopf.Diff(scale_data_diff_items),
                    logger,
                    self.in_progress_message,
                )
