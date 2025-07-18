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

import datetime
import hashlib
import logging
from typing import List

import kopf

from crate.operator.change_compute import (
    AfterChangeComputeSubHandler,
    ChangeComputeSubHandler,
)
from crate.operator.config import config
from crate.operator.constants import CLUSTER_UPDATE_ID, OperationType
from crate.operator.expand_volume import ExpandVolumeSubHandler
from crate.operator.operations import (
    DELAY_CRONJOB,
    AfterClusterUpdateSubHandler,
    BeforeClusterUpdateSubHandler,
    RestartSubHandler,
    RestoreUserJWTAuthSubHandler,
    StartClusterSubHandler,
    SuspendClusterSubHandler,
    set_cronjob_delay,
)
from crate.operator.rollback import FinalRollbackSubHandler, RollbackUpgradeSubHandler
from crate.operator.scale import ScaleSubHandler
from crate.operator.upgrade import AfterUpgradeSubHandler, UpgradeSubHandler
from crate.operator.utils.crd import has_compute_changed
from crate.operator.utils.notifications import FlushNotificationsSubHandler
from crate.operator.webhooks import WebhookAction


async def update_cratedb(
    namespace: str,
    name: str,
    patch: kopf.Patch,
    body: kopf.Body,
    status: kopf.Status,
    diff: kopf.Diff,
    started: datetime.datetime,
    logger: logging.Logger,
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

    The hash includes the new diff + the time this particular run started.
    This helps avoid duplicate hashes for situations when the same op is
    performed repeatedly, i.e. suspend/resume/suspend/resume/...
    """
    context = status.get(CLUSTER_UPDATE_ID)
    hash_string = str(diff) + str(started)
    change_hash = hashlib.md5(hash_string.encode("utf-8")).hexdigest()
    if not context:
        context = {"ref": change_hash}
    elif context.get("ref", "") != change_hash:
        context["ref"] = change_hash

    rollback_handlers = [
        RollbackUpgradeSubHandler(namespace, name, body, patch, logger),
    ]

    # Determines whether the before_cluster_update and after_cluster_update handlers
    # will be registered
    do_before_update = True
    do_after_update = True

    do_upgrade = False
    do_change_compute = False
    do_restart = False
    do_scale = False
    do_expand_volume = False

    operation = OperationType.UNKNOWN

    for _, field_path, old_spec, new_spec in diff:
        if field_path in {
            ("spec", "cluster", "imageRegistry"),
            ("spec", "cluster", "version"),
        }:
            do_upgrade = True
            do_restart = True
            operation = OperationType.UPGRADE
        elif field_path == ("spec", "nodes", "master", "replicas"):
            do_scale = True
            operation = OperationType.SCALE
        elif field_path == ("spec", "nodes", "data"):
            for node_spec_idx in range(len(old_spec)):
                old_spec = old_spec[node_spec_idx]
                new_spec = new_spec[node_spec_idx]

                if old_spec.get("replicas") != new_spec.get("replicas"):
                    do_scale = True
                    operation = OperationType.SCALE
                    # When resuming the cluster do not register before_update
                    if old_spec.get("replicas") == 0:
                        do_before_update = False
                        operation = OperationType.RESUME

                        # Delay the cronjob re-enabling after resuming the cluster
                        await set_cronjob_delay(patch)

                    # When suspending the cluster do not register after_update
                    elif new_spec.get("replicas") == 0:
                        do_after_update = False
                        operation = OperationType.SUSPEND

                        # Do not re-enable the cronjobs if the cluster is suspended
                        patch.status[DELAY_CRONJOB] = False

                elif old_spec.get("resources", {}).get("disk", {}).get(
                    "size"
                ) != new_spec.get("resources", {}).get("disk", {}).get("size"):
                    do_expand_volume = True
                    if config.NO_DOWNTIME_STORAGE_EXPANSION:
                        do_before_update = False
                        do_after_update = False
                elif has_compute_changed(old_spec, new_spec):
                    do_change_compute = True
                    operation = OperationType.CHANGE_COMPUTE
                    # pod resources won't change until each pod is recreated
                    do_restart = True

    if (
        not do_upgrade
        and not do_restart
        and not do_scale
        and not do_expand_volume
        and not do_change_compute
    ):
        return

    for handler in rollback_handlers:
        if handler.is_in_rollback():
            rollback_operation = handler.get_operation_type()
            if rollback_operation == operation:
                logger.info(
                    f"Rollback in progress for {rollback_operation}: "
                    f"{handler.annotation_key()}, skipping update."
                )
                handler.clear_rollback()
                return
            else:
                logger.info(
                    f"Rollback active for {rollback_operation}, but current operation "
                    f"is {operation}. Proceeding with update."
                )

    depends_on: List[str] = []

    if do_before_update:
        register_before_update_handlers(
            namespace, name, change_hash, context, depends_on, operation
        )

    if do_upgrade:
        register_upgrade_handlers(
            namespace, name, change_hash, context, depends_on, operation
        )

        # Delay the cronjob re-enabling after upgrading a cluster
        # It is called here to not mess up with the values stored in the status
        # by the update subhandlers.
        await set_cronjob_delay(patch)

    if do_change_compute:
        register_change_compute_handlers(
            namespace, name, change_hash, context, depends_on, operation
        )

    if do_restart:
        register_restart_handlers(
            namespace,
            name,
            change_hash,
            context,
            depends_on,
            do_upgrade,
            do_change_compute,
            operation,
        )

    if do_scale:
        register_scale_handlers(
            namespace, name, change_hash, context, depends_on, operation
        )

    if do_expand_volume:
        register_storage_expansion_handlers(
            namespace, name, change_hash, context, depends_on
        )

    if do_after_update:
        register_after_update_handlers(
            namespace, name, change_hash, context, depends_on, operation
        )

    patch.status[CLUSTER_UPDATE_ID] = context

    # Ensure all success notifications are only sent at the very end of the handler
    kopf.register(
        fn=FlushNotificationsSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            run_on_dep_failures=True,
        )(),
        id="notify_success_update",
        backoff=get_backoff(),
    )

    # Rollback operation in case of failed dependencies
    kopf.register(
        fn=FinalRollbackSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            run_on_dep_failures=True,
            operation=operation,
        )(),
        id="final_rollback",
        backoff=get_backoff(),
    )


def register_storage_expansion_handlers(
    namespace: str, name: str, change_hash: str, context: dict, depends_on: list
):
    if config.NO_DOWNTIME_STORAGE_EXPANSION:
        kopf.register(
            fn=ExpandVolumeSubHandler(
                namespace, name, change_hash, context, depends_on=depends_on.copy()
            )(),
            id="expand_volume",
            backoff=get_backoff(),
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/expand_volume")
    else:
        kopf.register(
            fn=SuspendClusterSubHandler(
                namespace, name, change_hash, context, depends_on=depends_on.copy()
            )(),
            id="suspend_cluster",
            backoff=get_backoff(),
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/suspend_cluster")

        kopf.register(
            fn=ExpandVolumeSubHandler(
                namespace, name, change_hash, context, depends_on=depends_on.copy()
            )(),
            id="expand_volume",
            backoff=get_backoff(),
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/expand_volume")

        kopf.register(
            fn=StartClusterSubHandler(
                namespace,
                name,
                change_hash,
                context,
                depends_on=depends_on.copy(),
                run_on_dep_failures=True,
            )(),
            id="start_cluster",
            backoff=get_backoff(),
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/start_cluster")


def register_restart_handlers(
    namespace: str,
    name: str,
    change_hash: str,
    context: dict,
    depends_on: list,
    do_upgrade: bool,
    do_change_compute: bool,
    operation: OperationType,
):
    kopf.register(
        fn=RestartSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            operation=operation,
        )(action=WebhookAction.UPGRADE if do_upgrade else WebhookAction.CHANGE_COMPUTE),
        id="restart",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_UPDATE_ID}/restart")
    # Send a webhook success notification after upgrade and restart handlers
    if do_upgrade:
        kopf.register(
            fn=RestoreUserJWTAuthSubHandler(
                namespace,
                name,
                change_hash,
                context,
                depends_on=depends_on.copy(),
            )(),
            id="restore_user_jwt_auth",
            backoff=get_backoff(),
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/restore_user_jwt_auth")
        kopf.register(
            fn=AfterUpgradeSubHandler(
                namespace, name, change_hash, context, depends_on=depends_on.copy()
            )(),
            id="after_upgrade",
            backoff=get_backoff(),
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/after_upgrade")

    # Send a webhook success notification after change_compute and restart handlers
    if do_change_compute:
        kopf.register(
            fn=AfterChangeComputeSubHandler(
                namespace, name, change_hash, context, depends_on=depends_on.copy()
            )(),
            id="after_change_compute",
            backoff=get_backoff(),
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/after_change_compute")


def register_change_compute_handlers(
    namespace: str,
    name: str,
    change_hash: str,
    context: dict,
    depends_on: list,
    operation: OperationType,
):
    kopf.register(
        fn=ChangeComputeSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            operation=operation,
        )(),
        id="change_compute",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_UPDATE_ID}/change_compute")


def register_scale_handlers(
    namespace: str,
    name: str,
    change_hash: str,
    context: dict,
    depends_on: list,
    operation: OperationType,
):
    kopf.register(
        fn=ScaleSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            operation=operation,
        )(),
        id="scale",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_UPDATE_ID}/scale")


def register_upgrade_handlers(
    namespace: str,
    name: str,
    change_hash: str,
    context: dict,
    depends_on: list,
    operation: OperationType,
):
    kopf.register(
        fn=UpgradeSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            operation=operation,
        )(),
        id="upgrade",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_UPDATE_ID}/upgrade")


def register_after_update_handlers(
    namespace: str,
    name: str,
    change_hash: str,
    context: dict,
    depends_on: list,
    operation: OperationType,
):
    kopf.register(
        fn=AfterClusterUpdateSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            run_on_dep_failures=True,
            operation=operation,
        )(),
        id="after_cluster_update",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_UPDATE_ID}/after_cluster_update")


def register_before_update_handlers(
    namespace: str,
    name: str,
    change_hash: str,
    context: dict,
    depends_on: list,
    operation: OperationType,
):
    kopf.register(
        fn=BeforeClusterUpdateSubHandler(
            namespace, name, change_hash, context, operation=operation
        )(),
        id="before_cluster_update",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_UPDATE_ID}/before_cluster_update")


def get_backoff() -> int:
    """
    When in testing mode, use a shorter backoff period as it help with speeding up some
    tests (they don't have to wait so long due to transient errors).
    """
    if config.TESTING:
        return 5
    return 30
