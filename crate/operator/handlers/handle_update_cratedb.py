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
from typing import List

import kopf

from crate.operator.change_compute import (
    AfterChangeComputeSubHandler,
    ChangeComputeSubHandler,
    has_compute_changed,
)
from crate.operator.config import config
from crate.operator.constants import CLUSTER_UPDATE_ID
from crate.operator.expand_volume import ExpandVolumeSubHandler
from crate.operator.operations import (
    AfterClusterUpdateSubHandler,
    BeforeClusterUpdateSubHandler,
    RestartSubHandler,
    StartClusterSubHandler,
    SuspendClusterSubHandler,
)
from crate.operator.scale import ScaleSubHandler
from crate.operator.upgrade import AfterUpgradeSubHandler, UpgradeSubHandler
from crate.operator.utils.notifications import FlushNotificationsSubHandler


async def update_cratedb(
    namespace: str,
    name: str,
    patch: kopf.Patch,
    status: kopf.Status,
    diff: kopf.Diff,
    started: datetime.datetime,
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

    # Determines whether the before_cluster_update and after_cluster_update handlers
    # will be registered
    do_before_update = True
    do_after_update = True

    do_upgrade = False
    do_change_compute = False
    do_restart = False
    do_scale = False
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
                    do_scale = True
                    # When resuming the cluster do not register before_update
                    if old_spec.get("replicas") == 0:
                        do_before_update = False
                    # When suspending the cluster do not register after_update
                    elif new_spec.get("replicas") == 0:
                        do_after_update = False
                elif old_spec.get("resources", {}).get("disk", {}).get(
                    "size"
                ) != new_spec.get("resources", {}).get("disk", {}).get("size"):
                    do_expand_volume = True
                elif has_compute_changed(old_spec, new_spec):
                    do_change_compute = True
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

    depends_on: List[str] = []

    if do_before_update:
        register_before_update_handlers(
            namespace, name, change_hash, context, depends_on
        )

    if do_upgrade:
        register_upgrade_handlers(namespace, name, change_hash, context, depends_on)

    if do_change_compute:
        register_change_compute_handlers(
            namespace, name, change_hash, context, depends_on
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
        )

    if do_scale:
        register_scale_handlers(namespace, name, change_hash, context, depends_on)

    if do_expand_volume:
        register_storage_expansion_handlers(
            namespace, name, change_hash, context, depends_on
        )

    if do_after_update:
        register_after_update_handlers(
            namespace, name, change_hash, context, depends_on
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


def register_storage_expansion_handlers(
    namespace: str, name: str, change_hash: str, context: dict, depends_on: list
):
    # Volume expansion and cluster suspension can run in parallel. Scaling the
    # cluster back up needs to wait until both operations are finished.
    kopf.register(
        fn=ExpandVolumeSubHandler(
            namespace, name, change_hash, context, depends_on=depends_on.copy()
        )(),
        id="expand_volume",
        backoff=get_backoff(),
    )
    if config.NO_DOWNTIME_STORAGE_EXPANSION:
        depends_on.extend(
            [
                f"{CLUSTER_UPDATE_ID}/expand_volume",
            ]
        )
    else:
        kopf.register(
            fn=SuspendClusterSubHandler(
                namespace, name, change_hash, context, depends_on=depends_on.copy()
            )(),
            id="suspend_cluster",
            backoff=get_backoff(),
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
):
    kopf.register(
        fn=RestartSubHandler(
            namespace, name, change_hash, context, depends_on=depends_on.copy()
        )(),
        id="restart",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_UPDATE_ID}/restart")
    # Send a webhook success notification after upgrade and restart handlers
    if do_upgrade:
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
    namespace: str, name: str, change_hash: str, context: dict, depends_on: list
):
    kopf.register(
        fn=ChangeComputeSubHandler(
            namespace, name, change_hash, context, depends_on=depends_on.copy()
        )(),
        id="change_compute",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_UPDATE_ID}/change_compute")


def register_scale_handlers(
    namespace: str, name: str, change_hash: str, context: dict, depends_on: list
):
    kopf.register(
        fn=ScaleSubHandler(
            namespace, name, change_hash, context, depends_on=depends_on.copy()
        )(),
        id="scale",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_UPDATE_ID}/scale")


def register_upgrade_handlers(
    namespace: str, name: str, change_hash: str, context: dict, depends_on: list
):
    kopf.register(
        fn=UpgradeSubHandler(
            namespace, name, change_hash, context, depends_on=depends_on.copy()
        )(),
        id="upgrade",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_UPDATE_ID}/upgrade")


def register_after_update_handlers(
    namespace: str, name: str, change_hash: str, context: dict, depends_on: list
):
    kopf.register(
        fn=AfterClusterUpdateSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            run_on_dep_failures=True,
        )(),
        id="after_cluster_update",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_UPDATE_ID}/after_cluster_update")


def register_before_update_handlers(
    namespace: str, name: str, change_hash: str, context: dict, depends_on: list
):
    kopf.register(
        fn=BeforeClusterUpdateSubHandler(namespace, name, change_hash, context)(),
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
