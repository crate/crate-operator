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
import datetime
import hashlib

import kopf

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
    hash = hashlib.md5(hash_string.encode("utf-8")).hexdigest()
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
                namespace, name, hash, context, depends_on=depends_on.copy()
            )(),
            id="scale",
        )
        depends_on.append(f"{CLUSTER_UPDATE_ID}/scale")
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
                namespace, name, hash, context, depends_on=depends_on.copy()
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
