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

import kopf

from crate.operator.constants import CLUSTER_UPDATE_ID
from crate.operator.operations import (
    AfterClusterUpdateSubHandler,
    BeforeClusterUpdateSubHandler,
    RestartSubHandler,
)
from crate.operator.scale import ScaleSubHandler
from crate.operator.upgrade import AfterUpgradeSubHandler, UpgradeSubHandler


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

    do_upgrade = False
    do_restart = False
    do_scale = False
    for _, field_path, *_ in diff:
        if field_path in {
            ("spec", "cluster", "imageRegistry"),
            ("spec", "cluster", "version"),
        }:
            do_upgrade = True
            do_restart = True
        elif field_path == ("spec", "nodes", "master", "replicas"):
            do_scale = True
        elif field_path == ("spec", "nodes", "data"):
            do_scale = True

    if not do_upgrade and not do_restart and not do_scale:
        return

    depends_on = [f"{CLUSTER_UPDATE_ID}/before_cluster_update"]
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
