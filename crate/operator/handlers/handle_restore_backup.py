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

from crate.operator.config import config
from crate.operator.constants import (
    DEFAULT_BACKUP_STORAGE_PROVIDER,
    SnapshotRestoreType,
)
from crate.operator.handlers.handle_update_cratedb import get_backoff
from crate.operator.operations import (
    AfterClusterUpdateSubHandler,
    BeforeClusterUpdateSubHandler,
    RestoreUserJWTAuthSubHandler,
)
from crate.operator.restore_backup import (
    AfterRestoreBackupSubHandler,
    BeforeRestoreBackupSubHandler,
    ResetSnapshotSubHandler,
    RestoreBackupSubHandler,
    RestoreInternalUsersPasswordSubHandler,
    SendSuccessNotificationSubHandler,
    ValidateRestoreCompleteSubHandler,
    ensure_no_restore_in_progress,
    get_crash_pod_name,
    get_crash_scheme,
)
from crate.operator.utils.kubeapi import get_cratedb_resource
from crate.operator.utils.notifications import FlushNotificationsSubHandler

CLUSTER_RESTORE_FIELD_ID = "cluster_restore/spec.cluster.restoreSnapshot"


async def restore_backup(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    new: kopf.Body,
    patch: kopf.Patch,
    status: kopf.Status,
    started: datetime.datetime,
    logger: logging.Logger,
):
    context = status.get(CLUSTER_RESTORE_FIELD_ID)
    hash_string = str(diff) + str(started)
    change_hash = hashlib.md5(hash_string.encode("utf-8")).hexdigest()
    if not context:
        context = {"ref": change_hash}
    elif context.get("ref", "") != change_hash:
        context["ref"] = change_hash

    cratedb = await get_cratedb_resource(namespace, name)
    snapshot = new["snapshot"]
    restore_type = new.get("type", SnapshotRestoreType.TABLES.value)
    tables = new.get("tables", [])
    partitions = new.get("partitions", [])
    sections = new.get("sections", [])
    backup_provider = new.get("backupProvider", DEFAULT_BACKUP_STORAGE_PROVIDER)

    scheme = get_crash_scheme(cratedb)
    pod_name = get_crash_pod_name(cratedb, name)
    repository = f"restore_backup_{int(started.timestamp())}"

    # If there is a restore operation in progress, we do not do
    # anything else until it is finished.
    await ensure_no_restore_in_progress(
        namespace, name, snapshot, pod_name, scheme, logger
    )

    depends_on: List[str] = []

    register_before_restore_handlers(
        namespace, name, patch, change_hash, context, depends_on
    )

    register_restore_handlers(
        namespace,
        name,
        change_hash,
        context,
        depends_on,
        repository,
        snapshot,
        restore_type,
        backup_provider,
        tables,
        partitions,
        sections,
    )

    register_after_restore_handlers(
        namespace,
        name,
        status,
        change_hash,
        context,
        depends_on,
        repository,
    )

    kopf.register(
        fn=FlushNotificationsSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            run_on_dep_failures=True,
        )(),
        id="flush_notifications",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_RESTORE_FIELD_ID}/flush_notifications")

    # This needs to be the last subhandler, otherwise the operation does
    # not finish properly.
    kopf.register(
        fn=ResetSnapshotSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            run_on_dep_failures=True,
        )(),
        id="reset_snapshot",
        backoff=get_backoff(),
    )

    patch.status[CLUSTER_RESTORE_FIELD_ID] = context


def register_before_restore_handlers(
    namespace: str,
    name: str,
    patch: kopf.Patch,
    change_hash: str,
    context: dict,
    depends_on: list,
):
    kopf.register(
        fn=BeforeClusterUpdateSubHandler(
            namespace, name, change_hash, context, depends_on=depends_on.copy()
        )(),
        id="before_cluster_update",
        backoff=get_backoff(),
    )
    kopf.register(
        fn=BeforeRestoreBackupSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
        )(patch=patch),
        id="before_restore_backup",
        backoff=get_backoff(),
    )
    depends_on.extend(
        [
            f"{CLUSTER_RESTORE_FIELD_ID}/before_cluster_update",
            f"{CLUSTER_RESTORE_FIELD_ID}/before_restore_backup",
        ]
    )


def register_restore_handlers(
    namespace: str,
    name: str,
    change_hash: str,
    context: dict,
    depends_on: list,
    repository: str,
    snapshot: str,
    restore_type: str,
    backup_provider: str,
    tables: list,
    partitions: list,
    sections: list,
):
    kopf.register(
        fn=RestoreBackupSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
        )(
            repository=repository,
            snapshot=snapshot,
            restore_type=restore_type,
            backup_provider=backup_provider,
            tables=tables,
            partitions=partitions,
            sections=sections,
        ),
        id="restore_backup_data",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_RESTORE_FIELD_ID}/restore_backup_data")

    kopf.register(
        fn=RestoreInternalUsersPasswordSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            run_on_dep_failures=True,
        )(),
        id="restore_system_user_password",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_RESTORE_FIELD_ID}/restore_system_user_password")

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
    depends_on.append(f"{CLUSTER_RESTORE_FIELD_ID}/restore_user_jwt_auth")

    kopf.register(
        fn=ValidateRestoreCompleteSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
        )(
            snapshot=snapshot,
            restore_type=restore_type,
            tables=tables,
            partitions=partitions,
            sections=sections,
        ),
        id="validate_restore_complete",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_RESTORE_FIELD_ID}/validate_restore_complete")


def register_after_restore_handlers(
    namespace: str,
    name: str,
    status: kopf.Status,
    change_hash: str,
    context: dict,
    depends_on: list,
    repository: str,
):
    kopf.register(
        fn=AfterRestoreBackupSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
            run_on_dep_failures=True,
        )(status=status, repository=repository),
        id="after_restore_backup",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_RESTORE_FIELD_ID}/after_restore_backup")

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
        timeout=config.RESTORE_BACKUP_TIMEOUT,
    )
    depends_on.append(f"{CLUSTER_RESTORE_FIELD_ID}/after_cluster_update")

    # Ensure success notification is only sent after all other handlers
    # finished successfully.
    kopf.register(
        fn=SendSuccessNotificationSubHandler(
            namespace,
            name,
            change_hash,
            context,
            depends_on=depends_on.copy(),
        )(),
        id="send_success_notification",
        backoff=get_backoff(),
    )
    depends_on.append(f"{CLUSTER_RESTORE_FIELD_ID}/send_success_notification")
