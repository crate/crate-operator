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
import logging
import os

import kopf
from prometheus_client import start_http_server

from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    CLUSTER_UPDATE_ID,
    KOPF_STATE_STORE_PREFIX,
    LABEL_MANAGED_BY,
    LABEL_PART_OF,
    LABEL_USER_PASSWORD,
    RESOURCE_CONFIGMAP,
    RESOURCE_CRATEDB,
)
from crate.operator.handlers.handle_create_cratedb import create_cratedb
from crate.operator.handlers.handle_notify_external_ip_changed import (
    external_ip_changed,
)
from crate.operator.handlers.handle_ping_cratedb_status import ping_cratedb_status
from crate.operator.handlers.handle_restore_backup import restore_backup
from crate.operator.handlers.handle_update_allowed_cidrs import (
    update_service_allowed_cidrs,
)
from crate.operator.handlers.handle_update_backup_schedule import update_backup_schedule
from crate.operator.handlers.handle_update_cratedb import update_cratedb
from crate.operator.handlers.handle_update_user_password_secret import (
    update_user_password_secret,
)
from crate.operator.kube_auth import login_via_kubernetes_asyncio
from crate.operator.operations import update_sql_exporter_configmap
from crate.operator.restore_backup import is_valid_snapshot
from crate.operator.utils import crate
from crate.operator.webhooks import webhook_client

NO_VALUE = object()


@kopf.on.startup()
async def startup(settings: kopf.OperatorSettings, **_kwargs):
    config.load()
    if (
        config.WEBHOOK_PASSWORD is not None
        and config.WEBHOOK_URL is not None
        and config.WEBHOOK_USERNAME is not None
    ):
        webhook_client.configure(
            config.WEBHOOK_URL, config.WEBHOOK_USERNAME, config.WEBHOOK_PASSWORD
        )

    settings.persistence.diffbase_storage = kopf.AnnotationsDiffBaseStorage(
        prefix=KOPF_STATE_STORE_PREFIX, key="last", v1=False
    )
    settings.persistence.finalizer = f"operator.{API_GROUP}/finalizer"
    settings.persistence.progress_storage = kopf.AnnotationsProgressStorage(
        prefix=KOPF_STATE_STORE_PREFIX, v1=False
    )

    # Timeout passed along to the Kubernetes API as timeoutSeconds=x
    settings.watching.server_timeout = 300
    # Total number of seconds for a whole watch request per aiohttp:
    # https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientTimeout.total
    settings.watching.client_timeout = 300
    # Timeout for attempting to connect to the peer per aiohttp:
    # https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientTimeout.sock_connect
    settings.watching.connect_timeout = 30
    # Wait for that many seconds between watching events
    settings.watching.reconnect_backoff = 1

    # Only start the prometheus server in non-testing mode.
    if not config.TESTING:
        start_http_server(config.PROMETHEUS_PORT)


@kopf.on.login()
async def login(**kwargs):
    return await login_via_kubernetes_asyncio(**kwargs)


def annotation_filter():
    """
    If running in parallel testing mode, filter the cratedbs to only match those that
    have the "testing" annotation set to the current PID.

    This allows running several operators in parallel in each xdist worker.
    """
    if not config.TESTING or not config.PARALLEL_TESTING:
        return

    return {"testing": f"{os.getpid()}"}


@kopf.on.create(API_GROUP, "v1", RESOURCE_CRATEDB, annotations=annotation_filter())
@crate.on.error(error_handler=crate.send_create_failed_notification)
@crate.timeout(timeout=float(config.BOOTSTRAP_TIMEOUT))
async def cluster_create(
    namespace: str,
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    **_kwargs,
):
    """
    Handles creation of CrateDB Clusters.
    """
    await create_cratedb(namespace, meta, spec, patch, status, logger)


@kopf.on.update(
    API_GROUP,
    "v1",
    RESOURCE_CRATEDB,
    id=CLUSTER_UPDATE_ID,
    annotations=annotation_filter(),
)
@crate.on.error(error_handler=crate.send_update_failed_notification)
@crate.timeout(timeout=float(config.CLUSTER_UPDATE_TIMEOUT))
async def cluster_update(
    namespace: str,
    name: str,
    patch: kopf.Patch,
    status: kopf.Status,
    diff: kopf.Diff,
    started: datetime.datetime,
    **_kwargs,
):
    """
    Handles updates to the CrateDB resource.
    """
    await update_cratedb(namespace, name, patch, status, diff, started)


@kopf.on.update(
    "",
    "v1",
    "secrets",
    labels={LABEL_USER_PASSWORD: "true"},
)
async def secret_update(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    logger: logging.Logger,
    **_kwargs,
):
    """
    Handles changes to the password for a CrateDB cluster.
    """
    await update_user_password_secret(namespace, name, diff, logger)


@kopf.on.field(
    API_GROUP,
    "v1",
    RESOURCE_CRATEDB,
    field="spec.cluster.restoreSnapshot",
    when=is_valid_snapshot,
    annotations=annotation_filter(),
)
@crate.timeout(timeout=float(config.RESTORE_BACKUP_TIMEOUT))
async def cluster_restore(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    new: kopf.Body,
    patch: kopf.Patch,
    status: kopf.Status,
    started: datetime.datetime,
    logger: logging.Logger,
    **_kwargs,
):
    """
    Handles field changes which trigger restoring data from a backup.
    """
    await restore_backup(namespace, name, diff, new, patch, status, started, logger)


@kopf.on.field(
    API_GROUP,
    "v1",
    RESOURCE_CRATEDB,
    field="spec.cluster.allowedCIDRs",
    annotations=annotation_filter(),
)
async def service_cidr_changes(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    logger: logging.Logger,
    **_kwargs,
):
    """
    Handles updates to the list of allowed CIDRs, and updates the relevant k8s Service.
    """
    await update_service_allowed_cidrs(namespace, name, diff, logger)


@kopf.on.field(
    "",
    "v1",
    "services",
    labels={LABEL_PART_OF: "cratedb", LABEL_MANAGED_BY: "crate-operator"},
    field="status.loadBalancer.ingress",
    timeout=3600,
)
async def service_external_ip_update(
    name: str,
    namespace: str,
    diff: kopf.Diff,
    meta: dict,
    logger: logging.Logger,
    **_kwargs,
):
    """
    Handle new IP addresses being assigned to LoadBalancer-type services.

    This gets posted to the backend for further handling as a webhook
    (if webhooks are enabled).
    """
    await external_ip_changed(namespace, diff, meta, logger)


@kopf.on.field(
    API_GROUP,
    "v1",
    RESOURCE_CRATEDB,
    field="spec.backups.aws.cron",
    annotations=annotation_filter(),
)
async def service_backup_schedule_update(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    logger: logging.Logger,
    **_kwargs,
):
    """
    Handles updates to the backup schedule for AWS s3 backups.
    """
    await update_backup_schedule(namespace, name, diff, logger)


@kopf.timer(
    API_GROUP,
    "v1",
    RESOURCE_CRATEDB,
    interval=config.CRATEDB_STATUS_CHECK_INTERVAL,  # check interval
    idle=15,  # Initial delay, CrateDB very unlikely to be up in less than 15s
    annotations=annotation_filter(),
)
async def ping_cratedb(
    namespace: str,
    name: str,
    spec: kopf.Spec,
    patch: kopf.Patch,
    logger: logging.Logger,
    **_kwargs,
):
    await ping_cratedb_status(namespace, name, spec["cluster"]["name"], patch, logger)


@kopf.on.resume(
    "",
    "v1",
    RESOURCE_CONFIGMAP,
    labels={LABEL_PART_OF: "cratedb", LABEL_MANAGED_BY: "crate-operator"},
)
async def resume_sql_exporter_configmap(
    namespace: str,
    name: str,
    spec: kopf.Spec,
    logger: logging.Logger,
    **kwargs,
):
    """
    Updates sql-exporter configmap in all namespaces to latest.
    """
    await update_sql_exporter_configmap(namespace, name, logger)
