# CrateDB Kubernetes Operator
# Copyright (C) 2020 Crate.IO GmbH
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

import logging

import kopf

from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    CLUSTER_UPDATE_ID,
    KOPF_STATE_STORE_PREFIX,
    LABEL_MANAGED_BY,
    LABEL_PART_OF,
    LABEL_USER_PASSWORD,
    RESOURCE_CRATEDB,
)
from crate.operator.handlers.handle_create_cratedb import create_cratedb
from crate.operator.handlers.handle_migrate_discovery_service import (
    migrate_discovery_service,
)
from crate.operator.handlers.handle_migrate_user_password_label import (
    migrate_user_password_label,
)
from crate.operator.handlers.handle_notify_external_ip_changed import (
    external_ip_changed,
)
from crate.operator.handlers.handle_update_allowed_cidrs import (
    update_service_allowed_cidrs,
)
from crate.operator.handlers.handle_update_cratedb import update_cratedb
from crate.operator.handlers.handle_update_user_password_secret import (
    update_user_password_secret,
)
from crate.operator.kube_auth import login_via_kubernetes_asyncio
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


@kopf.on.login()
async def login(**kwargs):
    return await login_via_kubernetes_asyncio(**kwargs)


@kopf.on.create(API_GROUP, "v1", RESOURCE_CRATEDB)
async def cluster_create(
    namespace: str, meta: kopf.Meta, spec: kopf.Spec, logger: logging.Logger, **_kwargs
):
    """
    Handles creation of CrateDB Clusters.
    """
    await create_cratedb(namespace, meta, spec, logger)


@kopf.on.update(API_GROUP, "v1", RESOURCE_CRATEDB, id=CLUSTER_UPDATE_ID)
async def cluster_update(
    namespace: str,
    name: str,
    patch: kopf.Patch,
    status: kopf.Status,
    diff: kopf.Diff,
    **_kwargs,
):
    """
    Handles updates to the CrateDB resource.
    """
    await update_cratedb(namespace, name, patch, status, diff)


@kopf.on.resume(API_GROUP, "v1", RESOURCE_CRATEDB)
async def update_cratedb_resource(
    namespace: str,
    spec: kopf.Spec,
    **_kwargs,
):
    """
    Migrates CrateDB secrets that do not have the LABEL_USER_PASSWORD label on them.

    This handler here is for backwards-compatibility and will be removed in the next
    major operator release.
    """
    await migrate_user_password_label(namespace, spec)


@kopf.on.update("", "v1", "secrets", labels={LABEL_USER_PASSWORD: "true"})
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


@kopf.on.field(API_GROUP, "v1", RESOURCE_CRATEDB, field="spec.cluster.allowedCIDRs")
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


@kopf.on.resume(
    "",
    "v1",
    "services",
    labels={LABEL_PART_OF: "cratedb", LABEL_MANAGED_BY: "crate-operator"},
)
async def update_discovery_service_handler(
    namespace: str,
    name: str,
    logger: logging.Logger,
    **_kwargs,
):
    """
    Detects any crate-discovery services that do not have an HTTP port and updates them.

    The HTTP port is required for internal communications with CrateDB, since the
    "crate" service might have IP restrictions that make it unreachable by the operator.

    This handler here is for backwards-compatibility and will be removed in the next
    major operator release.
    """
    await migrate_discovery_service(namespace, name, logger)


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
    await external_ip_changed(name, namespace, diff, meta, logger)
