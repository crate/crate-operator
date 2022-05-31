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

import logging

from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.cratedb import connection_factory, get_healthiness
from crate.operator.prometheus import PrometheusClusterStatus, report_cluster_status
from crate.operator.utils.kubeapi import get_host, get_system_user_password
from crate.operator.webhooks import (
    WebhookClusterHealthPayload,
    WebhookEvent,
    WebhookStatus,
    webhook_client,
)
from crate.operator.operations import get_desired_nodes_count

HEALTHINESS_TO_STATUS = {
    1: PrometheusClusterStatus.GREEN,
    2: PrometheusClusterStatus.YELLOW,
    3: PrometheusClusterStatus.RED,
}


async def ping_cratedb_status(
    namespace: str,
    name: str,
    logger: logging.Logger,
) -> None:
    desired_instances = await get_desired_nodes_count(namespace, name)
    # When the cluster is meant to be suspended do not ping it
    if desired_instances == 0:
        return

    async with ApiClient() as api_client:
        core = CoreV1Api(api_client)
        host = await get_host(core, namespace, name)
        password = await get_system_user_password(core, namespace, name)
        conn_factory = connection_factory(host, password)

        try:
            async with conn_factory() as conn:
                async with conn.cursor() as cursor:
                    healthiness = await get_healthiness(cursor)
                    # If there are no tables in the cluster, get_healthiness returns
                    # none: default to `Green`, as cluster is reachable
                    status = HEALTHINESS_TO_STATUS.get(
                        healthiness, PrometheusClusterStatus.GREEN
                    )
        except Exception as e:
            logger.warning("Failed to ping cluster.", exc_info=e)
            status = PrometheusClusterStatus.UNREACHABLE

        report_cluster_status(name, status)

        await webhook_client.send_notification(
            namespace,
            name,
            WebhookEvent.HEALTH,
            WebhookClusterHealthPayload(status=status.name),
            WebhookStatus.SUCCESS,
            logger,
        )
