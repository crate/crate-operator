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

import kopf
from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.cratedb import connection_factory, get_healthiness
from crate.operator.operations import get_desired_nodes_count
from crate.operator.prometheus import PrometheusClusterStatus, report_cluster_status
from crate.operator.utils.kubeapi import get_host, get_system_user_password
from crate.operator.webhooks import (
    WebhookClusterHealthPayload,
    WebhookEvent,
    WebhookStatus,
    webhook_client,
)

HEALTHINESS_TO_STATUS = {
    1: PrometheusClusterStatus.GREEN,
    2: PrometheusClusterStatus.YELLOW,
    3: PrometheusClusterStatus.RED,
}

CLUSTER_STATUS_KEY = "crateDBStatus"


async def ping_cratedb_status(
    namespace: str,
    name: str,
    cluster_name: str,
    patch: kopf.Patch,
    logger: logging.Logger,
) -> None:
    desired_instances = await get_desired_nodes_count(namespace, name)
    # When the cluster is meant to be suspended do not ping it
    if desired_instances == 0:
        patch.status[CLUSTER_STATUS_KEY] = {"health": "SUSPENDED"}
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

        report_cluster_status(name, cluster_name, namespace, status)
        patch.status[CLUSTER_STATUS_KEY] = {"health": status.name}

        await webhook_client.send_notification(
            namespace,
            name,
            WebhookEvent.HEALTH,
            WebhookClusterHealthPayload(status=status.name),
            WebhookStatus.SUCCESS,
            logger,
        )
