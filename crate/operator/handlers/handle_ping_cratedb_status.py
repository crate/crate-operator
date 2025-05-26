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
from asyncio import TimeoutError

import kopf
from aiohttp.client_exceptions import ClientConnectorError
from kubernetes_asyncio.client import CoreV1Api

from crate.operator.cratedb import connection_factory, get_healthiness
from crate.operator.prometheus import PrometheusClusterStatus, report_cluster_status
from crate.operator.utils.k8s_api_client import GlobalApiClient
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
    desired_instances: int,
    patch: kopf.Patch,
    logger: logging.Logger,
) -> None:
    # When the cluster is meant to be suspended do not ping it
    if desired_instances == 0:
        patch.status[CLUSTER_STATUS_KEY] = {"health": "SUSPENDED"}
        return

    try:
        async with GlobalApiClient() as api_client:
            core = CoreV1Api(api_client)
            host = await get_host(core, namespace, name)
            password = await get_system_user_password(core, namespace, name)
        conn_factory = connection_factory(host, password)
        connection = conn_factory()

        async with connection as conn:
            async with conn.cursor() as cursor:
                healthiness = await get_healthiness(cursor)
                # If there are no tables in the cluster, get_healthiness returns
                # none: default to `Green`, as cluster is reachable
                status = HEALTHINESS_TO_STATUS.get(
                    healthiness, PrometheusClusterStatus.GREEN
                )
    except Exception as e:
        if isinstance(e, ClientConnectorError):
            error_msg = (
                "Transient Kubernetes API connection error during health check: %s"
            )
            logger.warning(error_msg, e)
        elif isinstance(e, TimeoutError):
            error_msg = "Timeout while connecting to CrateDB during health check: %s"
            logger.warning(error_msg, e)
        else:
            logger.warning(
                "Unexpected error during CrateDB health check.", exc_info=True
            )

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
