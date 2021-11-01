import enum
import logging
import time

from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.cratedb import connection_factory, get_healthiness
from crate.operator.prometheus import cluster_last_seen_gauge, cluster_status_gauge
from crate.operator.utils.kubeapi import get_host, get_system_user_password
from crate.operator.webhooks import (
    WebhookClusterHealthPayload,
    WebhookEvent,
    WebhookStatus,
    webhook_client,
)


class PrometheusStatus(enum.Enum):
    GREEN = 0
    YELLOW = 1
    RED = 2
    UNREACHABLE = 3


HEALTHINESS_TO_STATUS = {
    1: PrometheusStatus.GREEN,
    2: PrometheusStatus.YELLOW,
    3: PrometheusStatus.RED,
}


async def ping_cratedb_status(
    namespace: str,
    name: str,
    logger: logging.Logger,
) -> None:
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
                        healthiness, PrometheusStatus.GREEN
                    )
        except Exception as e:
            logger.warning("Failed to ping cluster.", exc_info=e)
            status = PrometheusStatus.UNREACHABLE

        cluster_status_gauge.labels(cluster_id=name).set(status.value)
        if status != PrometheusStatus.UNREACHABLE:
            cluster_last_seen_gauge.labels(cluster_id=name).set(int(time.time()))

        await webhook_client.send_notification(
            namespace,
            name,
            WebhookEvent.HEALTH,
            WebhookClusterHealthPayload(status=status.name),
            WebhookStatus.SUCCESS,
            logger,
        )
