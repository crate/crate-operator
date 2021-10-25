import logging

from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.cratedb import HEALTHINESS, connection_factory, get_healthiness
from crate.operator.utils.kubeapi import get_host, get_system_user_password
from crate.operator.webhooks import (
    WebhookClusterHealthPayload,
    WebhookEvent,
    WebhookStatus,
    webhook_client,
)


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

        async with conn_factory() as conn:
            async with conn.cursor() as cursor:
                try:
                    healthiness = await get_healthiness(cursor)
                    # If there are no tables in the cluster, get_healthiness returns
                    # none: default to `Green`, as cluster is reachable
                    status = HEALTHINESS.get(healthiness, "GREEN")
                except Exception as e:
                    logger.warning("Failed to ping cluster.", exc_info=e)
                    status = "UNREACHABLE"

        await webhook_client.send_notification(
            namespace,
            name,
            WebhookEvent.HEALTH,
            WebhookClusterHealthPayload(status=status),
            WebhookStatus.SUCCESS,
            logger,
        )
