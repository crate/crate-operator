import logging

import kopf
from kopf import DiffItem
from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.api_client import ApiClient


async def update_service_allowed_cidrs(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    logger: logging.Logger,
):
    change: DiffItem = diff[0]
    logger.info(f"Updating load balancer source ranges to {change.new}")

    async with ApiClient() as api_client:
        core = CoreV1Api(api_client)
        # This also runs on creation events, so we want to double check that the service
        # exists before attempting to do anything.
        services = await core.list_namespaced_service(namespace=namespace)
        service = next(
            (svc for svc in services.items if svc.metadata.name == f"crate-{name}"),
            None,
        )
        if not service:
            return

        await core.patch_namespaced_service(
            name=f"crate-{name}",
            namespace=namespace,
            body={"spec": {"loadBalancerSourceRanges": change.new}},
        )
