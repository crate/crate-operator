import logging
from typing import Any

import kopf
from kubernetes_asyncio.client import AppsV1Api
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.create import (
    get_statefulset_affinity,
    get_statefulset_env_crate_heap,
)
from crate.operator.utils import crate
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.webhooks import (
    WebhookChangeComputePayload,
    WebhookEvent,
    WebhookStatus,
)


class ChangeComputeSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        webhook_payload = generate_change_compute_payload(old, body)
        async with ApiClient() as api_client:
            apps = AppsV1Api(api_client)
            await change_cluster_compute(apps, namespace, name, webhook_payload, logger)

        self.schedule_notification(
            WebhookEvent.COMPUTE_CHANGED,
            webhook_payload,
            WebhookStatus.IN_PROGRESS,
        )
        await self.send_notifications(logger)


class AfterChangeComputeSubHandler(StateBasedSubHandler):
    """
    A handler which depends on``restart`` having finished successfully and sends a
    success notification of the change compute process.
    """

    @crate.on.error(error_handler=crate.send_update_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        self.schedule_notification(
            WebhookEvent.COMPUTE_CHANGED,
            generate_change_compute_payload(old, body),
            WebhookStatus.SUCCESS,
        )
        await self.send_notifications(logger)


def generate_change_compute_payload(old, body):
    old_data = old["spec"]["nodes"]["data"][0].get("resources", {})
    new_data = body["spec"]["nodes"]["data"][0].get("resources", {})
    return WebhookChangeComputePayload(
        old_cpu_limit=old_data.get("limits", {}).get("cpu"),
        old_memory_limit=old_data.get("limits", {}).get("memory"),
        old_cpu_request=old_data.get("requests", {}).get("cpu"),
        old_memory_request=old_data.get("requests", {}).get("memory"),
        old_heap_ratio=old_data.get("heapRatio"),
        new_cpu_limit=new_data.get("limits", {}).get("cpu"),
        new_memory_limit=new_data.get("limits", {}).get("memory"),
        new_cpu_request=new_data.get("requests", {}).get("cpu"),
        new_memory_request=new_data.get("requests", {}).get("memory"),
        new_heap_ratio=new_data.get("heapRatio"),
    )


async def change_cluster_compute(
    apps: AppsV1Api,
    namespace: str,
    name: str,
    compute_change_data: WebhookChangeComputePayload,
    logger: logging.Logger,
):
    """
    Patches the statefulset with the new cpu and memory requests and limits.
    """
    body = generate_body_patch(name, compute_change_data, logger)

    # Note only the stateful set is updated. Pods will become updated on restart
    sts_name = f"crate-data-hot-{name}"
    await apps.patch_namespaced_stateful_set(
        namespace=namespace,
        name=sts_name,
        body=body,
    )
    logger.info("updated the statefulset with name %s with body: %s", sts_name, body)
    pass


def generate_body_patch(
    name: str,
    compute_change_data: WebhookChangeComputePayload,
    logger: logging.Logger,
) -> dict:
    """
    Generates a dict representing the patch that will be applied to the statefulset.
    That patch modifies cpu/memory requests/limits based on compute_change_data.
    It also patches affinity as needed based on the existence or not of requests data.
    """
    node_spec = {
        "name": "crate",
        "env": [
            get_statefulset_env_crate_heap(
                memory=compute_change_data["new_memory_limit"],
                heap_ratio=compute_change_data["new_heap_ratio"],
            )
        ],
        "resources": {
            "limits": {
                "cpu": compute_change_data["new_cpu_limit"],
                "memory": compute_change_data["new_memory_limit"],
            },
            "requests": {
                "cpu": compute_change_data.get(
                    "new_cpu_request",
                    compute_change_data["new_cpu_limit"],
                ),
                "memory": compute_change_data.get(
                    "new_memory_request",
                    compute_change_data["new_memory_limit"],
                ),
            },
        },
    }
    body = {
        "spec": {
            "template": {
                "spec": {
                    "affinity": get_statefulset_affinity(name, logger, node_spec),
                    "containers": [node_spec],
                }
            }
        }
    }

    return body


def has_compute_changed(old_spec, new_spec) -> bool:
    return (
        old_spec.get("resources", {}).get("limits", {}).get("cpu")
        != new_spec.get("resources", {}).get("limits", {}).get("cpu")
        or old_spec.get("resources", {}).get("requests", {}).get("cpu")
        != new_spec.get("resources", {}).get("requests", {}).get("cpu")
        or old_spec.get("resources", {}).get("limits", {}).get("memory")
        != new_spec.get("resources", {}).get("limits", {}).get("memory")
        or old_spec.get("resources", {}).get("requests", {}).get("memory")
        != new_spec.get("resources", {}).get("requests", {}).get("memory")
    )
