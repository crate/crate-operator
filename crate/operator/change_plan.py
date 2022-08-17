import logging
from typing import Any

import kopf
from kubernetes_asyncio.client import AppsV1Api
from kubernetes_asyncio.client.api_client import ApiClient
from crate.operator.webhooks import WebhookChangePlanPayload, WebhookEvent, WebhookStatus

from crate.operator.utils import crate
from crate.operator.utils.kopf import StateBasedSubHandler


class ChangePlanSubHandler(StateBasedSubHandler):
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
        webhook_payload = generate_webhook_changeplan_payload(old, body)
        async with ApiClient() as api_client:
            apps = AppsV1Api(api_client)
            await change_cluster_plan(apps, namespace, name, webhook_payload, logger)

        self.schedule_notification(
            WebhookEvent.PLAN_CHANGED,
            webhook_payload,
            WebhookStatus.IN_PROGRESS,
        )
        await self.send_notifications(logger)


class AfterChangePlanSubHandler(StateBasedSubHandler):
    """
    A handler which depends on``restart`` having finishe successfully and sends a
    success notification of the change plan process.
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
            WebhookEvent.PLAN_CHANGED,
            generate_webhook_changeplan_payload(old, body),
            WebhookStatus.SUCCESS,
        )
        await self.send_notifications(logger)


def generate_webhook_changeplan_payload(old, body):
    old_data = old["spec"]["nodes"]["data"][0].get("resources", {})
    new_data = body["spec"]["nodes"]["data"][0].get("resources", {})
    return WebhookChangePlanPayload(
        old_cpu_limit=old_data.get("limits", {}).get("cpu"),
        old_memory_limit=old_data.get("limits", {}).get("memory"),
        old_cpu_request=old_data.get("requests", {}).get("cpu"),
        old_memory_request=old_data.get("requests", {}).get("memory"),
        new_cpu_limit=new_data.get("limits", {}).get("cpu"),
        new_memory_limit=new_data.get("limits", {}).get("memory"),
        new_cpu_request=new_data.get("requests", {}).get("cpu"),
        new_memory_request=new_data.get("requests", {}).get("memory"),
    )


async def change_cluster_plan(
    apps: AppsV1Api,
    namespace: str,
    name: str,
    plan_change_data: WebhookChangePlanPayload,
    logger: logging.Logger,
):
    """
    Patches the statefulset with the new cpu and memory requests and limits.
    """
    # Patch cpu/memory limit/request
    body = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "crate",
                            "resources": {
                                "limits": {
                                    "cpu": plan_change_data["new_cpu_limit"],
                                    "memory": plan_change_data["new_memory_limit"],
                                },
                                "requests": {
                                    "cpu": plan_change_data["new_cpu_request"]
                                    or plan_change_data["new_cpu_limit"],
                                    "memory": plan_change_data["new_memory_request"]
                                    or plan_change_data["new_memory_limit"],
                                },
                            },
                        }
                    ]
                }
            }
        }
    }

    # Note only the stateful set is updated. Pods will become updated on restart
    sts_name = f"crate-data-hot-{name}"
    await apps.patch_namespaced_stateful_set(
        namespace=namespace,
        name=sts_name,
        body=body,
    )
    logger.info("updated the statefulset with name %s with body: %s", sts_name, body)
    pass
