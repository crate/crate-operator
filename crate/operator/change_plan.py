import logging
from typing import Any
import kopf

from crate.operator.utils import crate
from crate.operator.utils.kopf import StateBasedSubHandler
from kubernetes_asyncio.client import AppsV1Api
from kubernetes_asyncio.client.api_client import ApiClient


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
        async with ApiClient() as api_client:
            apps = AppsV1Api(api_client)
            await change_cluster_plan(apps, namespace, name, body, old, logger)

        # self.schedule_notification(
        #     WebhookEvent.UPGRADE,
        #     WebhookUpgradePayload(
        #         old_registry=old["spec"]["cluster"]["imageRegistry"],
        #         new_registry=body.spec["cluster"]["imageRegistry"],
        #         old_version=old["spec"]["cluster"]["version"],
        #         new_version=body.spec["cluster"]["version"],
        #     ),
        #     WebhookStatus.IN_PROGRESS,
        # )
        await self.send_notifications(logger)


async def change_cluster_plan(
        apps: AppsV1Api,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
):
    # statefulset = await apps.read_namespaced_stateful_set(
    #     namespace=namespace, name=f"crate-master-{name}"
    # )

    new_cpu_limit = 1
    new_memory_limit = "5Gi"
    new_cpu_request = new_cpu_limit
    new_memory_request = new_memory_limit

    # Patch cpu limit
    body = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "crate",
                            "resources": {
                                "limits": {
                                    "cpu": new_cpu_limit,
                                    "memory": new_memory_limit,
                                },
                                "requests": {
                                    "cpu": new_cpu_request,
                                    "memory": new_memory_request,
                                }
                            }
                        }
                    ]
                }
            }
        }
    }
    await apps.patch_namespaced_stateful_set(
        namespace=namespace,
        name=f"crate-data-hot-{name}",
        body=body,
    )
    pass

    # Restart nodes one by one


# ("nodes", "data", "resources", "limits", "cpu"),
# ("nodes", "data", "resources", "limits", "memory"),
