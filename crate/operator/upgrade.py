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

import asyncio
import logging
from typing import Any, Dict, List

import kopf
from kubernetes_asyncio.client import AppsV1Api
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.config import config
from crate.operator.operations import get_total_nodes_count
from crate.operator.scale import get_container
from crate.operator.utils import crate, quorum
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.version import CrateVersion
from crate.operator.webhooks import WebhookEvent, WebhookStatus, WebhookUpgradePayload


def upgrade_command(old_command: List[str], total_data_nodes: int) -> List[str]:
    """
    Iterate through the ``old_command`` items and upgrade the setting's
    names where required in versions >= 4.7.

    Return the list making up the new CrateDB command.

    :param old_command: The command used to start-up CrateDB inside a
        Kubernetes container. This consists of the path to the Docker
        entrypoint script, the ``crate`` command argument and any additional
        settings.
    :param total_data_nodes: The number of data nodes that will be in the
        CrateDB cluster. From that, the quorum is derived as well.
    :return: The list forming the new CrateDB command.
    """
    new_command: List[str] = []
    for item in old_command:
        if item.startswith("-Cgateway.recover_after_nodes="):
            item = f"-Cgateway.recover_after_data_nodes={quorum(total_data_nodes)}"
        elif item.startswith("-Cgateway.expected_nodes="):
            item = f"-Cgateway.expected_data_nodes={total_data_nodes}"
        new_command.append(item)
    return new_command


async def update_statefulset(
    apps: AppsV1Api,
    namespace: str,
    sts_name: str,
    crate_image: str,
    old_version: str,
    new_version: str,
    data_nodes_count: int,
    logger: logging.Logger,
):
    await apps.patch_namespaced_stateful_set(
        namespace=namespace,
        name=sts_name,
        body={"spec": {"rollingUpdate": None, "updateStrategy": {"type": "OnDelete"}}},
    )
    body: Dict[str, Any] = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": "crate", "image": crate_image}],
                    "initContainers": [
                        {"name": "mkdir-heapdump", "image": crate_image}
                    ],
                }
            }
        }
    }
    if CrateVersion(old_version) < CrateVersion(
        config.GATEWAY_SETTINGS_DATA_NODES_VERSION
    ) and CrateVersion(new_version) >= CrateVersion(
        config.GATEWAY_SETTINGS_DATA_NODES_VERSION
    ):
        # upgrading to a version >= 4.7 requires changing the gateway settings
        # names and using the number of data nodes instead of total nodes.
        statefulset = await apps.read_namespaced_stateful_set(
            namespace=namespace, name=sts_name
        )
        crate_container = get_container(statefulset)
        new_command = upgrade_command(crate_container.command, data_nodes_count)
        logger.info("upgraded sts command: %s", new_command)
        body["spec"]["template"]["spec"]["containers"][0]["command"] = new_command
    await apps.patch_namespaced_stateful_set(
        namespace=namespace,
        name=sts_name,
        body=body,
    )


async def upgrade_cluster(
    apps: AppsV1Api,
    namespace: str,
    name: str,
    body: kopf.Body,
    old: kopf.Body,
    logger: logging.Logger,
):
    """
    Update the Docker image in all StatefulSets for the cluster.

    For the changes to take affect, the cluster needs to be restarted.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the ``CrateDB`` custom resource.
    :param body: The full body of the ``CrateDB`` custom resource per
        :class:`kopf.Body`.
    :param old: The old resource body. Required to get the old version.
    """
    old_version = old["spec"]["cluster"]["version"]
    crate_image = (
        body.spec["cluster"]["imageRegistry"] + ":" + body.spec["cluster"]["version"]
    )
    data_nodes_count = get_total_nodes_count(body.spec["nodes"], "data")

    updates = []
    if "master" in body.spec["nodes"]:
        updates.append(
            update_statefulset(
                apps,
                namespace,
                f"crate-master-{name}",
                crate_image,
                old_version,
                body.spec["cluster"]["version"],
                data_nodes_count,
                logger,
            )
        )
    updates.extend(
        [
            update_statefulset(
                apps,
                namespace,
                f"crate-data-{node_spec['name']}-{name}",
                crate_image,
                old_version,
                body.spec["cluster"]["version"],
                data_nodes_count,
                logger,
            )
            for node_spec in body.spec["nodes"]["data"]
        ]
    )

    await asyncio.gather(*updates)


class UpgradeSubHandler(StateBasedSubHandler):
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
            await upgrade_cluster(apps, namespace, name, body, old, logger)

        self.schedule_notification(
            WebhookEvent.UPGRADE,
            WebhookUpgradePayload(
                old_registry=old["spec"]["cluster"]["imageRegistry"],
                new_registry=body.spec["cluster"]["imageRegistry"],
                old_version=old["spec"]["cluster"]["version"],
                new_version=body.spec["cluster"]["version"],
            ),
            WebhookStatus.IN_PROGRESS,
        )
        await self.send_notifications(logger)


class AfterUpgradeSubHandler(StateBasedSubHandler):
    """
    A handler which depends on ``upgrade`` and ``restart`` having finished
    successfully and sends a success notification of the upgrade process.
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
            WebhookEvent.UPGRADE,
            WebhookUpgradePayload(
                old_registry=old["spec"]["cluster"]["imageRegistry"],
                new_registry=body.spec["cluster"]["imageRegistry"],
                old_version=old["spec"]["cluster"]["version"],
                new_version=body.spec["cluster"]["version"],
            ),
            WebhookStatus.SUCCESS,
        )
        await self.send_notifications(logger)
