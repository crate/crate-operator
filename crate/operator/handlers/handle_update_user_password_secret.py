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
import re

import kopf
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.config import config
from crate.operator.constants import API_GROUP, RESOURCE_CRATEDB
from crate.operator.update_user_password import update_user_password
from crate.operator.utils.kopf import subhandler_partial
from crate.operator.utils.kubeapi import get_host
from crate.operator.utils.notifications import send_operation_progress_notification
from crate.operator.webhooks import WebhookOperation, WebhookStatus


async def update_user_password_secret(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    logger: logging.Logger,
):
    # extract cluster_id from the secret name which has the
    # format `user-password-<cluster_id>-<ordinal>`
    cluster_id = re.sub(r"(^user-password-)|(\-\d+$)", "", name)

    await send_operation_progress_notification(
        namespace=namespace,
        name=cluster_id,
        message="Updating password.",
        logger=logger,
        status=WebhookStatus.IN_PROGRESS,
        operation=WebhookOperation.UPDATE,
    )
    async with ApiClient() as api_client:
        coapi = CustomObjectsApi(api_client)
        core = CoreV1Api(api_client)

        for operation, field_path, old_value, new_value in diff:
            custom_objects = await coapi.list_namespaced_custom_object(
                namespace=namespace,
                group=API_GROUP,
                version="v1",
                plural=RESOURCE_CRATEDB,
            )

            for crate_custom_object in custom_objects["items"]:
                host = await get_host(
                    core, namespace, crate_custom_object["metadata"]["name"]
                )

                for user_spec in crate_custom_object["spec"]["users"]:
                    expected_field_path = (
                        "data",
                        user_spec["password"]["secretKeyRef"]["key"],
                    )
                    if (
                        user_spec["password"]["secretKeyRef"]["name"] == name
                        and field_path == expected_field_path
                    ):
                        kopf.register(
                            fn=subhandler_partial(
                                update_user_password,
                                host,
                                user_spec["name"],
                                old_value,
                                new_value,
                                namespace,
                                cluster_id,
                                logger,
                            ),
                            id=f"update-{crate_custom_object['metadata']['name']}-{user_spec['name']}",  # noqa
                            timeout=config.BOOTSTRAP_TIMEOUT,
                        )
