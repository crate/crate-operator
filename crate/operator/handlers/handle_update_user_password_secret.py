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
import re

import kopf
from kubernetes_asyncio.client import CustomObjectsApi
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.config import config
from crate.operator.constants import API_GROUP, RESOURCE_CRATEDB
from crate.operator.update_user_password import update_user_password
from crate.operator.utils.kopf import subhandler_partial
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

        for operation, field_path, old_value, new_value in diff:
            custom_objects = await coapi.list_namespaced_custom_object(
                namespace=namespace,
                group=API_GROUP,
                version="v1",
                plural=RESOURCE_CRATEDB,
            )

            for crate_custom_object in custom_objects["items"]:
                for user_spec in crate_custom_object["spec"]["users"]:
                    expected_field_path = (
                        "data",
                        user_spec["password"]["secretKeyRef"]["key"],
                    )
                    if (
                        user_spec["password"]["secretKeyRef"]["name"] == name
                        and field_path == expected_field_path
                    ):
                        has_master_nodes = (
                            "master" in crate_custom_object["spec"]["nodes"]
                        )
                        if has_master_nodes:
                            pod_name = f"crate-master-{cluster_id}-0"
                        else:
                            node_name = crate_custom_object["spec"]["nodes"]["data"][0][
                                "name"
                            ]
                            pod_name = f"crate-data-{node_name}-{cluster_id}-0"
                        kopf.register(
                            fn=subhandler_partial(
                                update_user_password,
                                namespace,
                                cluster_id,
                                pod_name,
                                user_spec["name"],
                                new_value,
                                "ssl" in crate_custom_object["spec"]["cluster"],
                                logger,
                            ),
                            id=f"update-{crate_custom_object['metadata']['name']}-{user_spec['name']}",  # noqa
                            timeout=config.BOOTSTRAP_TIMEOUT,
                        )
