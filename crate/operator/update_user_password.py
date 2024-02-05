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

import json
import logging
from typing import List

from aiohttp.client_exceptions import WSServerHandshakeError
from kopf import TemporaryError
from kubernetes_asyncio.client import ApiException, CoreV1Api
from kubernetes_asyncio.stream import WsApiClient

from crate.operator.config import config
from crate.operator.utils.formatting import b64decode
from crate.operator.utils.notifications import send_operation_progress_notification
from crate.operator.webhooks import WebhookAction, WebhookOperation, WebhookStatus


# update_user_password(host, username, old_password, new_password)
async def update_user_password(
    namespace: str,
    cluster_id: str,
    pod_name: str,
    username: str,
    new_password: str,
    has_ssl: bool,
    logger: logging.Logger,
):
    """
    Update the password of a given ``user_spec`` in a CrateDB cluster.

    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param cluster_id: The ID of the CrateDB cluster.
    :param pod_name: The name of the pod to ``exec`` into.
    :param username: The username of the user of the CrateDB resource that
        should be updated.
    :param new_password: The new password of the user that should be updated.
    :param has_ssl: When ``True``, ``crash`` will establish a connection to
        the CrateDB cluster from inside the ``crate`` container using SSL/TLS.
        This must match how the cluster is configured, otherwise ``crash``
        won't be able to connect, since non-encrypted connections are forbidden
        when SSL/TLS is enabled, and encrypted connections aren't possible when
        no SSL/TLS is configured.
    """
    scheme = "https" if has_ssl else "http"
    password = b64decode(new_password)

    def get_curl_command(payload: dict) -> List[str]:
        return [
            "curl",
            "-k",
            "-X",
            "POST",
            f"{scheme}://localhost:4200/_sql",
            "-H",
            "Content-Type: application/json",
            "-d",
            json.dumps(payload),
            "-w",
            "\\n",
        ]

    command_alter_user = get_curl_command(
        {
            "stmt": 'ALTER USER "{}" SET (password = $1)'.format(username),
            "args": [password],
        }
    )
    exception_logger = logger.exception if config.TESTING else logger.error

    async with WsApiClient() as ws_api_client:
        core_ws = CoreV1Api(ws_api_client)
        try:
            logger.info("Trying to update user password ...")
            result = await core_ws.connect_get_namespaced_pod_exec(
                namespace=namespace,
                name=pod_name,
                command=command_alter_user,
                container="crate",
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
            if "rowcount" in result:
                logger.info("... success")
            else:
                logger.info("... error. %s", result)
                raise TemporaryError(delay=config.BOOTSTRAP_RETRY_DELAY)

            await send_operation_progress_notification(
                namespace=namespace,
                name=cluster_id,
                message="Password updated successfully.",
                logger=logger,
                status=WebhookStatus.SUCCESS,
                operation=WebhookOperation.UPDATE,
                action=WebhookAction.PASSWORD_UPDATE,
            )
        except ApiException as e:
            # We don't use `logger.exception()` to not accidentally include the
            # password in the log messages which might be part of the string
            # representation of the exception.
            exception_logger("... failed. Status: %s Reason: %s", e.status, e.reason)
            raise TemporaryError(delay=config.BOOTSTRAP_RETRY_DELAY)
        except WSServerHandshakeError as e:
            # We don't use `logger.exception()` to not accidentally include the
            # password in the log messages which might be part of the string
            # representation of the exception.
            exception_logger("... failed. Status: %s Message: %s", e.status, e.message)
            raise TemporaryError(delay=config.BOOTSTRAP_RETRY_DELAY)
        except TemporaryError:
            raise
        except Exception as e:
            exception_logger(
                "... failed. Unexpected exception was raised. Class: %s. Message: %s",
                type(e).__name__,
                str(e),
            )
            raise TemporaryError(delay=config.BOOTSTRAP_RETRY_DELAY)
