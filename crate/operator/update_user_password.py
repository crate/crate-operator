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
from crate.operator.constants import CloudProvider
from crate.operator.sql import execute_sql_via_crate_control, normalize_crate_control
from crate.operator.utils.formatting import b64decode
from crate.operator.utils.jwt import crate_version_supports_jwt
from crate.operator.utils.kubeapi import get_cratedb_resource
from crate.operator.utils.notifications import send_operation_progress_notification
from crate.operator.webhooks import WebhookAction, WebhookOperation, WebhookStatus


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
    cratedb = await get_cratedb_resource(namespace, cluster_id)
    crate_version = cratedb["spec"]["cluster"]["version"]
    exception_logger = logger.exception if config.TESTING else logger.error

    if config.CLOUD_PROVIDER == CloudProvider.OPENSHIFT:
        await _update_user_password_via_sidecar(
            namespace=namespace,
            name=cluster_id,
            cluster_id=cluster_id,
            username=username,
            password=password,
            cratedb=cratedb,
            crate_version=crate_version,
            logger=logger,
            exception_logger=exception_logger,
        )
    else:
        await _update_user_password_via_pod_exec(
            namespace=namespace,
            pod_name=pod_name,
            cluster_id=cluster_id,
            username=username,
            password=password,
            cratedb=cratedb,
            crate_version=crate_version,
            scheme=scheme,
            logger=logger,
            exception_logger=exception_logger,
        )

    await send_operation_progress_notification(
        namespace=namespace,
        name=cluster_id,
        message="Password updated successfully.",
        logger=logger,
        status=WebhookStatus.SUCCESS,
        operation=WebhookOperation.UPDATE,
        action=WebhookAction.PASSWORD_UPDATE,
    )


async def _update_user_password_via_sidecar(
    namespace: str,
    name: str,
    cluster_id: str,
    username: str,
    password: str,
    cratedb: dict,
    crate_version: str,
    logger: logging.Logger,
    exception_logger,
) -> None:
    """
    Update user password using the crate-control sidecar (OpenShift path).
    """
    iss = cratedb["spec"].get("grandCentral", {}).get("jwkUrl")
    if crate_version_supports_jwt(crate_version) and iss:
        stmt_reset_jwt = f'ALTER USER "{username}" SET (jwt = NULL)'
        try:
            logger.info("Resetting JWT config for user %s ...", username)
            result = normalize_crate_control(
                await execute_sql_via_crate_control(
                    namespace=namespace,
                    name=name,
                    sql=stmt_reset_jwt,
                    args=[],
                    logger=logger,
                )
            )
        except Exception as e:
            exception_logger("Failed to reset JWT for user %s: %s", username, str(e))
            raise _temporary_error()
        else:
            if (result.rowcount or 0) > 0:
                logger.info("... JWT reset success")
            else:
                logger.info("... JWT reset error: %s", result)
                raise _temporary_error()

        stmt_update = (
            f'ALTER USER "{username}" SET '
            f'(password = ?, jwt = {{"iss" = ?, "username" = ?, "aud" = ?}})'
        )
        args = [password, iss, username, cluster_id]
    else:
        stmt_update = f'ALTER USER "{username}" SET (password = ?)'
        args = [password]

    try:
        logger.info("Updating password for user %s ...", username)
        result = normalize_crate_control(
            await execute_sql_via_crate_control(
                namespace=namespace,
                name=name,
                sql=stmt_update,
                args=args,
                logger=logger,
            )
        )
    except Exception as e:
        exception_logger("Password update failed for user %s: %s", username, str(e))
        raise _temporary_error()
    else:
        if (result.rowcount or 0) > 0:
            logger.info("... password update success")
        else:
            logger.info("... password update error: %s", result)
            raise _temporary_error()


async def _update_user_password_via_pod_exec(
    namespace: str,
    pod_name: str,
    cluster_id: str,
    username: str,
    password: str,
    cratedb: dict,
    crate_version: str,
    scheme: str,
    logger: logging.Logger,
    exception_logger,
) -> None:
    """
    Update user password using pod_exec with curl (legacy path).

    Uses curl with a JSON body so that parameterised args are correctly
    substituted by CrateDB's HTTP API, matching the original behaviour.
    """

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

    async def pod_exec(cmd):
        async with WsApiClient() as ws_api_client:
            core_ws = CoreV1Api(ws_api_client)
            return await core_ws.connect_get_namespaced_pod_exec(
                namespace=namespace,
                name=pod_name,
                command=cmd,
                container="crate",
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )

    command_alter_user = get_curl_command(
        {
            "stmt": 'ALTER USER "{}" SET (password = $1)'.format(username),
            "args": [password],
        }
    )

    iss = cratedb["spec"].get("grandCentral", {}).get("jwkUrl")
    if crate_version_supports_jwt(crate_version) and iss:
        # For users with `jwt` and `password` set, we need to reset
        # `jwt` config first to be able to update the password.
        command_reset_user_jwt = get_curl_command(
            {
                "stmt": 'ALTER USER "{}" SET (jwt = NULL)'.format(username),
                "args": [],
            }
        )
        try:
            logger.info("Trying to reset user jwt config ...")
            result = await pod_exec(command_reset_user_jwt)
        except ApiException as e:
            exception_logger("... failed. Status: %s Reason: %s", e.status, e.reason)
            raise _temporary_error()
        except TemporaryError:
            raise
        except WSServerHandshakeError as e:
            exception_logger("... failed. Status: %s Message: %s", e.status, e.message)
            raise _temporary_error()
        except Exception as e:
            exception_logger(
                "... failed. Unexpected exception. Class: %s. Message: %s",
                type(e).__name__,
                str(e),
            )
            raise _temporary_error()
        else:
            if "rowcount" in result:
                logger.info("... success")
                command_alter_user = get_curl_command(
                    {
                        "stmt": (
                            'ALTER USER "{}" SET (password = $1, jwt = '
                            '{{"iss" = $2, "username" = $3, "aud" = $4}})'
                        ).format(username),
                        "args": [password, iss, username, cluster_id],
                    }
                )
            else:
                logger.info("... error. %s", result)
                raise _temporary_error()

    try:
        logger.info("Trying to update user password ...")
        result = await pod_exec(command_alter_user)
    except ApiException as e:
        exception_logger("... failed. Status: %s Reason: %s", e.status, e.reason)
        raise _temporary_error()
    except TemporaryError:
        raise
    except WSServerHandshakeError as e:
        exception_logger("... failed. Status: %s Message: %s", e.status, e.message)
        raise _temporary_error()
    except Exception as e:
        exception_logger(
            "... failed. Unexpected exception was raised. Class: %s. Message: %s",
            type(e).__name__,
            str(e),
        )
        raise _temporary_error()
    else:
        if "rowcount" in result:
            logger.info("... success")
        else:
            logger.info("... error. %s", result)
            raise _temporary_error()


def _temporary_error():
    return TemporaryError(delay=config.BOOTSTRAP_RETRY_DELAY)
