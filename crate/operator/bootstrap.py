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
from typing import Any, Dict, List, Optional

from aiohttp.client_exceptions import WSServerHandshakeError
from kopf import TemporaryError
from kubernetes_asyncio.client import ApiException, CoreV1Api
from kubernetes_asyncio.stream import WsApiClient

from crate.operator.config import config
from crate.operator.constants import CONNECT_TIMEOUT, GC_USERNAME, SYSTEM_USERNAME
from crate.operator.cratedb import create_user, get_connection
from crate.operator.utils import crate
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.kubeapi import (
    ensure_user_password_label,
    get_host,
    get_system_user_password,
    resolve_secret_key_ref,
)
from crate.operator.utils.notifications import send_operation_progress_notification
from crate.operator.webhooks import WebhookAction, WebhookOperation, WebhookStatus


async def bootstrap_system_user(
    core: CoreV1Api,
    namespace: str,
    name: str,
    master_node_pod: str,
    has_ssl: bool,
    logger: logging.Logger,
) -> None:
    """
    Exec into to a CrateDB container and create the system user.

    When starting up a cluster, the operator doesn't have a system user yet
    that it could use. The operator will therefore ``exec`` into the ``crate``
    container in the ``master_node_pod`` and attempt to create a user and grant
    it all privileges.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the ``CrateDB`` custom resource. Used to lookup
        the password for the system user created during deployment.
    :param master_node_pod: The pod name of one of the eligible master nodes in
        the cluster. Used to ``exec`` into.
    :param has_ssl: When ``True``, ``crash`` will establish a connection to
        the CrateDB cluster from inside the ``crate`` container using SSL/TLS.
        This must match how the cluster is configured, otherwise ``crash``
        won't be able to connect, since non-encrypted connections are forbidden
        when SSL/TLS is enabled, and encrypted connections aren't possible when
        no SSL/TLS is configured.
    """
    scheme = "https" if has_ssl else "http"
    password = await get_system_user_password(core, namespace, name)

    def get_curl_command(payload: dict) -> List[str]:
        return [
            "curl",
            "-s",
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

    command_create_user = get_curl_command(
        {
            "stmt": 'CREATE USER "{}" WITH (password = $1)'.format(SYSTEM_USERNAME),
            "args": [password],
        }
    )
    command_alter_user = get_curl_command(
        {
            "stmt": 'ALTER USER "{}" SET (password = $1)'.format(SYSTEM_USERNAME),
            "args": [password],
        }
    )
    command_grant = get_curl_command(
        {"stmt": 'GRANT ALL PRIVILEGES TO "{}" '.format(SYSTEM_USERNAME)}
    )
    exception_logger = logger.exception if config.TESTING else logger.error

    async def pod_exec(cmd):
        return await core_ws.connect_get_namespaced_pod_exec(
            namespace=namespace,
            name=master_node_pod,
            command=cmd,
            container="crate",
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )

    needs_update = False
    async with WsApiClient() as ws_api_client:
        core_ws = CoreV1Api(ws_api_client)
        try:
            logger.info("Trying to create system user ...")
            result = await pod_exec(command_create_user)
        except ApiException as e:
            # We don't use `logger.exception()` to not accidentally include the
            # password in the log messages which might be part of the string
            # representation of the exception.
            exception_logger("... failed. Status: %s Reason: %s", e.status, e.reason)
            raise _temporary_error()
        except WSServerHandshakeError as e:
            # We don't use `logger.exception()` to not accidentally include the
            # password in the log messages which might be part of the string
            # representation of the exception.
            exception_logger("... failed. Status: %s Message: %s", e.status, e.message)
            raise _temporary_error()
        else:
            if "rowcount" in result:
                logger.info("... success")
            elif "AlreadyExistsException" in result:
                needs_update = True
                logger.info("... success. Already present")
            else:
                logger.info("... error. %s", result)
                raise _temporary_error()

        if needs_update:
            try:
                logger.info("Trying to update system user password ...")
                result = await pod_exec(command_alter_user)
            except ApiException as e:
                # We don't use `logger.exception()` to not accidentally include the
                # password in the log messages which might be part of the string
                # representation of the exception.
                exception_logger(
                    "... failed. Status: %s Reason: %s", e.status, e.reason
                )
                raise _temporary_error()
            except WSServerHandshakeError as e:
                # We don't use `logger.exception()` to not accidentally include the
                # password in the log messages which might be part of the string
                # representation of the exception.
                exception_logger(
                    "... failed. Status: %s Message: %s", e.status, e.message
                )
                raise _temporary_error()
            else:
                if "rowcount" in result:
                    logger.info("... success")
                else:
                    logger.info("... error. %s", result)
                    raise _temporary_error()

        try:
            logger.info("Trying to grant system user all privileges ...")
            result = await pod_exec(command_grant)
        except (ApiException, WSServerHandshakeError):
            logger.exception("... failed")
            raise _temporary_error()
        else:
            if "rowcount" in result:
                logger.info("... success")
            else:
                logger.info("... error. %s", result)
                raise _temporary_error()


async def bootstrap_gc_admin_user(core: CoreV1Api, namespace: str, name: str):
    """
    Create the gc_admin user, which is used by Grand Central to run
    queries against CrateDB.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the ``CrateDB`` custom resource. Used to lookup
        the password for the system user created during deployment.
    """
    host = await get_host(core, namespace, name)
    password = await get_system_user_password(core, namespace, name)
    async with get_connection(host, password, timeout=CONNECT_TIMEOUT) as conn:
        async with conn.cursor() as cursor:
            password = await resolve_secret_key_ref(
                core, namespace, {"key": "password", "name": f"user-gc-{name}"}
            )
            await create_user(cursor, namespace, name, GC_USERNAME, password)


async def bootstrap_users(
    core: CoreV1Api, namespace: str, name: str, users: List[Dict[str, Any]]
):
    """
    Create all users in the CrateDB clusters that are defined in the cluster
    spec.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the ``CrateDB`` custom resource. Used to lookup
        the password for the system user created during deployment.
    :param users: A list of user definitions containing the username and the
        secret key reference to their password.
    """
    host = await get_host(core, namespace, name)
    password = await get_system_user_password(core, namespace, name)
    async with get_connection(host, password, timeout=CONNECT_TIMEOUT) as conn:
        async with conn.cursor() as cursor:
            for user_spec in users:
                username = user_spec["name"]
                secret_key_ref = user_spec["password"]["secretKeyRef"]
                password = await resolve_secret_key_ref(
                    core,
                    namespace,
                    secret_key_ref,
                )
                await ensure_user_password_label(
                    core, namespace, secret_key_ref["name"]
                )
                await create_user(cursor, namespace, name, username, password)


async def create_users(
    core: CoreV1Api,
    namespace: str,
    name: str,
    master_node_pod: str,
    has_ssl: bool,
    users: Optional[List[Dict[str, Any]]],
    logger: logging.Logger,
):
    """
    Create the system user, and any additional configured users.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the ``CrateDB`` custom resource. Used to lookup
        the password for the system user created during deployment.
    :param master_node_pod: The pod name of one of the eligible master nodes in
        the cluster. Used to ``exec`` into.
    :param has_ssl: When ``True``, ``crash`` will establish a connection to
        the CrateDB cluster from inside the ``crate`` container using SSL/TLS.
        This must match how the cluster is configured, otherwise ``crash``
        won't be able to connect, since non-encrypted connections are forbidden
        when SSL/TLS is enabled, and encrypted connections aren't possible when
        no SSL/TLS is configured.
    :param users: An optional list of user definitions containing the username
        and the secret key reference to their password.
    """
    await send_operation_progress_notification(
        namespace=namespace,
        name=name,
        message="Cluster creation started. Waiting for the node(s) to be created "
        "and creating other required resources.",
        logger=logger,
        status=WebhookStatus.IN_PROGRESS,
        operation=WebhookOperation.CREATE,
        action=WebhookAction.CREATE,
    )
    await bootstrap_system_user(core, namespace, name, master_node_pod, has_ssl, logger)
    if users:
        await bootstrap_users(core, namespace, name, users)


def _temporary_error():
    return TemporaryError(delay=config.BOOTSTRAP_RETRY_DELAY)


class CreateUsersSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_create_failed_notification)
    @crate.timeout(timeout=float(config.BOOTSTRAP_TIMEOUT))
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        master_node_pod: str,
        has_ssl: bool,
        users: Optional[List[Dict[str, Any]]],
        logger: logging.Logger,
        **kwargs: Any,
    ):
        async with GlobalApiClient() as api_client:
            core = CoreV1Api(api_client)
            await create_users(
                core, namespace, name, master_node_pod, has_ssl, users, logger
            )
        await send_operation_progress_notification(
            namespace=namespace,
            name=name,
            message="The cluster has been created successfully.",
            logger=logger,
            status=WebhookStatus.SUCCESS,
            operation=WebhookOperation.CREATE,
            action=WebhookAction.CREATE,
        )
