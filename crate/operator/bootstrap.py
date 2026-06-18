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

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

from aiohttp.client_exceptions import WSServerHandshakeError
from kopf import TemporaryError
from kubernetes_asyncio.client import ApiException, CoreV1Api
from kubernetes_asyncio.stream import WsApiClient

from crate.operator.config import config
from crate.operator.constants import (
    CONNECT_TIMEOUT,
    GC_USER_SECRET_NAME,
    GC_USERNAME,
    SYSTEM_USERNAME,
    CloudProvider,
)
from crate.operator.cratedb import create_user, get_connection
from crate.operator.sql import execute_sql_via_crate_control
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
    password = await get_system_user_password(core, namespace, name)

    command_create_user: Dict[str, Any] = {
        "stmt": f'CREATE USER "{SYSTEM_USERNAME}" WITH (password = ?)',
        "args": [password],
    }

    command_alter_user: Dict[str, Any] = {
        "stmt": f'ALTER USER "{SYSTEM_USERNAME}" SET (password = ?)',
        "args": [password],
    }

    command_grant: Dict[str, Any] = {
        "stmt": f'GRANT ALL PRIVILEGES TO "{SYSTEM_USERNAME}"',
        "args": [],
    }

    exception_logger = logger.exception if config.TESTING else logger.error

    if config.CLOUD_PROVIDER == CloudProvider.OPENSHIFT:
        await _bootstrap_user_via_sidecar(
            namespace,
            name,
            command_create_user,
            command_alter_user,
            command_grant,
            logger,
            exception_logger,
        )
    else:
        scheme = "https" if has_ssl else "http"
        await _bootstrap_user_via_pod_exec(
            namespace,
            master_node_pod,
            scheme,
            command_create_user,
            command_alter_user,
            command_grant,
            logger,
            exception_logger,
        )


async def _bootstrap_user_via_sidecar(
    namespace: str,
    name: str,
    command_create_user: Dict[str, Any],
    command_alter_user: Dict[str, Any],
    command_grant: Dict[str, Any],
    logger: logging.Logger,
    exception_logger,
) -> None:
    """
    Bootstrap system user using the crate-control sidecar.
    """

    needs_update = False
    try:
        logger.info("Trying to create system user via sidecar...")
        result = await execute_sql_via_crate_control(
            namespace=namespace,
            name=name,
            sql=command_create_user["stmt"],
            args=command_create_user["args"],
            logger=logger,
        )
    except Exception as e:
        exception_logger("... failed. %s", str(e))
        raise _temporary_error()
    else:
        logger.info("Create user result: %s", result)
        if "rowcount" in result:
            logger.info("... success")
        elif (
            "error" in result
            and "RoleAlreadyExistsException" in result["error"]["message"]
        ):
            needs_update = True
            logger.info("... success. Already present")
        else:
            logger.info("... error. %s", result)
            raise _temporary_error()

    if needs_update:
        try:
            logger.info("Trying to update system user password via sidecar...")
            result = await execute_sql_via_crate_control(
                namespace=namespace,
                name=name,
                sql=command_alter_user["stmt"],
                args=command_alter_user["args"],
                logger=logger,
            )
        except Exception as e:
            exception_logger("... failed: %s", str(e))
            raise _temporary_error()
        else:
            if "rowcount" in result:
                logger.info("... success")
            else:
                logger.info("... error. %s", result)
                raise _temporary_error()

    try:
        logger.info("Trying to grant system user all privileges via sidecar...")
        result = await execute_sql_via_crate_control(
            namespace=namespace,
            name=name,
            sql=command_grant["stmt"],
            args=command_grant["args"],
            logger=logger,
        )
    except Exception as e:
        exception_logger("... failed. %s", str(e))
        raise _temporary_error()
    else:
        if "rowcount" in result:
            logger.info("... success")
        else:
            logger.info("... error. %s", result)
            raise _temporary_error()


# How long a single bootstrap attempt waits for the crate container to become
# ready before deferring back to kopf's handler retry. Polls frequently so the
# exec fires the moment the container is up, rather than bouncing off a
# not-ready container with 400/500s and waiting a full retry cycle.
#
# Generous on purpose: a crate container's readiness covers disk attach (slow on
# azure-disk), init containers (incl. a JMX-jar download), JVM boot and the
# readiness probe -- routinely a few minutes under load, so a tight budget would
# just churn kopf retries. This is only a per-attempt cap; kopf still retries
# beyond it, bounded by the overall BOOTSTRAP_TIMEOUT.
_READINESS_POLL_INTERVAL = 2
_READINESS_MAX_WAIT = 300


async def _wait_for_crate_container_ready(
    namespace: str, master_node_pod: str, logger: logging.Logger
) -> bool:
    """Wait for the ``crate`` container in ``master_node_pod`` to report ready.

    The bootstrap exec targets the ``crate`` container; attempting it before the
    container is ready makes the apiserver reject the exec upgrade with a
    ``400/500 Invalid response status``. Under kopf's retry cadence that adds
    minutes to cluster bring-up (and cascades into timeouts in other tests).
    Polling readiness first turns it into a single clean exec.

    Returns ``True`` once ready, or ``False`` if the per-attempt budget elapses
    (the caller then defers to kopf's handler retry, which calls this again). On
    timeout it logs the pod's last-seen phase and per-container state so a slow
    or stuck start (image pull, crash loop, init container waiting on a volume)
    is diagnosable from the run.
    """
    waited = 0
    last_pod = None
    while True:
        try:
            async with GlobalApiClient() as api_client:
                last_pod = await CoreV1Api(api_client).read_namespaced_pod(
                    name=master_node_pod, namespace=namespace
                )
            if any(
                c.name == "crate" and c.ready
                for c in (last_pod.status.container_statuses or [])
            ):
                return True
        except ApiException as e:
            # Transient read error -- keep polling within the budget.
            logger.warning("readiness check for %s failed: %s", master_node_pod, e)
        if waited >= _READINESS_MAX_WAIT:
            _log_pod_not_ready(master_node_pod, last_pod, waited, logger)
            return False
        await asyncio.sleep(_READINESS_POLL_INTERVAL)
        waited += _READINESS_POLL_INTERVAL


def _log_pod_not_ready(master_node_pod, pod, waited, logger):
    """Log the target pod's phase and per-container state when the crate
    container fails to become ready within the bootstrap readiness budget."""
    if pod is None:
        logger.warning(
            "crate container in %s not ready after %ss; pod could not be read",
            master_node_pod,
            waited,
        )
        return
    containers = {
        c.name: {
            "ready": c.ready,
            "restartCount": c.restart_count,
            "state": str(c.state),
        }
        for c in (pod.status.container_statuses or [])
    }
    init_containers = {
        c.name: {"ready": c.ready, "state": str(c.state)}
        for c in (pod.status.init_container_statuses or [])
    }
    logger.warning(
        "crate container in %s not ready after %ss: phase=%s containers=%s "
        "initContainers=%s",
        master_node_pod,
        waited,
        pod.status.phase,
        containers,
        init_containers,
    )


async def _log_pod_exec_failure(namespace, master_node_pod, e, logger):
    """
    Log diagnostics for a failed bootstrap pod-exec so transient/persistent
    websocket failures (e.g. ``400 Invalid response status``) are debuggable
    from the run: the exception detail plus the target pod's phase and per
    container readiness (was the ``crate`` container actually ready to exec?).
    """
    logger.warning(
        "bootstrap pod_exec into %s failed: type=%s status=%s detail=%s",
        master_node_pod,
        type(e).__name__,
        getattr(e, "status", None),
        getattr(e, "message", None) or str(e),
    )
    try:
        async with GlobalApiClient() as api_client:
            pod = await CoreV1Api(api_client).read_namespaced_pod(
                name=master_node_pod, namespace=namespace
            )
        containers = {
            c.name: {
                "ready": c.ready,
                "restartCount": c.restart_count,
                "state": str(c.state),
            }
            for c in (pod.status.container_statuses or [])
        }
        logger.warning(
            "bootstrap target pod %s phase=%s containers=%s",
            master_node_pod,
            pod.status.phase,
            containers,
        )
    except Exception as diag_e:  # diagnostics must never mask the real failure
        logger.warning(
            "could not read pod %s for bootstrap diagnostics: %s",
            master_node_pod,
            diag_e,
        )


async def _bootstrap_user_via_pod_exec(
    namespace: str,
    master_node_pod: str,
    scheme: str,
    command_create_user: Dict[str, Any],
    command_alter_user: Dict[str, Any],
    command_grant: Dict[str, Any],
    logger: logging.Logger,
    exception_logger,
) -> None:
    """
    Bootstrap system user using pod_exec (legacy approach).
    """

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

    command_create = get_curl_command(
        {
            "stmt": command_create_user["stmt"].replace("?", "$1"),
            "args": command_create_user["args"],
        }
    )
    command_alter = get_curl_command(
        {
            "stmt": command_alter_user["stmt"].replace("?", "$1"),
            "args": command_alter_user["args"],
        }
    )
    command_grant_curl = get_curl_command(
        {
            "stmt": command_grant["stmt"],
            "args": command_grant["args"],
        }
    )

    async def pod_exec(cmd):
        async with WsApiClient() as ws_api_client:
            core_ws = CoreV1Api(ws_api_client)
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

    # Don't exec into a container that isn't ready yet: that yields a
    # 400/500 'Invalid response status' and a wasted retry cycle. Wait for the
    # crate container to be ready first; if it isn't within this attempt's
    # budget, defer to kopf's handler retry.
    if not await _wait_for_crate_container_ready(namespace, master_node_pod, logger):
        logger.info(
            "crate container in %s not ready yet; deferring system-user bootstrap",
            master_node_pod,
        )
        raise _temporary_error()

    needs_update = False
    try:
        logger.info("Trying to create system user via pod_exec...")
        result = await pod_exec(command_create)
    except (ApiException, WSServerHandshakeError) as e:
        exception_logger("... failed. %s", str(e))
        await _log_pod_exec_failure(namespace, master_node_pod, e, logger)
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
            logger.info("Trying to update system user password via pod_exec...")
            result = await pod_exec(command_alter)
        except (ApiException, WSServerHandshakeError) as e:
            exception_logger("... failed. %s", str(e))
            await _log_pod_exec_failure(namespace, master_node_pod, e, logger)
            raise _temporary_error()
        else:
            if "rowcount" in result:
                logger.info("... success")
            else:
                logger.info("... error. %s", result)
                raise _temporary_error()

    try:
        logger.info("Trying to grant system user all privileges via pod_exec...")
        result = await pod_exec(command_grant_curl)
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
                core,
                namespace,
                {"key": "password", "name": GC_USER_SECRET_NAME.format(name=name)},
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
