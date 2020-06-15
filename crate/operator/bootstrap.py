import asyncio
import logging
from typing import Any, Dict, List, Optional

from aiohttp.client_exceptions import WSServerHandshakeError
from kubernetes_asyncio.client import ApiException, CoreV1Api
from kubernetes_asyncio.stream import WsApiClient
from psycopg2 import OperationalError
from psycopg2.extensions import QuotedString

from crate.operator.config import config
from crate.operator.constants import BACKOFF_TIME, SYSTEM_USERNAME
from crate.operator.cratedb import create_user, get_connection
from crate.operator.utils.kubeapi import (
    get_host,
    get_system_user_password,
    resolve_secret_key_ref,
)
from crate.operator.utils.typing import SecretKeyRefContainer

logger = logging.getLogger(__name__)


async def bootstrap_license(
    core: CoreV1Api,
    namespace: str,
    master_node_pod: str,
    has_ssl: bool,
    license: SecretKeyRefContainer,
) -> None:
    """
    Set a license key on a CrateDB cluster.

    When starting up a cluster, the operator doesn't have a system user yet
    that it could use. The operator will therefore ``exec`` into the ``crate``
    container in the ``master_node_pod`` and attempt to set a license key.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param master_node_pod: The pod name of one of the eligible master nodes in
        the cluster. Used to ``exec`` into.
    :param has_ssl: When ``True``, ``crash`` will establish a connection to
        the CrateDB cluster from inside the ``crate`` container using SSL/TLS.
        This must match how the cluster is configured, otherwise ``crash``
        won't be able to connect, since non-encrypted connections are forbidden
        when SSL/TLS is enabled, and encrypted connections aren't possible when
        no SSL/TLS is configured.
    :param license: A ``secretKeyRef`` to the Kubernetes secret that holds the
        CrateDB license key.
    """
    scheme = "https" if has_ssl else "http"
    license_key = await resolve_secret_key_ref(namespace, license["secretKeyRef"], core)
    license_key_quoted = QuotedString(license_key).getquoted().decode()
    command = [
        "crash",
        "--verify-ssl=false",
        f"--host={scheme}://localhost:4200",
        "-c",
        f"SET LICENSE {license_key_quoted};",
    ]

    core_ws = CoreV1Api(api_client=WsApiClient())
    exception_logger = logger.exception if config.TESTING else logger.error

    while True:
        try:
            logger.info("Trying to set license ...")
            result = await core_ws.connect_get_namespaced_pod_exec(
                namespace=namespace,
                name=master_node_pod,
                command=command,
                container="crate",
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
        except ApiException as e:
            # We don't use `logger.exception()` to not accidentally include the
            # license key in the log messages which might be part of the string
            # representation of the exception.
            exception_logger("... failed. Status: %s Reason: %s", e.status, e.reason)
            await asyncio.sleep(BACKOFF_TIME / 2.0)
        except WSServerHandshakeError as e:
            # We don't use `logger.exception()` to not accidentally include the
            # license key in the log messages which might be part of the string
            # representation of the exception.
            exception_logger("... failed. Status: %s Message: %s", e.status, e.message)
            await asyncio.sleep(BACKOFF_TIME / 2.0)
        else:
            if "SET OK" in result:
                logger.info("... success")
                break
            else:
                logger.info("... error. %s", result)
                await asyncio.sleep(BACKOFF_TIME / 2.0)


async def bootstrap_system_user(
    core: CoreV1Api, namespace: str, name: str, master_node_pod: str, has_ssl: bool
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
    password = await get_system_user_password(namespace, name, core)
    password_quoted = QuotedString(password).getquoted().decode()
    # Yes, we're constructing the SQL for the system user manually. But that's
    # fine in this case, because we have full control of the formatting of
    # the username and it's only `[a-z]+`.
    command_create_user = [
        "crash",
        "--verify-ssl=false",
        f"--host={scheme}://localhost:4200",
        "-c",
        f'CREATE USER "{SYSTEM_USERNAME}" WITH (password={password_quoted});',
    ]
    command_alter_user = [
        "crash",
        "--verify-ssl=false",
        f"--host={scheme}://localhost:4200",
        "-c",
        f'ALTER USER "{SYSTEM_USERNAME}" SET (password={password_quoted});',
    ]
    command_grant = [
        "crash",
        "--verify-ssl=false",
        f"--host={scheme}://localhost:4200",
        "-c",
        f'GRANT ALL PRIVILEGES TO "{SYSTEM_USERNAME}";',
    ]

    core_ws = CoreV1Api(api_client=WsApiClient())
    exception_logger = logger.exception if config.TESTING else logger.error

    needs_update = False
    while True:
        try:
            logger.info("Trying to create system user ...")
            result = await core_ws.connect_get_namespaced_pod_exec(
                namespace=namespace,
                name=master_node_pod,
                command=command_create_user,
                container="crate",
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
        except ApiException as e:
            # We don't use `logger.exception()` to not accidentally include the
            # password in the log messages which might be part of the string
            # representation of the exception.
            exception_logger("... failed. Status: %s Reason: %s", e.status, e.reason)
            await asyncio.sleep(BACKOFF_TIME / 2.0)
        except WSServerHandshakeError as e:
            # We don't use `logger.exception()` to not accidentally include the
            # password in the log messages which might be part of the string
            # representation of the exception.
            exception_logger("... failed. Status: %s Message: %s", e.status, e.message)
            await asyncio.sleep(BACKOFF_TIME / 2.0)
        else:
            if "CREATE OK" in result:
                logger.info("... success")
                break
            elif "UserAlreadyExistsException" in result:
                needs_update = True
                logger.info("... success. Already present")
                break
            else:
                logger.info("... error. %s", result)
                await asyncio.sleep(BACKOFF_TIME / 2.0)

    if needs_update:
        while True:
            try:
                logger.info("Trying to update system user password ...")
                result = await core_ws.connect_get_namespaced_pod_exec(
                    namespace=namespace,
                    name=master_node_pod,
                    command=command_alter_user,
                    container="crate",
                    stderr=True,
                    stdin=False,
                    stdout=True,
                    tty=False,
                )
            except ApiException as e:
                # We don't use `logger.exception()` to not accidentally include the
                # password in the log messages which might be part of the string
                # representation of the exception.
                exception_logger(
                    "... failed. Status: %s Reason: %s", e.status, e.reason
                )
                await asyncio.sleep(BACKOFF_TIME / 2.0)
            except WSServerHandshakeError as e:
                # We don't use `logger.exception()` to not accidentally include the
                # password in the log messages which might be part of the string
                # representation of the exception.
                exception_logger(
                    "... failed. Status: %s Message: %s", e.status, e.message
                )
                await asyncio.sleep(BACKOFF_TIME / 2.0)
            else:
                if "ALTER OK" in result:
                    logger.info("... success")
                    break
                else:
                    logger.info("... error. %s", result)
                    await asyncio.sleep(BACKOFF_TIME / 2.0)

    while True:
        try:
            logger.info("Trying to grant system user all privileges ...")
            result = await core_ws.connect_get_namespaced_pod_exec(
                namespace=namespace,
                name=master_node_pod,
                command=command_grant,
                container="crate",
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
        except (ApiException, WSServerHandshakeError):
            logger.exception("... failed")
            await asyncio.sleep(BACKOFF_TIME / 2.0)
        else:
            if "GRANT OK" in result:
                logger.info("... success")
                break
            else:
                logger.info("... error. %s", result)
                await asyncio.sleep(BACKOFF_TIME / 2.0)


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
    password = await get_system_user_password(namespace, name)
    while True:
        try:
            async with get_connection(
                host, password, timeout=BACKOFF_TIME / 4.0
            ) as conn:
                async with conn.cursor() as cursor:
                    for user_spec in users:
                        username = user_spec["name"]
                        password = await resolve_secret_key_ref(
                            namespace, user_spec["password"]["secretKeyRef"], core,
                        )
                        await create_user(cursor, username, password)
                    break
        except OperationalError:
            await asyncio.sleep(BACKOFF_TIME / 2.0)


async def bootstrap_cluster(
    core: CoreV1Api,
    namespace: str,
    name: str,
    master_node_pod: str,
    license: Optional[SecretKeyRefContainer],
    has_ssl: bool,
    users: Optional[List[Dict[str, Any]]],
):
    """
    Bootstrap an entire cluster, including license, system user, and additional
    users.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the ``CrateDB`` custom resource. Used to lookup
        the password for the system user created during deployment.
    :param master_node_pod: The pod name of one of the eligible master nodes in
        the cluster. Used to ``exec`` into.
    :param license: An optional ``secretKeyRef`` to the Kubernetes secret that
        holds the CrateDB license key.
    :param has_ssl: When ``True``, ``crash`` will establish a connection to
        the CrateDB cluster from inside the ``crate`` container using SSL/TLS.
        This must match how the cluster is configured, otherwise ``crash``
        won't be able to connect, since non-encrypted connections are forbidden
        when SSL/TLS is enabled, and encrypted connections aren't possible when
        no SSL/TLS is configured.
    :param users: An optional list of user definitions containing the username
        and the secret key reference to their password.
    """
    # We first need to set the license, in case the CrateDB cluster
    # contains more nodes than available in the free license.
    if license:
        await bootstrap_license(
            core, namespace, master_node_pod, has_ssl, license,
        )
    await bootstrap_system_user(core, namespace, name, master_node_pod, has_ssl)
    if users:
        await bootstrap_users(core, namespace, name, users)
