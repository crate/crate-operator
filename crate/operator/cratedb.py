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

import functools
import logging
from typing import Dict, Optional, Tuple, Union

import aiopg
from aiopg import Cursor
from psycopg2 import ProgrammingError
from psycopg2.extensions import quote_ident

from crate.operator.config import config
from crate.operator.constants import SYSTEM_USERNAME

HEALTHINESS = {1: "GREEN", 2: "YELLOW", 3: "RED"}


def get_connection(host: str, password: str, username: str = SYSTEM_USERNAME, **kwargs):
    """
    Create a connection object to ``host`` as system user.

    :param host: The CrateDB cluster to connect to.
    :param password: The password for the ``system`` user.
    :param username: An optional username to establish a connection. Defaults
        to ``"system"``.
    :param kwargs: Any additional arguments are passed as-is to
        :func:`aiopg.connect`.
    :returns: An async context manager and coroutine wrapper around
        :class:`aiopg.Connection`.
    """
    return aiopg.connect(host=host, user=username, password=password, **kwargs)


def connection_factory(
    host: str, password: str, username: str = SYSTEM_USERNAME, **kwargs
):
    """
    Create a connection factory to connect as user ``system`` to ``host``.

    :param host: The CrateDB cluster to connect to.
    :param password: The password for the ``system`` user.
    :param username: An optional username to establish a connection. Defaults
        to ``"system"``.
    :param kwargs: Any additional arguments are passed as-is to
        :func:`aiopg.connect`.
    :returns: A partial function that, when called, will call
        :func:`get_connection` with the provided arguments and return its
        result.
    """
    return functools.partial(get_connection, host, password, username, **kwargs)


async def create_user(cursor: Cursor, username: str, password: str) -> None:
    """
    Create user ``username`` and grant it ``ALL PRIVILEGES``.

    :param cursor: A database cursor object to the CrateDB cluster where the
        user should be added.
    :param username: The username for the new user.
    :param password: The password for the new user.
    """
    await cursor.execute(
        "SELECT count(*) = 1 FROM sys.users WHERE name = %s", (username,)
    )
    row = await cursor.fetchone()
    user_exists = bool(row[0])

    username_ident = quote_ident(username, cursor._impl)
    if not user_exists:
        await cursor.execute(
            f"CREATE USER {username_ident} WITH (password = %s)", (password,)
        )
    await cursor.execute(f"GRANT ALL PRIVILEGES TO {username_ident}")


async def update_user(cursor: Cursor, username: str, password: str) -> None:
    """
    Update the users password.

    :param cursor: A database cursor object to the CrateDB cluster where the
        user should be updated.
    :param username: The username of the user that should be updated.
    :param password: The new password.
    """

    username_ident = quote_ident(username, cursor._impl)
    await cursor.execute(
        f"ALTER USER {username_ident} SET (password = %s)", (password,)
    )


async def get_number_of_nodes(cursor: Cursor) -> int:
    """
    Return the number of nodes in the cluster from ``sys.nodes``, which is the
    number of nodes that can see each other.

    :param cursor: A database cursor to a current and open database connection.
    """
    await cursor.execute("SELECT COUNT(*) FROM sys.nodes")
    row = await cursor.fetchone()
    return row and row[0]


async def get_healthiness(cursor: Cursor) -> int:
    """
    Return the maximum severity of any table in the cluster from ``sys.health``.

    :param cursor: A database cursor to a current and open database connection.
    """
    await cursor.execute(
        "SELECT max(severity) FROM ("
        "  SELECT MAX(severity) as severity FROM sys.health"
        "  UNION ALL"
        '  SELECT 3 as "severity" FROM sys.cluster where master_node is null'
        ") sub;"
    )
    row = await cursor.fetchone()
    return row and row[0]


async def is_cluster_healthy(
    conn_factory, expected_nodes: int, logger: logging.Logger
) -> bool:
    """
    Check if a cluster is healthy.

    The function checks for the cluster health using the `sys.health
    <https://crate.io/docs/crate/reference/en/latest/admin/system-information.html#health>`_
    table and the expected number of nodes in the cluster three times.

    :param conn_factory: A callable that allows the operator to connect
        to the database. We regularly need to reconnect to ensure the
        connection wasn't closed because it was opened to a CrateDB node that
        was shut down since the connection was opened.
    :param expected_nodes: The number of nodes that make up a healthy cluster.
    """
    # We need to establish a new connection because the peer of a
    # previous connection could have been shut down. And by
    # re-establishing a connection for _each_ polling we can assert
    # that the connection is open
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            logger.debug("Checking if cluster is healthy ...")
            try:
                num_nodes = await get_number_of_nodes(cursor)
                healthiness = await get_healthiness(cursor)
                if num_nodes == expected_nodes and healthiness in {1, None}:
                    logger.info("Cluster has expected number of nodes and is healthy")
                    return True
                else:
                    logger.info(
                        "Cluster has %d of %d nodes and is in %s state.",
                        num_nodes,
                        expected_nodes,
                        HEALTHINESS.get(healthiness, "UNKNOWN"),
                    )
                    return False
            except ProgrammingError as e:
                logger.warning("Failed to run health check query", exc_info=e)
                return False


async def are_snapshots_in_progress(
    conn_factory, logger: logging.Logger
) -> Tuple[bool, Optional[str]]:
    """
    Check if there are any snapshots in progress by querying sys.jobs
    (or the configured table for testing purposes).

    Since sys.jobs will also contain the current query (i.e. us selecting from sys.jobs)
    we also need to exclude our statement.

    :param conn_factory the connection factory to connect to CrateDB
    :param logger the logger on which we're logging
    :return A Tuple of the result (Bool) and the statement that is progress (if any).
    """
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            logger.info("Checking if there are running snapshots ...")
            try:
                await cursor.execute(
                    f"SELECT stmt FROM {config.JOBS_TABLE} WHERE stmt "
                    f"LIKE '%CREATE SNAPSHOT%' "
                    f"AND stmt NOT LIKE '%{config.JOBS_TABLE}%'"
                )
                row = await cursor.fetchone()
                return (True, row[0]) if row else (False, None)
            except ProgrammingError as e:
                logger.warning("Failed to run snapshot query", exc_info=e)
                return False, None


async def set_cluster_setting(
    conn_factory,
    logger: logging.Logger,
    *,
    setting: str,
    value: Union[str, int],
    mode: str = "TRANSIENT",
) -> None:
    """
    Change a global cluster setting, see `Cluster-wide settings
    <https://crate.io/docs/crate/reference/en/latest/config/cluster.html#conf-cluster-settings>`_,
    to a different value.

    :param conn_factory: The connection factory to connect to CrateDB.
    :param logger: The logger on which we're logging.
    :param setting: The name of the setting that should be changed.
    :param value: The new value of the setting.
    :param mode: The level of persistence. Settings that are set using the
        "TRANSIENT" mode will be discarded if the cluster is stopped or
        restarted. Using "PERSISTENT" mode will preserve the value of the
        setting if the cluster restarts, defaults to "TRANSIENT".
    """
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            logger.info(
                f"Trying to set setting {setting} to value {value} with mode {mode}"
            )
            try:
                await cursor.execute(f"SET GLOBAL {mode} {setting} = %s", (value,))
            except ProgrammingError as e:
                logger.warning("Failed to run set setting query", exc_info=e)


async def reset_cluster_setting(
    conn_factory,
    logger: logging.Logger,
    *,
    setting: str,
) -> None:
    """
    Reset the value of a cluster setting to its default value or to the
    value defined in the configuration file, if it was set on a node start-up.

    :param conn_factory: The connection factory to connect to CrateDB.
    :param logger: The logger on which we're logging.
    :param setting: The name of the setting that should be reset.
    """
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            logger.info(f"Trying to reset setting {setting}")
            try:
                await cursor.execute(f"RESET GLOBAL {setting}")
            except ProgrammingError as e:
                logger.warning("Failed to run reset setting query", exc_info=e)


async def get_cluster_settings(cursor: Cursor) -> Dict:
    """
    Return information about the currently applied cluster settings
    from ``sys.cluster.settings``.

    :param cursor: A database cursor to a current and open database connection.
    :return: A Dict with all currently applied cluster settings
    """
    ret_val = {}
    await cursor.execute("SELECT sys.cluster.settings FROM sys.cluster")
    row = await cursor.fetchone()
    if row:
        ret_val = row[0]
    return ret_val


async def get_cluster_admin_username(conn_factory, logger) -> Optional[str]:
    """
    Returns the CrateDB admin username.

    :param cursor: A database cursor to a current and open database connection.
    :return: Either a string that represents the CrateDB admin username or None
    """
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            try:
                # Retrieve the admin username.
                # It should be the only admin that was created by system.
                await cursor.execute(
                    "SELECT grantee FROM sys.privileges"
                    " where grantor = 'system' limit 1"
                )
                row = await cursor.fetchone()
                return row[0] if row else None
            except ProgrammingError as e:
                logger.warning("Failed to run query", exc_info=e)
                return None
