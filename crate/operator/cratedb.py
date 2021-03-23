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

import functools
import logging
from typing import Optional, Tuple

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
    await cursor.execute("SELECT MAX(severity) FROM sys.health")
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
