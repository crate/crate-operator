import asyncio
import functools
import logging

import aiopg
from aiopg import Cursor
from psycopg2 import ProgrammingError
from psycopg2.extensions import quote_ident

from crate.operator.constants import BACKOFF_TIME, SYSTEM_USERNAME

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


async def wait_for_healthy_cluster(
    connection_factory, expected_nodes: int, logger: logging.Logger,
) -> None:
    """
    Indefinitely wait for the cluster to become healty.

    The function repeatedly checks for the cluster health using the
    `sys.health
    <https://crate.io/docs/crate/reference/en/latest/admin/system-information.html#health>`_
    table and the expected number of nodes in the cluster. The function
    potentially runs indefinitely if the cluster is not in a ``GREEN`` state.
    The reason for that is, that an issue with cluster health could be solved
    by intervening on the cluster directly, e.g. by adjusting the number of
    replicas.

    :param connection_factory: A callable that allows the operator to connect
        to the database. We regularly need to reconnect to ensure the
        connection wasn't closed because it was opened to a CrateDB node that
        was shut down since the connection was opened.
    :param expected_nodes: The number of nodes that make up a healthy cluster.
    """
    # An integer healthiness flag. Increased upon succeeding healthy
    # responses, and expected number of nodes; reset to 0 upon unhealtiness or
    # on missing nodes.
    healthy_count = 0

    # An integer that counts the number of unhandled exceptions that occurred
    # while trying to obtain the cluster health.
    exception_count = 0

    while True:
        try:
            # We need to establish a new connection because the peer of a
            # previous connection could have been shut down. And by
            # re-establishing a connection for _each_ polling we can assert
            # that the connection is open
            async with connection_factory() as conn:
                async with conn.cursor() as cursor:
                    logger.info("Waiting for cluster to get healthy again ...")
                    try:
                        num_nodes = await get_number_of_nodes(cursor)
                        healthiness = await get_healthiness(cursor)
                        if num_nodes == expected_nodes and healthiness in {1, None}:
                            healthy_count += 1
                            logger.info(
                                "Cluster has expected nodes and is healthy (%d of 3)",
                                healthy_count,
                            )
                        else:
                            logger.info(
                                "Cluster has %d of %d nodes and is in %s state.",
                                num_nodes,
                                expected_nodes,
                                HEALTHINESS.get(healthiness, "UNKNOWN"),
                            )
                            healthy_count = 0
                    except ProgrammingError as e:
                        logger.warning("Failed to run health check query", exc_info=e)
                        # We decided to endlessly retry. The reason for that
                        # is, that an issue with cluster health could be solved
                        # by intervening on the cluster directly, e.g. by
                        # adjusting the number of replicas, and in this case
                        # the operator should not fail but continue.
                        healthy_count = 0
                        await asyncio.sleep(BACKOFF_TIME)
                    else:
                        if healthy_count >= 3:
                            break
                        else:
                            # Wait some time before we retry.
                            await asyncio.sleep(BACKOFF_TIME / 2.0)
        except Exception as e:
            # Sometimes the client is not able to connect to a CrateDB cluster,
            # e.g. when a load balancer is timing out, or when a load balancer
            # routes to an non-existent node. In this case we retry 5 times and
            # only fail then.
            if exception_count > 5:
                raise e
            else:
                healthy_count = 0
                logger.warning("Failed to connect to cluster", exc_info=e)
            exception_count += 1
            await asyncio.sleep(BACKOFF_TIME)
