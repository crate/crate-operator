import logging

import aiopg
from aiopg import Cursor
from psycopg2.extensions import quote_ident

from crate.operator.constants import SYSTEM_USERNAME

logger = logging.getLogger(__name__)


def get_connection(host: str, password: str, username: str = SYSTEM_USERNAME, **kwargs):
    """
    Create a connection object to ``host`` as system user.

    :param host: The CrateDB cluster to connect to.
    :param password: The password for the ``system`` user.
    :param username: An optional username to establish a connection. Defaults
        to ``'system'``.
    :param kwargs: Any additional arguments are passed as-is to
        :func:`aiopg.connect`.
    :returns: An async context manager and coroutine wrapper around
        :class:`aiopg.Connection`.
    """
    return aiopg.connect(host=host, user=username, password=password, **kwargs)


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
