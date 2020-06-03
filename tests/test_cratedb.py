from unittest import mock

import pytest

from crate.operator.cratedb import create_user

pytestmark = pytest.mark.asyncio


async def test_create_user(faker):
    password = faker.password()
    username = faker.user_name()
    username_ident = f'"{username}"'  # This is to check that "quote_ident" is called
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (0,)
    with mock.patch("crate.operator.cratedb.quote_ident", return_value=username_ident):
        await create_user(cursor, username, password)

    cursor.fetchone.assert_awaited_once()
    cursor.execute.assert_has_awaits(
        [
            mock.call(
                "SELECT count(*) = 1 FROM sys.users WHERE name = %s", (username,)
            ),
            mock.call(
                f"CREATE USER {username_ident} WITH (password = %s)", (password,)
            ),
            mock.call(f"GRANT ALL PRIVILEGES TO {username_ident}"),
        ]
    )


async def test_create_user_duplicate(faker):
    password = faker.password()
    username = faker.user_name()
    username_ident = f'"{username}"'  # This is to check that "quote_ident" is called
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (1,)
    with mock.patch("crate.operator.cratedb.quote_ident", return_value=username_ident):
        await create_user(cursor, username, password)

    cursor.fetchone.assert_awaited_once()
    cursor.execute.assert_has_awaits(
        [
            mock.call(
                "SELECT count(*) = 1 FROM sys.users WHERE name = %s", (username,)
            ),
            mock.call(f"GRANT ALL PRIVILEGES TO {username_ident}"),
        ]
    )
