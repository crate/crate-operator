from unittest import mock

import pytest

from crate.operator.cratedb import create_user, has_expected_nodes, is_healthy

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


@pytest.mark.parametrize(
    "n,expected", [(-1, False), (None, False), (1, False), (3, True), (4, False)]
)
async def test_has_expected_nodes(n, expected):
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (n,) if n is not None else None
    assert (await has_expected_nodes(cursor, 3)) is expected
    cursor.execute.assert_awaited_once_with("SELECT COUNT(*) FROM sys.nodes")
    cursor.fetchone.assert_awaited_once()


@pytest.mark.parametrize(
    "health,expected", [(None, False), (0, False), (1, True), (2, False), (3, False)]
)
async def test_is_healthy(health, expected):
    cursor = mock.AsyncMock()
    cursor.fetchone.return_value = (health,) if health is not None else None
    assert (await is_healthy(cursor)) is expected
    cursor.execute.assert_awaited_once_with("SELECT MAX(severity) FROM sys.health")
    cursor.fetchone.assert_awaited_once()
