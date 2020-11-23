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

import logging

from crate.operator.constants import BACKOFF_TIME
from crate.operator.cratedb import get_connection, update_user
from crate.operator.utils.formatting import b64decode


# update_user_password(host, username, old_password, new_password)
async def update_user_password(
    host: str,
    username: str,
    old_password: str,
    new_password: str,
    logger: logging.Logger,
):
    """
    Update the password of a given ``user_spec`` in a CrateDB cluster.

    :param host: The host of the CrateDB resource that should be updated.
    :param username: The username of the user of the CrateDB resource that
        should be updated.
    :param old_password: The old password of the user that should be updated.
    :param new_password: The new password of the user that should be updated.
    """
    async with get_connection(
        host, b64decode(old_password), username, timeout=BACKOFF_TIME / 4.0
    ) as conn:
        async with conn.cursor() as cursor:
            logger.info("Updating password for user '%s'", username)
            await update_user(cursor, username, b64decode(new_password))
