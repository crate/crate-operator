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

import secrets
import string

_ALPHABET = string.ascii_letters + string.digits


def gen_password(length: int, alphabet=_ALPHABET) -> str:
    """
    Generate a cryptographically secure password of length ``length``.

    :param length: The desired password length.
    :param alphabet: The characters to use within the password.
    """
    return "".join(secrets.choice(alphabet) for i in range(length))
