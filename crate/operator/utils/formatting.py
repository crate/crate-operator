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

import base64
import functools
from typing import Callable

import bitmath


def encode_decode_wrapper(fn: Callable[[bytes], bytes], s: str) -> str:
    """
    Encode ``s`` to bytes, call ``fn`` with that and decode the result again.

    The function uses UTF-8 for encoding and decoding.

    :param fn: A function that should be called with the byte encoding and
        who's response should be decoded to string again.
    :param s: The string to encode, pass to ``fn``, and decode.
    """
    if s is None:
        return None
    return fn(s.encode("utf-8")).decode("utf-8")


#: Wrapper to base 64 encode a string and return a string.
b64decode = functools.partial(encode_decode_wrapper, base64.b64decode)
#: Wrapper to base 64 decode a string and return a string.
b64encode = functools.partial(encode_decode_wrapper, base64.b64encode)


def format_bitmath(value: bitmath.Byte) -> str:
    """
    Format a :class:`bitmath.Byte` such that it is safe to use with Kubernetes.

    Under the hood, the format ``{value}{unit}`` is used, but without the
    trailing ``B``. Additionally, the "best" unit is picked. For example,
    passing ``bitmath.GiB(0.25)`` would result in ``"256Mi"``.
    """
    # All bitmath units end with "b" (for bits, e.g. 19 kb) or "B" (for bytes,
    # e.g. 2 MB or 2 MiB). Since Kubernetes expects units to be without the
    # trailing "b" or "B", we're stripping that here.
    # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory
    return value.best_prefix().format("{value}{unit}")[:-1]
