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

import base64
import functools
from typing import Callable, Union

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


def convert_to_bytes(value: Union[str, int]) -> bitmath.Byte:
    """
    Converts disk size strings used in Kubernetes, e.g. ``"256Gi"`` or Bytes
    to :class:`bitmath.Byte`.
    """
    if isinstance(value, str):
        return bitmath.parse_string_unsafe(value).to_Byte()
    if isinstance(value, int):
        return bitmath.Byte(value)
