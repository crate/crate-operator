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
