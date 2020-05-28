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
