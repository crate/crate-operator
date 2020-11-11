from functools import wraps
from typing import Callable


def subhandler_partial(awaitable: Callable, *args, **kwargs):
    """
    A utility function to create a partial coroutine suitable for ``kopf.register``.

    When scheduling asynchronous sub-handlers in Kopf, one needs to be careful
    to not create coroutines when they're not used in an execution cycle.

        >>> async def some_coro(arg1, kwarg1=None):
        ...     pass
        >>> kopf.register(
        ...     fn=subhandler_partial(
        ...         some_coro,
        ...         'abc',
        ...         kwarg1='foo',
        ...     ),
        ...     id="some-id",
        ... )
    """

    @wraps(awaitable)
    async def _wrapper(**_):
        await awaitable(*args, **kwargs)

    return _wrapper
