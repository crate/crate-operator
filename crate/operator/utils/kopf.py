import abc
import functools
import logging
from functools import wraps
from typing import Any, Callable

import kopf

from crate.operator.webhooks import (
    WebhookEvent,
    WebhookStatus,
    WebhookSubPayload,
    webhook_client,
)

from .state import Context, State


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


class StateBasedSubHandler(abc.ABC):
    """
    State-based sub-handlers allow running sub-handlers in a defined order.

    Kopf sub-handlers run in parallel, meaning, the order of them being
    executed is not strictly defined. When updating a custom CrateDB resource,
    though, the order of the steps to deal with the changes in the operator are
    predefined.

    Subclasses of :class:`StateBasedSubHandler` are required to implement the
    :meth:`~StateBasedSubHandler.handle` method, which is the code that'll run
    as a sub-handler. The class will ensure that the sub-handler is only run
    when the current state in :class:`the statemachine
    <crate.operator.utils.state.StateMachine>` is :attr:`state`.
    """

    #: The :class:`~crate.operator.utils.state.State` at which to run the
    #: subhandler. Until the state has been reached, the sub-handler will be
    #: retried using :class:`kopf.TemporaryError`.
    state: State

    def __init__(self, namespace: str, name: str, context: Context):
        self.namespace = namespace
        self.name = name
        self._context = context

    def __call__(self):
        return functools.partial(self._subhandler)

    async def _subhandler(self, **kwargs: Any):
        if self._context.state_machine.current == self.state:
            await self.handle(**kwargs)
            self._context.state_machine.next()
        else:
            raise kopf.TemporaryError(
                f"Waiting to reach state '{self.state}'. "
                f"Current state '{self._context.state_machine.current}'."
            )

    @abc.abstractmethod
    async def handle(self, **kwargs: Any):
        raise NotImplementedError()

    def schedule_notification(
        self, event: WebhookEvent, payload: WebhookSubPayload, status: WebhookStatus
    ):
        self._context.schedule_notification(event, payload, status)

    async def send_notifications(self, logger: logging.Logger):
        for notification in self._context.notifications:
            logger.info(
                "Sending %s notification event %s with payload %s",
                notification["status"],
                notification["event"],
                notification["payload"],
            )
            webhook_client.send_notification(
                self.namespace,
                self.name,
                notification["event"],
                notification["payload"],
                notification["status"],
                logger,
            )
        self.clear_notifications()

    def clear_notifications(self):
        self._context.clear_notifications()
