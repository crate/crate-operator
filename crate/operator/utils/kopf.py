import abc
import functools
import json
import logging
from functools import wraps
from typing import Any, Callable, Optional, TypedDict

import kopf

from crate.operator.webhooks import (
    WebhookEvent,
    WebhookStatus,
    WebhookSubPayload,
    webhook_client,
)

from ..constants import KOPF_STATE_STORE_PREFIX


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
        return await awaitable(*args, **kwargs)

    return _wrapper


class Notification(TypedDict):
    event: WebhookEvent
    payload: WebhookSubPayload
    status: WebhookStatus


class StateBasedSubHandler(abc.ABC):
    """
    A handler capable of waiting for other handlers to finish.

    This can be expressed as a set of dependencies passed to the ``depends_on``
    parameter of the constructor.

    Basically, we wrap an actual handler here. Before executing the actual one,
    we check if the dependencies have completed yet - we do this by checking
    the statuses of those dependent handlers, as stored in the ``status`` field
    of the CrateDB resource.
    """

    def __init__(
        self,
        namespace: str,
        name: str,
        ref: str,
        context: dict,
        depends_on=None,
        run_on_dep_failures=False,
    ):
        """
        Constructs a new dependency-aware handler.

        :param namespace: the namespace to use
        :param name: the name of the CrateDB resource we're working on
        :param ref: reference for the current execution run.
        :param context: a dict allowing storage of status info between executions.
        :param depends_on: list of dependent handler this handler should wait for.
        :param run_on_dep_failures: whether we should still execute if our dependencies
            have failed. This is useful for handlers that clean up resources after
            other handlers have finished, and always need to run.
        """
        self.namespace = namespace
        self.name = name
        self.ref = ref
        self._context = context
        self.depends_on = depends_on if depends_on is not None else []
        self.run_on_dep_failures = run_on_dep_failures

    def __call__(self, **kwargs: Any):
        return functools.partial(self._subhandler, **kwargs)

    async def _subhandler(self, **kwargs: Any):
        status = kwargs["status"]
        annotations = kwargs["annotations"]
        logger = kwargs["logger"]
        waiting_for = []
        for dependency in self.depends_on:
            if self._get_status(status, dependency, logger) is None:
                if self._should_run_on_failed_dependency(
                    annotations, dependency, logger
                ):
                    continue

                waiting_for.append(dependency)

        if len(waiting_for) > 0:
            wt = ",".join(waiting_for)
            raise kopf.TemporaryError(f"Waiting for '{wt}'.", delay=30)

        res = await self.handle(**kwargs)
        return {"success": True, "ref": self.ref, "result": res}

    @abc.abstractmethod
    async def handle(self, **kwargs: Any):
        raise NotImplementedError()

    def schedule_notification(
        self,
        event: WebhookEvent,
        payload: WebhookSubPayload,
        status: WebhookStatus,
    ):
        self._context.setdefault("notifications", []).append(
            Notification(event=event, payload=payload, status=status)
        )

    async def send_notifications(self, logger: logging.Logger):
        for notification in self._context.get("notifications", []):
            logger.info(
                "Sending %s notification event %s with payload %s",
                notification["status"],
                notification["event"],
                notification["payload"],
            )
            await webhook_client.send_notification(
                self.namespace,
                self.name,
                notification["event"],
                notification["payload"],
                notification["status"],
                logger,
            )
        self._context.get("notifications", []).clear()

    def _get_status(self, statuses: dict, dependency: str, logger) -> Optional[dict]:
        """
        Get the status of the specified dependency, obeying the ref.
        """
        status = statuses.get(dependency, None)
        if not status:
            return None

        if status.get("ref", None) != self.ref:
            logger.debug(
                "Ignoring status for '%s' from previous run: %s", dependency, status
            )
            return None

        return status

    def _should_run_on_failed_dependency(
        self, annotations: dict, handler_name: str, logger: logging.Logger
    ) -> bool:
        """
        There is no way in kopf to say if a certain handler has failed or not.

        What we are doing instead is peeking into kopf's internal state storage -
        the annotations on the CrateDB objects to check if the handler has failed.

        Slightly naughty, but there is no better way at the time of writing.
        """
        # Handler names have dots instead of slashes in annotations
        normalized_name = handler_name.replace("/", ".")
        key = f"{KOPF_STATE_STORE_PREFIX}/{normalized_name}"
        status_str = annotations.get(key)
        if not status_str:
            return False
        status = json.loads(status_str)
        if not status["success"] and status["failure"]:
            if self.run_on_dep_failures:
                logger.warning(
                    f"Our dependency ({handler_name}) has failed but we'll still run."
                )
                return True
            else:
                raise kopf.PermanentError(
                    f"A dependency ({handler_name}) has failed. Giving up."
                )

        return False
