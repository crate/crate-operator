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

import abc
import functools
import json
import logging
from functools import wraps
from typing import Any, Callable, Optional, TypedDict

import kopf

from crate.operator.exceptions import SubHandlerFailedDependencyError
from crate.operator.webhooks import (
    WebhookEvent,
    WebhookStatus,
    WebhookSubPayload,
    webhook_client,
)

from ..config import config
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
            # If running in testing mode (i.e. running ITs) we can reduce the delay
            # significantly as things generally move fast.
            raise kopf.TemporaryError(
                f"Waiting for '{wt}'.", delay=5 if config.TESTING else 30
            )

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

    async def send_registered_notifications(self, logger: logging.Logger):
        for notification in self._context.get("notifications", []):
            await self.send_notification_now(
                logger,
                WebhookEvent(notification["event"]),
                notification["payload"],
                WebhookStatus(notification["status"]),
            )
        self._context.get("notifications", []).clear()

    async def send_notification_now(
        self,
        logger: logging.Logger,
        event: WebhookEvent,
        payload: WebhookSubPayload,
        status: WebhookStatus,
    ):
        await send_webhook_notification(
            self.namespace, self.name, logger, event, payload, status
        )

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
        # Use the same procedure as kopf to create the handler name for the
        # annotations lookup. Important if the handler name exceeds the maximum
        # allowed length of 63 chars which is likely for @kopf.on.field() handlers
        # that have the field path in the name.
        progressor = kopf.AnnotationsProgressStorage(
            v1=False, prefix=KOPF_STATE_STORE_PREFIX
        )
        key = progressor.make_v2_key(handler_name)
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
                raise SubHandlerFailedDependencyError(
                    f"A dependency ({handler_name}) has failed. Giving up."
                )

        return False


async def send_webhook_notification(
    namespace: str,
    name: str,
    logger: logging.Logger,
    event: WebhookEvent,
    payload: WebhookSubPayload,
    status: WebhookStatus,
):
    notification = Notification(event=event, payload=payload, status=status)
    logger.info(
        "Sending %s notification event %s with payload %s",
        notification["status"],
        notification["event"],
        notification["payload"],
    )
    await webhook_client.send_notification(
        namespace,
        name,
        notification["event"],
        notification["payload"],
        notification["status"],
        logger,
    )
