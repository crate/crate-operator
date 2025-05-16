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

import logging
from datetime import datetime, timezone
from typing import Callable

import kopf
import wrapt

from crate.operator.webhooks import (
    WebhookAction,
    WebhookEvent,
    WebhookFeedbackPayload,
    WebhookOperation,
    WebhookStatus,
    webhook_client,
)


async def send_feedback_notification(
    *,
    namespace: str,
    name: str,
    message: str,
    operation: WebhookOperation,
    status: WebhookStatus,
    logger: logging.Logger,
    action: WebhookAction = WebhookAction.UNKNOWN,
    **kwargs,
) -> None:
    await webhook_client.send_notification(
        namespace,
        name,
        WebhookEvent.FEEDBACK,
        WebhookFeedbackPayload(message=message, operation=operation, action=action),
        status,
        logger,
    )


async def send_update_failed_notification(*args, **kwargs):
    await send_feedback_notification(
        *args,
        **{
            **kwargs,
            "operation": WebhookOperation.UPDATE,
            "status": WebhookStatus.FAILURE,
        },
    )


async def send_create_failed_notification(*args, **kwargs):
    await send_feedback_notification(
        *args,
        **{
            **kwargs,
            "operation": WebhookOperation.CREATE,
            "status": WebhookStatus.FAILURE,
            "action": WebhookAction.CREATE,
        },
    )


def timeout(*, timeout: float) -> Callable:
    """
    The ``@crate.timeout()`` decorator for kopf handlers or StateBasedSubHandler.
    It checks if the runtime passed as kwarg to all handlers exceeded the given
    timeout and raises a kopf.HandlerTimeoutError accordingly.

    :param timeout: the overall runtime of the decorated handler
    """

    def decorator(fn: Callable) -> Callable:
        @wrapt.decorator
        async def _async_timeout(wrapped, instance, args, kwargs):
            def _handler_has_timed_out_working(instance, timeout, kwargs) -> bool:
                """
                This checks the runtime of StateBasedSubHandlers based on the
                real start time stored in `status.subhandlerStartedAt`.
                """
                if instance:
                    now = int(datetime.now(timezone.utc).timestamp())
                    runtime = 0
                    status = (
                        kwargs["status"]
                        .get("subhandlerStartedAt", {})
                        .get(instance.__class__.__name__)
                    )
                    # calculate runtime for the current execution run
                    if (
                        status
                        and status.get("started")
                        and status.get("ref") == getattr(instance, "ref", None)
                    ):
                        runtime = now - status["started"]
                    return runtime >= timeout
                else:
                    return kwargs["runtime"].total_seconds() >= timeout

            if _handler_has_timed_out_working(instance, timeout, kwargs):
                _handler = (
                    f"{instance.__class__.__name__}.{wrapped.__name__}"
                    if instance
                    else f"{wrapped.__name__}"
                )

                raise kopf.HandlerTimeoutError(
                    f'{_handler} has timed out after {kwargs["runtime"]}.'
                )
            return await wrapped(*args, **kwargs)

        return _async_timeout(fn)

    return decorator


def error(*, error_handler: Callable) -> Callable:
    """
    The ``@crate.on.error()`` decorator for kopf handlers or StateBasedSubHandler.
    It catches permanent errors in handlers and runs a given coroutine. It re-raises
    the exception for further processing by kopf.

    :param error_handler: a coroutine which is run on fatal errors
    """

    def decorator(fn: Callable) -> Callable:
        @wrapt.decorator
        async def _async_error(wrapped, instance, args, kwargs):
            try:
                return await wrapped(*args, **kwargs)
            except kopf.PermanentError as e:
                _namespace = getattr(instance, "namespace", None) or kwargs.get(
                    "namespace", None
                )
                _name = getattr(instance, "name", None) or kwargs.get("name", None)
                if (
                    type(e) in [kopf.PermanentError, kopf.HandlerTimeoutError]
                    and callable(error_handler)
                    and _namespace
                    and _name
                ):
                    action: WebhookAction = WebhookAction.for_diff(kwargs["diff"])
                    try:
                        await error_handler(
                            namespace=_namespace,
                            name=_name,
                            message=str(e),
                            logger=kwargs["logger"],
                            action=action,
                        )
                    except Exception:
                        pass
                raise

        return _async_error(fn)

    return decorator
