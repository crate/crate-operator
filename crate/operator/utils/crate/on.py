# CrateDB Kubernetes Operator
# Copyright (C) 2021 Crate.IO GmbH
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

import logging
from typing import Callable

import kopf
import wrapt

from crate.operator.webhooks import (
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
    **kwargs,
) -> None:
    await webhook_client.send_notification(
        namespace,
        name,
        WebhookEvent.FEEDBACK,
        WebhookFeedbackPayload(message=message, operation=operation),
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
            if kwargs["runtime"].total_seconds() >= timeout:
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
                    try:
                        await error_handler(
                            namespace=_namespace,
                            name=_name,
                            message=str(e),
                            logger=kwargs["logger"],
                        )
                    except Exception:
                        pass
                raise

        return _async_error(fn)

    return decorator
