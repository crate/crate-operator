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

import enum
import logging
from typing import List, Optional, TypedDict

import aiohttp
from pkg_resources import get_distribution


class WebhookEvent(str, enum.Enum):
    SCALE = "scale"
    UPGRADE = "upgrade"
    SNAPSHOT = "snapshot"
    DELAY = "delay"
    INFO_CHANGED = "info_changed"
    HEALTH = "health"
    ERROR = "error"


class WebhookStatus(str, enum.Enum):
    FAILURE = "failure"
    SUCCESS = "success"
    IN_PROGRESS = "in_progress"
    TEMPORARY_FAILURE = "temporary_failure"


class WebhookOperation(str, enum.Enum):
    CREATE = "create"
    UPDATE = "update"


class WebhookSubPayload(TypedDict):
    pass


class WebhookScaleNodePayload(TypedDict):
    name: str
    replicas: str


class WebhookScalePayload(WebhookSubPayload):
    old_data_replicas: List[WebhookScaleNodePayload]
    new_data_replicas: List[WebhookScaleNodePayload]
    old_master_replicas: Optional[int]
    new_master_replicas: Optional[int]


class WebhookTemporaryFailurePayload(WebhookSubPayload):
    reason: str


class WebhookUpgradePayload(WebhookSubPayload):
    old_registry: str
    new_registry: str
    old_version: str
    new_version: str


class WebhookInfoChangedPayload(WebhookSubPayload):
    external_ip: str


class WebhookClusterHealthPayload(WebhookSubPayload):
    status: str


class WebhookPermanentErrorPayload(WebhookSubPayload):
    reason: str
    operation: WebhookOperation


class WebhookPayload(TypedDict):
    event: WebhookEvent
    status: WebhookStatus
    namespace: str
    cluster: str
    scale_data: Optional[WebhookScalePayload]
    upgrade_data: Optional[WebhookUpgradePayload]
    temporary_failure_data: Optional[WebhookTemporaryFailurePayload]
    info_data: Optional[WebhookInfoChangedPayload]
    health_data: Optional[WebhookClusterHealthPayload]
    error_data: Optional[WebhookPermanentErrorPayload]


class WebhookClient:
    """
    A client to send HTTP POST requests to an API endpoint if configured.

    The ``WebhookClient`` is a wrapper around ``aiohttp``, providing a specific
    API that allows sending webhook notifications to an external service.

    By default, an instance is not configured. One needs to call
    :meth:`configure` to enable notification sending. otherwise, sending will
    be ignored and a log message written instead.

    The :ref:`webhooks section <concept-webhooks>` in the cepepts part of the
    documentation elaborates on the payload.
    """

    def __init__(self) -> None:
        self._configured = False

    def configure(self, url: str, username: str, password: str) -> None:
        """
        Configure and enable the client.

        :param url: The full URL (scheme, domain, port, path) where to POST
            requests to.
        :param username: All requests will include HTTP Basic Auth credentials.
            This is the username for that.
        :param password: All requests will include HTTP Basic Auth credentials.
            This is the password for that.
        """
        version = get_distribution("crate-operator").version
        self._url = url
        self._session = aiohttp.ClientSession(
            headers={
                "User-Agent": f"cratedb-operator/{version}",
                "Content-Type": "application/json",
            },
            auth=aiohttp.BasicAuth(username, password),
            raise_for_status=False,
        )

        self._configured = True

    @property
    def configured(self) -> bool:
        """
        Return if the client is configured. If not, all notification sending
        should be skipped.
        """
        return self._configured

    async def _send(
        self,
        event: WebhookEvent,
        status: WebhookStatus,
        namespace: str,
        name: str,
        *,
        scale_data: Optional[WebhookScalePayload] = None,
        upgrade_data: Optional[WebhookUpgradePayload] = None,
        temporary_failure_data: Optional[WebhookTemporaryFailurePayload] = None,
        info_data: Optional[WebhookInfoChangedPayload] = None,
        health_data: Optional[WebhookClusterHealthPayload] = None,
        error_data: Optional[WebhookPermanentErrorPayload] = None,
        unsafe: Optional[bool] = False,
        logger: logging.Logger,
    ) -> Optional[aiohttp.ClientResponse]:
        """
        Send a notification to the confiured endpoint.

        :param event: The operation that was performed or attempted.
        :param status: Did the operation fail or succeed?
        :param namespace: The namespace the cluster resides in.
        :param name: The name of the cluster.
        :param scale_data: Details about the scaling operation that took place
            or was attempted.
        :param upgrade_data: Details about the upgrading operation that took
            place or was attempted.
        :param temporary_failure_data: Details about the temporary failure
        :param info_data: Information details payload
        :param logger: The logger to use
        :param unsafe: Whether to re-throw exceptions if any are caught.
        """
        if not self.configured:
            # When the webhook is fired but not configured, we're short-circuiting here
            # and return directly.
            logger.info(
                "Webhooks not configured. Not processing event %s.",
                event,
            )
            return None

        payload = WebhookPayload(
            event=event,
            status=status,
            namespace=namespace,
            cluster=name,
            scale_data=scale_data,
            upgrade_data=upgrade_data,
            temporary_failure_data=temporary_failure_data,
            info_data=info_data,
            health_data=health_data,
            error_data=error_data,
        )

        logger.info(
            "Sending webhook for %s (%s).",
            event,
            status,
        )
        try:
            response = await self._session.post(self._url, json=payload)
            async with response:
                response_text = await response.text()
                response.raise_for_status()
        except aiohttp.ClientResponseError:
            logger.exception(
                "Webhook for %s failed (%d %s). Response: %r",
                event,
                response.status,
                response.reason,
                response_text,
            )
            if unsafe:
                raise
            return response
        except aiohttp.ClientError:
            logger.exception("Webhook for %s failed.", event)
            if unsafe:
                raise
        else:
            logger.info("Webhook for %s succeeded.", event)
            return response

        return None

    async def send_notification(
        self,
        namespace: str,
        name: str,
        event: WebhookEvent,
        sub_payload: WebhookSubPayload,
        status: WebhookStatus,
        logger: logging.Logger,
        *,
        unsafe: Optional[bool] = False,
    ):
        """
        Send a webhook notification to the configured webhook URL.
        Will do nothing if no webhooks are configured.

        Webhooks are normally fire-and-forget FYI information that must not block kopf
        from continuing with the handler chain. In some cases however, the webhook is
        the whole point (i.e. IP address acquired), so we want to fail if an error
        happens so that the operation could be retried.

        :param name: Name of the cluster this notification is about
        :param namespace: Namespaces where this event happened
        :param event: The event type
        :param sub_payload: The specific payload for the event type
        :param status: The status of the event
        :param logger: The logger to use
        :param unsafe: Whether to throw exceptions if any happen. Defaults to False -
                       exceptions will be logged but not propagated.
        """
        if event == WebhookEvent.SCALE:
            kwargs = {"scale_data": sub_payload}
        elif event == WebhookEvent.UPGRADE:
            kwargs = {"upgrade_data": sub_payload}
        elif event == WebhookEvent.DELAY:
            kwargs = {"temporary_failure_data": sub_payload}
        elif event == WebhookEvent.INFO_CHANGED:
            kwargs = {"info_data": sub_payload}
        elif event == WebhookEvent.HEALTH:
            kwargs = {"health_data": sub_payload}
        elif event == WebhookEvent.ERROR:
            kwargs = {"error_data": sub_payload}
        else:
            raise ValueError(f"Unknown event '{event}'")

        return await self._send(
            event,
            status,
            namespace,
            name,
            logger=logger,
            unsafe=unsafe,
            **kwargs,  # type:ignore
        )


webhook_client = WebhookClient()
