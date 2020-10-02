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


class WebhookStatus(str, enum.Enum):
    FAILURE = "failure"
    SUCCESS = "success"


class WebhookScaleNodePayload(TypedDict):
    name: str
    replicas: str


class WebhookScalePayload(TypedDict):
    old_data_replicas: List[WebhookScaleNodePayload]
    new_data_replicas: List[WebhookScaleNodePayload]
    old_master_replicas: Optional[int]
    new_master_replicas: Optional[int]


class WebhookUpgradePayload(TypedDict):
    old_registry: str
    new_registry: str
    old_version: str
    new_version: str


class WebhookPayload(TypedDict):
    event: WebhookEvent
    status: WebhookStatus
    namespace: str
    cluster: str
    scale_data: Optional[WebhookScalePayload]
    upgrade_data: Optional[WebhookUpgradePayload]


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
        """
        if not self.configured:
            # When the webhook is fired but not configured, we're short-circuiting here
            # and return directly.
            logger.info(
                "Webhooks not configured. Not POSTing because of %s on cluster %s/%s",
                event,
                namespace,
                name,
            )
            return None

        payload = WebhookPayload(
            event=event,
            status=status,
            namespace=namespace,
            cluster=name,
            scale_data=scale_data,
            upgrade_data=upgrade_data,
        )

        logger.info(
            "POSTing to %s because of %s on cluster %s/%s",
            self._url,
            event,
            namespace,
            name,
        )
        try:
            response = await self._session.post(self._url, json=payload)
            async with response:
                response_text = await response.text()
                response.raise_for_status()
        except aiohttp.ClientResponseError:
            logger.exception(
                "Failed POSTing to %s because of %s on cluster %s/%s. "
                "Status: %d, Reason: %s, Body: %r",
                self._url,
                event,
                namespace,
                name,
                response.status,
                response.reason,
                response_text,
            )
            return response
        except aiohttp.ClientError:
            logger.exception(
                "Failed POSTing to %s because of %s on cluster %s/%s.",
                self._url,
                event,
                namespace,
                name,
            )
        else:
            logger.info(
                "Successfully POSTed to %s because of %s on cluster %s/%s. "
                "Status: %d, Body: %r",
                self._url,
                event,
                namespace,
                name,
                response.status,
                response_text,
            )
            return response

        return None

    async def send_scale_notification(
        self,
        status: WebhookStatus,
        namespace: str,
        name: str,
        data: WebhookScalePayload,
        logger: logging.Logger,
    ) -> Optional[aiohttp.ClientResponse]:
        """
        Send a notification about a failed or successful scaling operation.

        :param status: Did the scaling fail or succeed?
        :param namespace: The namespace the cluster resides in.
        :param name: The name of the cluster.
        :param data: Details about the scaling that took place or was attempted.
        """
        return await self._send(
            WebhookEvent.SCALE, status, namespace, name, scale_data=data, logger=logger
        )

    async def send_upgrade_notification(
        self,
        status: WebhookStatus,
        namespace: str,
        name: str,
        data: WebhookUpgradePayload,
        logger: logging.Logger,
    ) -> Optional[aiohttp.ClientResponse]:
        """
        Send a notification about a failed or successful upgrade operation.

        :param status: Did the upgrade fail or succeed?
        :param namespace: The namespace the cluster resides in.
        :param name: The name of the cluster.
        :param data: Details about the upgrade that took place or was attempted.
        """
        return await self._send(
            WebhookEvent.UPGRADE,
            status,
            namespace,
            name,
            upgrade_data=data,
            logger=logger,
        )


webhook_client = WebhookClient()
