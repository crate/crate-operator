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

import enum
import logging
from typing import List, Optional, TypedDict

import aiohttp
from pkg_resources import get_distribution


class WebhookEvent(str, enum.Enum):
    SCALE = "scale"
    UPGRADE = "upgrade"
    COMPUTE_CHANGED = "compute_changed"
    SNAPSHOT = "snapshot"
    DELAY = "delay"
    INFO_CHANGED = "info_changed"
    HEALTH = "health"
    FEEDBACK = "feedback"
    BACKUP_SCHEDULE_UPDATE = "backup_schedule_update"
    SNAPSHOT_RESTORED = "snapshot_restored"


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


class WebhookChangeComputePayload(WebhookSubPayload):
    old_cpu_limit: int
    old_memory_limit: str
    old_cpu_request: int
    old_memory_request: str

    new_cpu_limit: int
    new_memory_limit: str
    new_cpu_request: int
    new_memory_request: str

    old_heap_ratio: float
    new_heap_ratio: float


class WebhookInfoChangedPayload(WebhookSubPayload):
    external_ip: str


class WebhookClusterHealthPayload(WebhookSubPayload):
    status: str


class WebhookFeedbackPayload(WebhookSubPayload):
    message: str
    operation: WebhookOperation


class WebhookBackupScheduleUpdatePayload(WebhookSubPayload):
    backup_schedule: str


class WebhookSnapshotRestoredPayload(WebhookSubPayload):
    admin_username: Optional[str]


class WebhookPayload(TypedDict):
    event: WebhookEvent
    status: WebhookStatus
    namespace: str
    cluster: str
    scale_data: Optional[WebhookScalePayload]
    upgrade_data: Optional[WebhookUpgradePayload]
    compute_changed_data: Optional[WebhookChangeComputePayload]
    temporary_failure_data: Optional[WebhookTemporaryFailurePayload]
    info_data: Optional[WebhookInfoChangedPayload]
    health_data: Optional[WebhookClusterHealthPayload]
    feedback_data: Optional[WebhookFeedbackPayload]
    backup_schedule_data: Optional[WebhookBackupScheduleUpdatePayload]
    snapshot_restored_data: Optional[WebhookSnapshotRestoredPayload]


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
        compute_changed_data: Optional[WebhookChangeComputePayload] = None,
        temporary_failure_data: Optional[WebhookTemporaryFailurePayload] = None,
        info_data: Optional[WebhookInfoChangedPayload] = None,
        health_data: Optional[WebhookClusterHealthPayload] = None,
        feedback_data: Optional[WebhookFeedbackPayload] = None,
        backup_schedule_data: Optional[WebhookBackupScheduleUpdatePayload] = None,
        snapshot_restored_data: Optional[WebhookSnapshotRestoredPayload] = None,
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
        :param compute_changed_data: Details about changing cpu and/or ram operation
            that took place or was attempted.
        :param temporary_failure_data: Details about the temporary failure
        :param info_data: Information details payload
        :param backup_schedule_data: Details about the change in the backup
            cronjob schedule.
        :param snapshot_restored_data: Details about the snapshot restore process.
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
            compute_changed_data=compute_changed_data,
            temporary_failure_data=temporary_failure_data,
            info_data=info_data,
            health_data=health_data,
            feedback_data=feedback_data,
            backup_schedule_data=backup_schedule_data,
            snapshot_restored_data=snapshot_restored_data,
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
        elif event == WebhookEvent.COMPUTE_CHANGED:
            kwargs = {"compute_changed_data": sub_payload}
        elif event == WebhookEvent.DELAY:
            kwargs = {"temporary_failure_data": sub_payload}
        elif event == WebhookEvent.INFO_CHANGED:
            kwargs = {"info_data": sub_payload}
        elif event == WebhookEvent.HEALTH:
            kwargs = {"health_data": sub_payload}
        elif event == WebhookEvent.FEEDBACK:
            kwargs = {"feedback_data": sub_payload}
        elif event == WebhookEvent.BACKUP_SCHEDULE_UPDATE:
            kwargs = {"backup_schedule_data": sub_payload}
        elif event == WebhookEvent.SNAPSHOT_RESTORED:
            kwargs = {"snapshot_restored_data": sub_payload}
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
