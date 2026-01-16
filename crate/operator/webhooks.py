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

import asyncio
import enum
import logging
import random
from typing import List, Optional, TypedDict

import aiohttp
import kopf
from aiohttp import ClientError, ClientResponseError, TCPConnector

from crate.operator import __version__
from crate.operator.utils.crd import has_compute_changed

RETRYABLE_STATUS_CODES = {502, 503, 504}
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 0.5


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
    ADMIN_USERNAME_CHANGED = "admin_username_changed"


class WebhookStatus(str, enum.Enum):
    FAILURE = "failure"
    SUCCESS = "success"
    IN_PROGRESS = "in_progress"
    TEMPORARY_FAILURE = "temporary_failure"


class WebhookOperation(str, enum.Enum):
    CREATE = "create"
    UPDATE = "update"


class WebhookAction(str, enum.Enum):
    UPGRADE = "upgrade"
    SCALE = "scale"
    CREATE = "create"
    ALLOWED_CIDR_UPDATE = "allowed_cidr_update"
    PASSWORD_UPDATE = "password_update"
    EXPAND_STORAGE = "expand_storage"
    SUSPEND = "suspend"
    CHANGE_COMPUTE = "change_compute"
    BACKUP_SCHEDULE_UPDATE = "backup_schedule_update"
    RESTORE_SNAPSHOT = "restore_snapshot"
    UNKNOWN = "unknown"

    @classmethod
    def for_diff(cls, diff: kopf.Diff):
        """
        Returns the performed type of operation based on the ``kopf.Diff`` of the
        current change.

        :param diff: The diff between the old and new resource body.
        """
        for op in diff:
            if op.operation == kopf.DiffOperation.ADD:
                if not op.field and op.new.get("snapshot"):
                    return cls.RESTORE_SNAPSHOT
                elif not op.field and op.new.get("spec", {}).get("cluster", {}).get(
                    "name"
                ):
                    return cls.CREATE
            elif op.operation == kopf.DiffOperation.CHANGE:
                if op.field in {
                    ("spec", "cluster", "imageRegistry"),
                    ("spec", "cluster", "version"),
                }:
                    return cls.UPGRADE
                elif op.field == ("spec", "nodes", "master", "replicas"):
                    return cls.SCALE
                elif op.field == ("spec", "nodes", "data"):
                    for node_spec_idx in range(len(op.old)):
                        old_spec = op.old[node_spec_idx]
                        new_spec = op.new[node_spec_idx]

                        if old_spec.get("replicas") != new_spec.get("replicas"):
                            if (
                                old_spec.get("replicas") == 0
                                or new_spec.get("replicas") == 0
                            ):
                                return cls.SUSPEND
                            return cls.SCALE
                        elif old_spec.get("resources", {}).get("disk", {}).get(
                            "size"
                        ) != new_spec.get("resources", {}).get("disk", {}).get("size"):
                            return cls.EXPAND_STORAGE
                        elif has_compute_changed(old_spec, new_spec):
                            return cls.CHANGE_COMPUTE
        return cls.UNKNOWN


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

    old_nodepool: str
    new_nodepool: str


class WebhookInfoChangedPayload(WebhookSubPayload):
    external_ip: str


class WebhookClusterHealthPayload(WebhookSubPayload):
    status: str


class WebhookFeedbackPayload(WebhookSubPayload):
    message: str
    operation: WebhookOperation
    action: WebhookAction


class WebhookBackupScheduleUpdatePayload(WebhookSubPayload):
    backup_schedule: str


class WebhookAdminUsernameChangedPayload(WebhookSubPayload):
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
    admin_username_changed_data: Optional[WebhookAdminUsernameChangedPayload]


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
        self._url = url
        self._session = aiohttp.ClientSession(
            headers={
                "User-Agent": f"cratedb-operator/{__version__}",
                "Content-Type": "application/json",
            },
            auth=aiohttp.BasicAuth(username, password),
            raise_for_status=False,
            connector=TCPConnector(limit=10),
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
        admin_username_changed_data: Optional[
            WebhookAdminUsernameChangedPayload
        ] = None,
        unsafe: Optional[bool] = False,
        retry: bool = True,
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
        :param admin_username_changed_data: Contains the new name of the admin username.
        :param logger: The logger to use
        :param unsafe: Whether to re-throw exceptions if any are caught.
        :param retry: Whether to retry requests. Defaults to True
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
            admin_username_changed_data=admin_username_changed_data,
        )

        logger.info(
            "Sending webhook for %s (%s).",
            event,
            status,
        )
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            response = None
            response_text = None
            try:
                async with self._session.post(self._url, json=payload) as response:
                    response_text = await response.text()
                    if response.status in RETRYABLE_STATUS_CODES and retry:
                        raise ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=response.reason,
                            headers=response.headers,
                        )
                    response.raise_for_status()
                    logger.info("Webhook for %s succeeded.", event)
                    return response
            except ClientResponseError as e:
                if (
                    e.status not in RETRYABLE_STATUS_CODES
                    or not retry
                    or attempt == RETRY_MAX_ATTEMPTS
                ):
                    logger.exception(
                        "Webhook for %s failed (%d %s). Response: %r",
                        event,
                        e.status,
                        e.message,
                        response_text,
                    )
                    if unsafe:
                        raise
                    return response

                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                delay *= random.uniform(0.8, 1.2)
                logger.warning(
                    "Webhook for %s failed with %d, retrying in %.1fs (%d/%d)",
                    event,
                    e.status,
                    delay,
                    attempt,
                    RETRY_MAX_ATTEMPTS,
                )
                await asyncio.sleep(delay)
            except ClientError:
                if attempt == RETRY_MAX_ATTEMPTS:
                    logger.exception("Webhook for %s failed after retries.", event)
                    if unsafe:
                        raise
                    return None

                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                delay *= random.uniform(0.8, 1.2)
                logger.warning(
                    "Webhook for %s connection error, retrying in %.1fs (%d/%d)",
                    event,
                    delay,
                    attempt,
                    RETRY_MAX_ATTEMPTS,
                )
                await asyncio.sleep(delay)

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
        retry: bool = True,
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
        :param retry: Whether to retry requests. Defaults to True
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
        elif event == WebhookEvent.ADMIN_USERNAME_CHANGED:
            kwargs = {"admin_username_changed_data": sub_payload}
        else:
            raise ValueError(f"Unknown event '{event}'")

        return await self._send(
            event,
            status,
            namespace,
            name,
            logger=logger,
            unsafe=unsafe,
            retry=retry,
            **kwargs,  # type:ignore
        )


webhook_client = WebhookClient()
