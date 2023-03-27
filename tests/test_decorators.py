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

import datetime
import logging
from typing import Any
from unittest import mock

import kopf
import pytest

from crate.operator.operations import StateBasedSubHandler
from crate.operator.utils import crate
from crate.operator.webhooks import (
    WebhookAction,
    WebhookEvent,
    WebhookFeedbackPayload,
    WebhookOperation,
    WebhookStatus,
)

from .utils import DEFAULT_TIMEOUT, assert_wait_for, was_notification_sent


class ActionSubhandler(StateBasedSubHandler):
    """A subhandler for testing timeout and error decorators"""

    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(20))
    async def handle(self, **kwargs: Any):
        return {"success": True}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "runtime, timeout",
    [(15, 20), (20, 20), (30, 20)],
)
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_upgrade_cluster_timeout(
    mock_send_notification: mock.AsyncMock,
    runtime,
    timeout,
    faker,
):
    name = faker.domain_word()
    hash = faker.md5()
    namespace = faker.uuid4()

    handler = ActionSubhandler(
        namespace,
        name,
        hash,
        {},
    )
    if runtime >= timeout:
        with pytest.raises(kopf.HandlerTimeoutError):
            await handler._subhandler(
                logger=logging.getLogger(__name__),
                runtime=datetime.timedelta(seconds=runtime),
                status={},
                annotations={},
                diff=(
                    kopf.DiffItem(
                        kopf.DiffOperation.CHANGE,
                        ("spec", "cluster", "version"),
                        "4.6.1",
                        "4.6.4",
                    ),
                ),
            )
        await assert_wait_for(
            True,
            was_notification_sent,
            mock_send_notification,
            mock.call(
                namespace,
                name,
                WebhookEvent.FEEDBACK,
                WebhookFeedbackPayload(
                    message=(
                        "ActionSubhandler.handle has timed out"
                        f" after {datetime.timedelta(seconds=runtime)}."
                    ),
                    operation=WebhookOperation.UPDATE,
                    action=WebhookAction.UPGRADE,
                ),
                WebhookStatus.FAILURE,
                mock.ANY,
            ),
            err_msg="Exception notification was not sent.",
            timeout=DEFAULT_TIMEOUT,
        )
    else:
        res = await handler._subhandler(
            logger=logging.getLogger(__name__),
            runtime=datetime.timedelta(seconds=runtime),
            status={},
            annotations={},
        )
        assert res["result"] == {"success": True}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "diff_item, expected_action",
    [
        (
            (
                kopf.DiffOperation.CHANGE,
                ("spec", "cluster", "version"),
                "4.6.1",
                "4.6.4",
            ),
            WebhookAction.UPGRADE,
        ),
        (
            (
                kopf.DiffOperation.ADD,
                (),
                None,
                {
                    "accessKeyId": {
                        "secretKeyRef": {
                            "key": "access-key-id",
                            "name": "abc",
                        }
                    },
                    "basePath": {
                        "secretKeyRef": {
                            "key": "base-path",
                            "name": "abc",
                        }
                    },
                    "bucket": {
                        "secretKeyRef": {
                            "key": "bucket",
                            "name": "abc",
                        }
                    },
                    "secretAccessKey": {
                        "secretKeyRef": {
                            "key": "secret-access-key",
                            "name": "abc",
                        }
                    },
                    "snapshot": "20230320123456",
                    "tables": ["all"],
                },
            ),
            WebhookAction.RESTORE_SNAPSHOT,
        ),
        (
            (
                kopf.DiffOperation.CHANGE,
                ("spec", "nodes", "data"),
                [
                    {
                        "name": "hot",
                        "replicas": 1,
                    }
                ],
                [
                    {
                        "name": "hot",
                        "replicas": 2,
                    }
                ],
            ),
            WebhookAction.SCALE,
        ),
        (
            (
                kopf.DiffOperation.CHANGE,
                ("spec", "nodes", "data"),
                [
                    {
                        "name": "hot",
                        "replicas": 1,
                        "resources": {
                            "heapRatio": 0.25,
                            "limits": {"cpu": 2, "memory": "8Gi"},
                            "requests": {"cpu": 2, "memory": "8Gi"},
                        },
                    }
                ],
                [
                    {
                        "name": "hot",
                        "replicas": 1,
                        "resources": {
                            "heapRatio": 0.25,
                            "limits": {"cpu": 4, "memory": "16Gi"},
                            "requests": {"cpu": 4, "memory": "16Gi"},
                        },
                    }
                ],
            ),
            WebhookAction.CHANGE_COMPUTE,
        ),
        (
            (
                kopf.DiffOperation.CHANGE,
                ("spec", "nodes", "data"),
                [
                    {
                        "name": "hot",
                        "replicas": 2,
                        "resources": {
                            "disk": {
                                "count": 1,
                                "size": "16GiB",
                                "storageClass": "default",
                            },
                            "heapRatio": 0.25,
                            "limits": {"cpu": 0.5, "memory": "2Gi"},
                        },
                    }
                ],
                [
                    {
                        "name": "hot",
                        "replicas": 2,
                        "resources": {
                            "disk": {
                                "count": 1,
                                "size": "32Gi",
                                "storageClass": "default",
                            },
                            "heapRatio": 0.25,
                            "limits": {"cpu": 0.5, "memory": "2Gi"},
                        },
                    }
                ],
            ),
            WebhookAction.EXPAND_STORAGE,
        ),
        (
            (
                kopf.DiffOperation.CHANGE,
                ("spec", "cluster", "unknownField"),
                "1",
                "2",
            ),
            WebhookAction.UNKNOWN,
        ),
    ],
)
@mock.patch("crate.operator.webhooks.webhook_client.send_notification")
async def test_get_action_for_diff(
    mock_send_notification: mock.AsyncMock,
    diff_item,
    expected_action,
    faker,
):
    name = faker.domain_word()
    hash = faker.md5()
    namespace = faker.uuid4()

    handler = ActionSubhandler(
        namespace,
        name,
        hash,
        {},
    )
    with pytest.raises(kopf.HandlerTimeoutError):
        await handler._subhandler(
            logger=logging.getLogger(__name__),
            runtime=datetime.timedelta(seconds=30),
            status={},
            annotations={},
            diff=(kopf.DiffItem(*diff_item),),
        )
    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        mock.call(
            namespace,
            name,
            WebhookEvent.FEEDBACK,
            WebhookFeedbackPayload(
                message=(
                    "ActionSubhandler.handle has timed out"
                    f" after {datetime.timedelta(seconds=30)}."
                ),
                operation=WebhookOperation.UPDATE,
                action=expected_action,
            ),
            WebhookStatus.FAILURE,
            mock.ANY,
        ),
        err_msg="Exception notification was not sent.",
        timeout=DEFAULT_TIMEOUT,
    )
