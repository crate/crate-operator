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
                status={
                    "subhandlerStartedAt": {
                        "ActionSubhandler": {
                            "started": int(datetime.datetime.utcnow().timestamp())
                            - runtime,
                            "ref": hash,
                        }
                    }
                },
                annotations={},
                patch={},
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
            status={
                "subhandlerStartedAt": {
                    "ActionSubhandler": {
                        "started": int(datetime.datetime.utcnow().timestamp())
                        - runtime,
                        "ref": hash,
                    }
                }
            },
            annotations={},
            patch={},
        )
        assert res["result"] == {"success": True}
