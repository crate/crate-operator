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
    WebhookOperation,
    WebhookPermanentErrorPayload,
    WebhookStatus,
)

from .utils import DEFAULT_TIMEOUT, assert_wait_for, was_notification_sent


class ActionSubhandler(StateBasedSubHandler):
    """A subhandler for testing timeout and error decorators"""

    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=20)
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
            )
        await assert_wait_for(
            True,
            was_notification_sent,
            mock_send_notification,
            mock.call(
                namespace,
                name,
                WebhookEvent.ERROR,
                WebhookPermanentErrorPayload(
                    reason=(
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
            status={},
            annotations={},
        )
        assert res["result"] == {"success": True}
