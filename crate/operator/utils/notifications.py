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
from datetime import datetime
from typing import Any

import kopf

from crate.operator.utils import crate
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.webhooks import WebhookOperation, WebhookStatus


async def send_operation_progress_notification(
    *,
    namespace: str,
    name: str,
    message: str,
    logger: logging.Logger,
    status: WebhookStatus,
    operation: WebhookOperation,
):
    await crate.send_feedback_notification(
        namespace=namespace,
        name=name,
        message=message,
        operation=operation,
        status=status,
        logger=logger,
    )


class FlushNotificationsSubHandler(StateBasedSubHandler):
    """
    Ensures any previously registered and pending notification is sent.
    """

    @crate.on.error(error_handler=crate.send_update_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        status: kopf.Status,
        **kwargs: Any,
    ):
        logger.info(f"{datetime.now()} NOTIFY-UPDATE handler started!")
        await self.send_notifications(logger)
        logger.info(f"{datetime.now()} NOTIFY-UPDATE handler finished!")
