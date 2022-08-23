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

from .webhooks import (
    WebhookEvent,
    WebhookInfoChangedPayload,
    WebhookStatus,
    webhook_client,
)


async def notify_service_ip(
    namespace: str, cluster_id: str, new_ip: str, logger: logging.Logger
):
    """
    Notify the configured webhooks URL about a service obtaining an IP address.
    """
    payload = WebhookInfoChangedPayload(external_ip=new_ip)
    await webhook_client.send_notification(
        namespace,
        cluster_id,
        WebhookEvent.INFO_CHANGED,
        payload,
        WebhookStatus.SUCCESS,
        logger,
        unsafe=True,
    )
