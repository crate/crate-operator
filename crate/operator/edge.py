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
