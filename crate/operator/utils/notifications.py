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


import logging

from crate.operator.utils import crate
from crate.operator.webhooks import WebhookOperation, WebhookStatus


async def send_update_progress_notification(
    *, namespace: str, name: str, message: str, logger: logging.Logger
):
    await crate.send_feedback_notification(
        namespace=namespace,
        name=name,
        message=message,
        operation=WebhookOperation.UPDATE,
        status=WebhookStatus.IN_PROGRESS,
        logger=logger,
    )
