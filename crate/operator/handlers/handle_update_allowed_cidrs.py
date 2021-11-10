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

import kopf
from kopf import DiffItem
from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.api_client import ApiClient


async def update_service_allowed_cidrs(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    logger: logging.Logger,
):
    change: DiffItem = diff[0]
    logger.info(f"Updating load balancer source ranges to {change.new}")

    async with ApiClient() as api_client:
        core = CoreV1Api(api_client)
        # This also runs on creation events, so we want to double check that the service
        # exists before attempting to do anything.
        services = await core.list_namespaced_service(namespace=namespace)
        service = next(
            (svc for svc in services.items if svc.metadata.name == f"crate-{name}"),
            None,
        )
        if not service:
            return

        await core.patch_namespaced_service(
            name=f"crate-{name}",
            namespace=namespace,
            body={"spec": {"loadBalancerSourceRanges": change.new}},
        )
