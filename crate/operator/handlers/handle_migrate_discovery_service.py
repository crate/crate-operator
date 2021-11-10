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

from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.constants import Port


async def migrate_discovery_service(
    namespace: str,
    name: str,
    logger: logging.Logger,
):
    if not name.startswith("crate-discovery-"):
        return

    async with ApiClient() as api_client:
        core = CoreV1Api(api_client)

        service = await core.read_namespaced_service(name, namespace)

        http_port = next(
            (port for port in service.spec.ports if port.name == "http"), None
        )

        if http_port:
            return

        logger.info("Found old discovery service w/o HTTP port, patching: %s", name)

        await core.patch_namespaced_service(
            name,
            namespace,
            body={
                "spec": {
                    "ports": [
                        {"name": "cluster", "port": Port.TRANSPORT.value},
                        {"name": "http", "port": Port.HTTP.value},
                        {"name": "psql", "port": Port.POSTGRES.value},
                    ]
                }
            },
        )
