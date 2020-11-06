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

import asyncio

import kopf
from kubernetes_asyncio.client import AppsV1Api


async def update_statefulset(
    apps: AppsV1Api, namespace: str, sts_name: str, crate_image: str
):
    await apps.patch_namespaced_stateful_set(
        namespace=namespace,
        name=sts_name,
        body={"spec": {"rollingUpdate": None, "updateStrategy": {"type": "OnDelete"}}},
    )
    await apps.patch_namespaced_stateful_set(
        namespace=namespace,
        name=sts_name,
        body={
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{"name": "crate", "image": crate_image}],
                        "initContainers": [
                            {"name": "mkdir-heapdump", "image": crate_image}
                        ],
                    }
                }
            }
        },
    )


async def upgrade_cluster(apps: AppsV1Api, namespace: str, name: str, body: kopf.Body):
    """
    Update the Docker image in all StatefulSets for the cluster.

    For the changes to take affect, the cluster needs to be restarted.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the ``CrateDB`` custom resource.
    :param body: The full body of the ``CrateDB`` custom resource per
        :class:`kopf.Body`.
    """
    crate_image = (
        body.spec["cluster"]["imageRegistry"] + ":" + body.spec["cluster"]["version"]
    )

    updates = []
    if "master" in body.spec["nodes"]:
        updates.append(
            update_statefulset(apps, namespace, f"crate-master-{name}", crate_image)
        )
    updates.extend(
        [
            update_statefulset(
                apps, namespace, f"crate-data-{node_spec['name']}-{name}", crate_image
            )
            for node_spec in body.spec["nodes"]["data"]
        ]
    )

    await asyncio.gather(*updates)
