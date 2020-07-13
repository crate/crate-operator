import asyncio

import kopf
from kubernetes_asyncio.client import AppsV1Api

from crate.operator.prometheus import prometheus


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
                    "spec": {"containers": [{"name": "crate", "image": crate_image}]}
                }
            }
        },
    )


async def upgrade_cluster(namespace: str, name: str, body: kopf.Body):
    """
    Update the Docker image in all StatefulSets for the cluster.

    For the changes to take affect, the cluster needs to be restarted.

    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the ``CrateDB`` custom resource.
    :param body: The full body of the ``CrateDB`` custom resource per
        :class:`kopf.Body`.
    """
    apps = AppsV1Api()
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

    prometheus.track_clusters_upgraded_total(namespace, name)
