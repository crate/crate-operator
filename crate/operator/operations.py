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
import logging
from typing import Any, Dict, List, Tuple, cast

import kopf
from kubernetes_asyncio.client import CoreV1Api, V1PodList
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.constants import (
    BACKOFF_TIME,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_NODE_NAME,
    LABEL_PART_OF,
)
from crate.operator.cratedb import connection_factory, is_cluster_healthy
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.kubeapi import get_host, get_system_user_password
from crate.operator.utils.state import State


def get_total_nodes_count(nodes: Dict[str, Any]) -> int:
    """
    Calculate the total number nodes a CrateDB cluster should have on startup.

    When starting CrateDB it's important to know the expected number of nodes
    in a cluster. The function takes the ``spec.nodes`` from the CrateDB custom
    resource and sums up all desired replicas for all nodes defined therein.

    :param nodes: The ``spec.nodes`` from a CrateDB custom resource.
    """
    total = 0
    if "master" in nodes:
        total += nodes["master"]["replicas"]
    for node in nodes["data"]:
        total += node["replicas"]
    return total


async def get_pods_in_cluster(
    core: CoreV1Api, namespace: str, name: str
) -> Tuple[Tuple[str], Tuple[str]]:
    """
    Return a two-tuple with two tuples, the first containing all pod IDs, the
    second the corresponding pod names within the cluster.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace where to look up the CrateDB
        cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    """
    labels = {
        LABEL_COMPONENT: "cratedb",
        LABEL_MANAGED_BY: "crate-operator",
        LABEL_NAME: name,
        LABEL_PART_OF: "cratedb",
    }
    label_selector = ",".join(f"{k}={v}" for k, v in labels.items())

    all_pods: V1PodList = await core.list_namespaced_pod(
        namespace=namespace, label_selector=label_selector
    )
    return cast(
        Tuple[Tuple[str], Tuple[str]],
        tuple(zip(*[(p.metadata.uid, p.metadata.name) for p in all_pods.items])),
    )


async def get_pods_in_statefulset(
    core: CoreV1Api, namespace: str, name: str, node_name: str
) -> List[Dict[str, str]]:
    """
    Return a list of all pod IDs and names belonging to a given StatefulSet.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace where to look up CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param node_name: Either ``"master"`` for dedicated master nodes, or the
        ``name`` for a data node spec. Used to determine which StatefulSet to
        of the cluster should be "restarted".
    """
    labels = {
        LABEL_COMPONENT: "cratedb",
        LABEL_MANAGED_BY: "crate-operator",
        LABEL_NAME: name,
        LABEL_NODE_NAME: node_name,
        LABEL_PART_OF: "cratedb",
    }
    label_selector = ",".join(f"{k}={v}" for k, v in labels.items())

    all_pods: V1PodList = await core.list_namespaced_pod(
        namespace=namespace, label_selector=label_selector
    )
    return [{"uid": p.metadata.uid, "name": p.metadata.name} for p in all_pods.items]


async def restart_cluster(
    core: CoreV1Api,
    namespace: str,
    name: str,
    old: kopf.Body,
    logger: logging.Logger,
    patch: kopf.Patch,
    status: kopf.Status,
) -> None:
    """
    Perform a rolling restart of the CrateDB cluster ``name`` in ``namespace``.

    One node at a time, this function will terminate first the master nodes and
    then the data nodes in the cluster. After triggering a pod's termination,
    the operator will wait for that pod to be terminated and gone. It will then
    wait for the cluster to have the desired number of nodes again and for the
    cluster to be in a ``GREEN`` state, before terminating the next pod.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace where to look up CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param old: The old resource body.
    """
    pending_pods: List[Dict[str, str]] = status.get("pendingPods") or []
    if not pending_pods:
        if "master" in old["spec"]["nodes"]:
            pending_pods.extend(
                await get_pods_in_statefulset(core, namespace, name, "master")
            )
        for node_spec in old["spec"]["nodes"]["data"]:
            pending_pods.extend(
                await get_pods_in_statefulset(core, namespace, name, node_spec["name"])
            )
        patch.status["pendingPods"] = pending_pods

    if not pending_pods:
        # We're all done
        patch.status["pendingPods"] = None  # Remove attribute from status stanza
        return

    next_pod_uid = pending_pods[0]["uid"]
    next_pod_name = pending_pods[0]["name"]

    all_pod_uids, all_pod_names = await get_pods_in_cluster(core, namespace, name)
    if next_pod_uid in all_pod_uids:
        # The next to-be-terminated pod still appears to be running.
        logger.info("Terminating pod '%s'", next_pod_name)
        # Trigger deletion of Pod.
        # This may take a while as it tries to gracefully stop the containers
        # of the Pod.
        await core.delete_namespaced_pod(namespace=namespace, name=next_pod_name)
        raise kopf.TemporaryError(
            f"Waiting for pod {next_pod_name} ({next_pod_uid}) to be terminated.",
            delay=BACKOFF_TIME,
        )
    elif next_pod_name in all_pod_names:
        total_nodes = get_total_nodes_count(old["spec"]["nodes"])
        # The new pod has been spawned. Only a matter of time until it's ready.
        password, host = await asyncio.gather(
            get_system_user_password(core, namespace, name),
            get_host(core, namespace, name),
        )
        conn_factory = connection_factory(host, password)
        if await is_cluster_healthy(conn_factory, total_nodes, logger):
            pending_pods.pop(0)  # remove the first item in the list

            if pending_pods:
                patch.status["pendingPods"] = pending_pods

                raise kopf.TemporaryError(
                    "Scheduling rerun because there are pods to be restarted", delay=5
                )
            else:
                # We're all done
                patch.status["pendingPods"] = None  # Remove attribute from `.status`
                return
        else:
            raise kopf.TemporaryError(
                "Cluster is not healthy yet.", delay=BACKOFF_TIME / 2.0
            )
    else:
        raise kopf.TemporaryError(
            "Scheduling rerun because there are pods to be restarted",
            delay=BACKOFF_TIME,
        )


class RestartSubHandler(StateBasedSubHandler):
    state = State.RESTART

    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        old: kopf.Body,
        logger: logging.Logger,
        patch: kopf.Patch,
        status: kopf.Status,
        **kwargs: Any,
    ):
        async with ApiClient() as api_client:
            core = CoreV1Api(api_client)
            await restart_cluster(core, namespace, name, old, logger, patch, status)

        await self.send_notifications(logger)
