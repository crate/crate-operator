import asyncio
import functools
import logging
from typing import Any, Awaitable, Callable, Dict

from aiopg import Connection
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi, V1Pod, V1PodList

from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    BACKOFF_TIME,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_NODE_NAME,
    LABEL_PART_OF,
    RESOURCE_CRATEDB,
)
from crate.operator.cratedb import (
    connection_factory,
    is_healthy,
    wait_for_healthy_cluster,
)
from crate.operator.utils.kubeapi import get_public_ip, get_system_user_password

logger = logging.getLogger(__name__)


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


async def wait_for_termination(
    pod: V1Pod, get_pods: Callable[[], Awaitable[V1PodList]]
) -> None:
    """
    Repeatedly and indefinitely check if ``pod``'s ``uid`` is still around.

    :param pod: The pod that is being terminated.
    :param get_pods: A callable that returns an awaitable list of pods.
    """
    uid = pod.metadata.uid
    pods = await get_pods()
    while uid in (p.metadata.uid for p in pods.items):
        logger.info(
            "Waiting for pod '%s' with uid='%s' to be terminated.",
            pod.metadata.name,
            uid,
        )
        await asyncio.sleep(BACKOFF_TIME / 2.0)
        pods = await get_pods()


async def restart_statefulset(
    core: CoreV1Api,
    connection_factory: Callable[[], Connection],
    namespace: str,
    name: str,
    node_name: str,
    total_nodes: int,
) -> None:
    """
    Perform a rolling restart of the nodes in the Kubernetes StatefulSet
    ``name`` in ``namespace``.

    :param core: An instance of the Kubernetes Core V1 API.
    :param connection_factory: A function establishes a connection to the
        CrateDB cluster to be used to SQL queries checking for health, etc.
    :param namespace: The Kubernetes namespace where to look up CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param node_name: Either ``"master"`` for dedicated master nodes, or the
        ``name`` for a data node spec. Used to determine which StatefulSet to
        of the cluster should be "restarted".
    :param total_nodes: The total number of nodes that the cluster should
        consist of, per the CrateDB cluster spec.
    """
    async with connection_factory() as conn:
        async with conn.cursor() as cursor:
            if not await is_healthy(cursor):
                raise ValueError("Unhealthy cluster")

    labels = {
        LABEL_COMPONENT: "cratedb",
        LABEL_MANAGED_BY: "crate-operator",
        LABEL_NAME: name,
        LABEL_NODE_NAME: node_name,
        LABEL_PART_OF: "cratedb",
    }

    get_pods = functools.partial(
        core.list_namespaced_pod,
        namespace=namespace,
        label_selector=",".join(f"{k}={v}" for k, v in labels.items()),
    )

    pods = await get_pods()
    for pod in pods.items:
        logger.info("Terminating pod '%s'", pod.metadata.name)
        # Trigger deletion of Pod.
        # This may take a while as it tries to gracefully stop the containers
        # of the Pod.
        await core.delete_namespaced_pod(namespace=namespace, name=pod.metadata.name)

        # Waiting for the pod to go down. This ensures we won't try to connect
        # to the killed pod through the load balancing service.
        await wait_for_termination(pod, get_pods)

        # Once the Crate node is terminated, we can start checking the health
        # of the cluster.
        await wait_for_healthy_cluster(connection_factory, total_nodes)
        logger.info("Cluster has recovered. Moving on ...")


async def restart_cluster(namespace: str, name: str) -> None:
    """
    Perform a rolling restart of the CrateDB cluster ``name`` in ``namespace``.

    One node at a time, this function will terminate first the master nodes and
    then the data nodes in the cluster. After triggering a pod's termination,
    the operator will wait for that pod to be terminated and gone. It will then
    wait for the cluster to have the desired number of nodes again and for the
    cluster to be in a ``GREEN`` state.

    :param namespace: The Kubernetes namespace where to look up CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    """
    coapi = CustomObjectsApi()
    core = CoreV1Api()

    cluster = await coapi.get_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace,
        name=name,
    )
    password = await get_system_user_password(namespace, name, core)
    if config.TESTING:
        # During testing we need to connect to the cluster via its public IP
        # address, because the operator isn't running inside the Kubernetes
        # cluster.
        host = await get_public_ip(core, namespace, name)
    else:
        host = f"crate-{name}.{namespace}"
    conn_factory = connection_factory(host, password)

    total_nodes = get_total_nodes_count(cluster["spec"]["nodes"])

    if "master" in cluster["spec"]["nodes"]:
        await restart_statefulset(
            core, conn_factory, namespace, name, "master", total_nodes
        )
    for node_spec in cluster["spec"]["nodes"]["data"]:
        await restart_statefulset(
            core, conn_factory, namespace, name, node_spec["name"], total_nodes
        )
