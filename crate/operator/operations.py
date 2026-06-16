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

import asyncio
import datetime
import logging
import pkgutil
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, cast

import kopf
import yaml
from aiopg import Cursor
from kopf import TemporaryError
from kubernetes_asyncio.client import (
    ApiException,
    AppsV1Api,
    BatchV1Api,
    CoreV1Api,
    CustomObjectsApi,
    V1ConfigMap,
    V1CronJobList,
    V1JobList,
    V1JobStatus,
    V1Namespace,
    V1PersistentVolumeClaimList,
    V1PodList,
    V1Service,
    V1ServiceList,
    V1StatefulSet,
    V1StatefulSetList,
)
from kubernetes_asyncio.client.models.v1_delete_options import V1DeleteOptions
from psycopg2 import DatabaseError, OperationalError
from psycopg2.extensions import quote_ident

from crate.operator.config import config
from crate.operator.constants import (
    BACKUP_METRICS_DEPLOYMENT_NAME,
    CONNECT_TIMEOUT,
    GRAND_CENTRAL_RESOURCE_PREFIX,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_NODE_NAME,
    LABEL_PART_OF,
    Port,
)
from crate.operator.cratedb import (
    are_snapshots_in_progress,
    connection_factory,
    get_connection,
    is_cluster_healthy,
    reset_cluster_setting,
    set_cluster_setting,
)
from crate.operator.create import get_owner_references, recreate_services
from crate.operator.exposure import (
    create_traefik_resources,
    delete_service as delete_clusterip_service,
    delete_traefik_resources,
)
from crate.operator.grand_central import (
    create_grand_central_exposure,
    delete_grand_central_ingress,
    delete_grand_central_traefik_resources,
    read_grand_central_deployment,
    read_grand_central_httproute,
    read_grand_central_ingress,
)
from crate.operator.sql import execute_sql
from crate.operator.utils import crate
from crate.operator.utils.jwt import crate_version_supports_jwt
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.kopf import StateBasedSubHandler, subhandler_partial
from crate.operator.utils.kubeapi import (
    call_kubeapi,
    get_cratedb_resource,
    get_host,
    get_system_user_password,
)
from crate.operator.utils.notifications import send_operation_progress_notification
from crate.operator.webhooks import (
    WebhookAction,
    WebhookEvent,
    WebhookOperation,
    WebhookStatus,
    WebhookTemporaryFailurePayload,
)


def get_total_nodes_count(nodes: Dict[str, Any], type: str = "all") -> int:
    """
    Calculate the total number nodes a CrateDB cluster should have on startup.

    When starting CrateDB it's important to know the expected number of nodes
    in a cluster. The function takes the ``spec.nodes`` from the CrateDB custom
    resource and sums up all desired replicas for all nodes defined therein.

    :param nodes: The ``spec.nodes`` from a CrateDB custom resource.
    :param type: Optionally get only the number of ``data`` or ``master`` nodes.
    """
    total = {
        "master": 0,
        "data": 0,
    }
    if "master" in nodes:
        total["master"] += nodes["master"]["replicas"]
    for node in nodes["data"]:
        total["data"] += node["replicas"]
    total["all"] = total["master"] + total["data"]
    return total.get(type, 0)


def get_master_nodes_names(nodes: Dict[str, Any]) -> List[str]:
    """
    Return the list of nodes service as master nodes in a CrateDB cluster.

    The function takes the ``spec.nodes`` from a CrateDB custom resource
    and checks if it defines explicit master nodes or not. Based on that, it
    will return the list of node names.

    :param nodes: The ``spec.nodes`` from a CrateDB custom resource.
    """
    if "master" in nodes:
        # We have dedicated master nodes. They are going to form the cluster
        # state.
        return [f"master-{i}" for i in range(nodes["master"]["replicas"])]
    else:
        node = nodes["data"][0]
        node_name = node["name"]
        return [f"data-{node_name}-{i}" for i in range(node["replicas"])]


@dataclass(frozen=True)
class NodeGroup:
    """
    A single CrateDB node group: the dedicated masters, or one data group.

    Lets callers treat masters and data groups uniformly while keeping the CRD
    asymmetry explicit: ``spec.nodes.master`` is a single object *without* a
    ``name``, whereas ``spec.nodes.data`` is a list of named groups.
    """

    #: Logical group name -- ``"master"`` for the dedicated masters, otherwise
    #: the data group's own name (e.g. ``"hot"``).
    name: str
    #: The node group's spec dict (``replicas``, ``resources``, ...).
    spec: Dict[str, Any]
    #: Whether this is the dedicated master group.
    is_master: bool

    @property
    def node_name_prefix(self) -> str:
        """
        Prefix of the node names CrateDB reports in ``sys.nodes`` (and the
        infix of the StatefulSet name): ``"master"`` (``master-0``, ...) or
        ``"data-<group>"`` (``data-hot-0``, ...).
        """
        return "master" if self.is_master else f"data-{self.name}"

    def statefulset_name(self, cluster_name: str) -> str:
        """
        The StatefulSet name for this group: ``crate-master-<cluster_name>``
        or ``crate-data-<group>-<cluster_name>``.
        """
        return f"crate-{self.node_name_prefix}-{cluster_name}"


def iter_node_groups(nodes: Dict[str, Any]) -> List[NodeGroup]:
    """
    Enumerate a cluster's node groups, dedicated masters **first**.

    Masters are yielded first so callers that must order operations
    master-before-data (cluster formation, resume) can just iterate. Legacy
    clusters without ``spec.nodes.master`` yield only their data groups.

    :param nodes: The ``spec.nodes`` from a CrateDB custom resource.
    """
    groups: List[NodeGroup] = []
    if "master" in nodes:
        groups.append(NodeGroup(name="master", spec=nodes["master"], is_master=True))
    for node in nodes["data"]:
        groups.append(NodeGroup(name=node["name"], spec=node, is_master=False))
    return groups


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


async def get_pvcs_in_namespace(
    core: CoreV1Api, namespace: str, name: str, node_name: str
) -> List[Dict[str, str]]:
    labels = {
        LABEL_COMPONENT: "cratedb",
        LABEL_NAME: name,
        LABEL_NODE_NAME: node_name,
    }
    label_selector = ",".join(f"{k}={v}" for k, v in labels.items())

    all_pvcs: V1PersistentVolumeClaimList = (
        await core.list_namespaced_persistent_volume_claim(
            namespace=namespace, label_selector=label_selector
        )
    )
    return [{"uid": p.metadata.uid, "name": p.metadata.name} for p in all_pvcs.items]


async def get_pods_in_deployment(
    core: CoreV1Api, namespace: str, name: str
) -> List[Dict[str, str]]:
    """
    Return a list of all pod IDs and names belonging to a given Deployment.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace where to look up CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    """
    labels = {
        LABEL_MANAGED_BY: "crate-operator",
        LABEL_NAME: name,
        LABEL_PART_OF: "cratedb",
    }
    label_selector = ",".join(f"{k}={v}" for k, v in labels.items())

    all_pods: V1PodList = await core.list_namespaced_pod(
        namespace=namespace, label_selector=label_selector
    )
    return [{"uid": p.metadata.uid, "name": p.metadata.name} for p in all_pods.items]


async def get_namespace_resource(namespace_name: str) -> V1Namespace:
    """
    Return the namespace for the given name.

    :param namespace_name: The Kubernetes namespace name to look up.
    """
    async with GlobalApiClient() as api_client:
        core = CoreV1Api(api_client)
        namespaces = await core.list_namespace()
        for ns in namespaces.items:
            if ns.metadata.name == namespace_name:
                return ns
        return None


async def is_namespace_terminating(namespace_name: str) -> bool:
    """
    Determines if the namespace identified by the given name is terminating or not.

    :param namespace_name: The Kubernetes namespace name to look up.
    """
    namespace_obj = await get_namespace_resource(namespace_name)
    return namespace_obj and namespace_obj.status.phase == "Terminating"


async def check_all_data_nodes_gone(
    core: CoreV1Api,
    namespace: str,
    name: str,
    old: kopf.Body,
):
    """
    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param old: The old resource body.
    :raises: A :class:`kopf.TemporaryError` when nodes are still available
    """
    pending_pods = []
    for node_spec in old["spec"]["nodes"]["data"]:
        pending_pods.extend(
            await get_pods_in_statefulset(core, namespace, name, node_spec["name"])
        )
    if pending_pods:
        raise kopf.TemporaryError(
            f"Waiting for pods to be gone {pending_pods}", delay=5
        )


async def check_all_data_nodes_present(
    connection_factory,
    old_replicas: int,
    new_replicas: int,
    node_prefix: str,
    logger: logging.Logger,
):
    """
    :param connection_factory: A callable that allows the operator to connect
        to the database. We regularly need to reconnect to ensure the
        connection wasn't closed because it was opened to a CrateDB node that
        was shut down since the connection was opened.
    :param old_replicas: The number of replicas in a StatefulSet before
        scaling.
    :param new_replicas: The number of replicas in a StatefulSet after scaling.
    :param node_prefix: The prefix of the node names in CrateDB.
    :raises: A :class:`kopf.TemporaryError` when nodes are missing
    """
    full_node_list = [
        f"{node_prefix}-{i}" for i in range(max(old_replicas, new_replicas))
    ]
    try:
        async with connection_factory() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT name FROM sys.nodes WHERE name = ANY(%s)
                    """,
                    (full_node_list,),
                )
                rows = await cursor.fetchall()
                available_nodes = {r[0] for r in rows} if rows else set()
                candidate_node_names = {
                    f"{node_prefix}-{i}"
                    for i in range(
                        min(old_replicas, new_replicas), max(old_replicas, new_replicas)
                    )
                }
                if old_replicas < new_replicas:
                    # scale up. Wait for missing nodes
                    if not candidate_node_names.issubset(available_nodes):
                        missing_nodes = ", ".join(sorted(candidate_node_names))

                        raise kopf.TemporaryError(
                            f"Waiting for nodes {missing_nodes} to be present.",
                            delay=15,
                        )
                else:
                    logger.info(
                        "No need to wait for nodes with prefix '%s', since the "
                        "number of replicas has not been increased.",
                        node_prefix,
                    )
    except (DatabaseError, OperationalError):
        raise kopf.TemporaryError("Waiting for database connection.", delay=15)


async def check_backup_metrics_pod_gone(
    core: CoreV1Api,
    namespace: str,
    name: str,
):
    """
    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :raises: A :class:`kopf.TemporaryError` when node is still present.
    """
    backup_metrics_pods = await get_pods_in_deployment(core, namespace, name)
    backup_metrics_name = BACKUP_METRICS_DEPLOYMENT_NAME.format(name=name)
    if any(p["name"].startswith(backup_metrics_name) for p in backup_metrics_pods):
        raise kopf.TemporaryError(
            f"Waiting for backup metrics pod in {backup_metrics_pods} to be gone.",
            delay=5,
        )


async def check_cluster_healthy(
    name: str, namespace: str, apps: AppsV1Api, conn_factory, logger
):
    """
    This looks for all the StatefulSets for this cluster and makes sure that the
    number of nodes in all the STS matches what CrateDB has in the sys.nodes table.

    If we have specific master/hot/cold node types configured these would be
    separate StatefulSets.
    """
    expected_number_of_nodes = 0
    all_sts: V1StatefulSetList = await apps.list_namespaced_stateful_set(namespace)
    for sts in all_sts.items:
        if sts.metadata.labels["app.kubernetes.io/name"] == name:
            expected_number_of_nodes += sts.spec.replicas

    if expected_number_of_nodes == 0:
        return

    if not await is_cluster_healthy(conn_factory, expected_number_of_nodes, logger):
        raise kopf.TemporaryError(
            "Waiting for cluster to be healthy.", delay=config.HEALTH_CHECK_RETRY_DELAY
        )


async def update_statefulset_replicas(
    apps: AppsV1Api,
    namespace: Optional[str],
    sts_name: Optional[str],
    statefulset: Optional[V1StatefulSet],
    replicas: Optional[int],
):
    """
    Call the Kubernetes API and update the number of replicas.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param sts_name: The name for the Kubernetes StatefulSet to update.
    :param replicas: The new number of replicas for the StatefulSet.
    """
    statefulset = statefulset or await apps.read_namespaced_stateful_set(
        namespace=namespace, name=sts_name
    )

    body: Dict[str, Any] = {
        "spec": {},
    }
    if replicas is not None:
        body["spec"]["replicas"] = replicas
        await apps.patch_namespaced_stateful_set(
            namespace=namespace, name=sts_name, body=body
        )


async def update_deployment_replicas(
    apps: AppsV1Api,
    namespace: Optional[str],
    name: Optional[str],
    replicas: Optional[int],
):
    """
    Call the Kubernetes API and update the number of replicas.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the Kubernetes Deployment to update.
    :param replicas: The new number of replicas for the Deployment.
    """
    body: Dict[str, Any] = {
        "spec": {},
    }
    if replicas is not None:
        body["spec"]["replicas"] = replicas
        await apps.patch_namespaced_deployment(
            namespace=namespace, name=name, body=body
        )


async def scale_backup_metrics_deployment(
    namespace: str,
    name: str,
    replicas: int,
):
    """
    Update the number of replicas of a backup-metrics deployment in the
    given namespace.

    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the backup-metrics deployment to update.
    :param replicas: The new number of replicas.
    """
    async with GlobalApiClient() as api_client:
        apps = AppsV1Api(api_client)
        backup_metrics_name = BACKUP_METRICS_DEPLOYMENT_NAME.format(name=name)
        await update_deployment_replicas(apps, namespace, backup_metrics_name, replicas)


async def restart_cluster(
    core: CoreV1Api,
    namespace: str,
    name: str,
    old: kopf.Body,
    logger: logging.Logger,
    patch: kopf.Patch,
    status: kopf.Status,
    action: WebhookAction,
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
        node_index = int(next_pod_name[next_pod_name.rindex("-") + 1 :])
        node_progress = f"{node_index + 1}/{len(all_pod_uids)}"
        await send_operation_progress_notification(
            namespace=namespace,
            name=name,
            message=f"Waiting for node {node_progress} to be terminated.",
            logger=logger,
            status=WebhookStatus.IN_PROGRESS,
            operation=WebhookOperation.UPDATE,
            action=action,
        )
        try:
            conn_factory = await _get_connection_factory(core, namespace, name)

            await set_cluster_setting(
                conn_factory,
                logger,
                setting="cluster.routing.allocation.enable",
                value="new_primaries",
                mode="PERSISTENT",
            )
        except Exception as e:
            logger.info(
                "Setting cluster allocation to 'new_primaries' failed: %s",
                str(e),
            )

        # Trigger deletion of Pod.
        # This may take a while as it tries to gracefully stop the containers
        # of the Pod.
        await core.delete_namespaced_pod(namespace=namespace, name=next_pod_name)
        raise kopf.TemporaryError(
            f"Waiting for pod {next_pod_name} ({next_pod_uid}) to be terminated.",
            delay=15,
        )
    elif next_pod_name in all_pod_names:
        total_nodes = get_total_nodes_count(old["spec"]["nodes"], "all")
        # The new pod has been spawned. Only a matter of time until it's ready.
        node_index = int(next_pod_name[next_pod_name.rindex("-") + 1 :])
        node_progress = f"{node_index + 1}/{len(all_pod_uids)}"
        await send_operation_progress_notification(
            namespace=namespace,
            name=name,
            message=f"Waiting for node {node_progress} to be restarted.",
            logger=logger,
            status=WebhookStatus.IN_PROGRESS,
            operation=WebhookOperation.UPDATE,
            action=action,
        )
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
            try:
                conn_factory = await _get_connection_factory(core, namespace, name)

                await reset_cluster_setting(
                    conn_factory,
                    logger,
                    setting="cluster.routing.allocation.enable",
                )
            except Exception as e:
                logger.info(
                    "Resetting cluster allocation failed: %s",
                    str(e),
                )
            raise kopf.TemporaryError(
                "Cluster is not healthy yet.", delay=config.HEALTH_CHECK_RETRY_DELAY
            )
    else:
        raise kopf.TemporaryError(
            "Scheduling rerun because there are pods to be restarted", delay=15
        )


async def is_service_present(core: CoreV1Api, namespace: str, name: str) -> bool:
    """
    Check if the main data service (crate-<name>) exists.

    :param core: Kubernetes CoreV1Api client.
    :param namespace: Namespace to check.
    :param name: Name of the CrateDB cluster.
    """
    svc_name = f"crate-{name}"
    selector = f"metadata.name={svc_name}"
    svc_list: V1ServiceList = await core.list_namespaced_service(
        namespace=namespace, field_selector=selector
    )
    return len(svc_list.items) >= 1


async def are_traefik_resources_present(namespace: str, name: str) -> bool:
    """
    Check if both IngressRouteTCP resources exist for this cluster.

    :param namespace: Namespace to check.
    :param name: Name of the CrateDB cluster.
    """
    async with GlobalApiClient() as api_client:
        custom_api = CustomObjectsApi(api_client)
        for suffix in ["pg", "http"]:
            try:
                await custom_api.get_namespaced_custom_object(
                    group="traefik.io",
                    version="v1alpha1",
                    namespace=namespace,
                    plural="ingressroutetcps",
                    name=f"crate-{suffix}-{name}",
                )
            except ApiException as e:
                if e.status == 404:
                    return False
                raise
        return True


async def is_grand_central_exposed(
    namespace: str, name: str, use_traefik: bool
) -> bool:
    """
    Return ``True`` if the active grand-central routing resource already exists.

    Checks for an HTTPRoute when ``use_traefik`` is ``True``, or for an nginx
    Ingress otherwise.

    :param namespace: The Kubernetes namespace to check.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param use_traefik: When ``True``, check for an HTTPRoute. When ``False``,
        check for an nginx Ingress.
    """
    if use_traefik:
        return await read_grand_central_httproute(namespace, name) is not None
    else:
        return await read_grand_central_ingress(namespace, name) is not None


async def _recreate_traefik_resources(
    namespace: str, name: str, logger: logging.Logger
):
    """
    Recreate Traefik resources after a resume operation.

    This is called when scaling a cluster back up from 0 replicas and the
    exposure is 'traefik'. It reads the current spec and recreates the
    MiddlewareTCP and IngressRouteTCP resources.

    :param namespace: Kubernetes namespace.
    :param name: Name of the CrateDB cluster.
    :param logger: Logger for operation tracking.
    """
    cratedb = await get_cratedb_resource(namespace, name)
    spec = cratedb["spec"]
    dns_record = spec.get("cluster", {}).get("externalDNS")
    if not dns_record:
        logger.warning("Cannot recreate Traefik resources: externalDNS missing")
        return

    ports_spec = spec.get("ports", {})
    http_port = ports_spec.get("http", Port.HTTP.value)
    postgres_port = ports_spec.get("postgres", Port.POSTGRES.value)
    source_ranges = spec["cluster"].get("allowedCIDRs", None)
    owner_references = get_owner_references(name, cratedb["metadata"])

    await create_traefik_resources(
        owner_references,
        namespace,
        name,
        dns_record,
        source_ranges,
        http_port,
        postgres_port,
        logger,
    )


async def suspend_or_start_cluster(
    apps: AppsV1Api,
    core: CoreV1Api,
    namespace: str,
    name: str,
    old: kopf.Body,
    data_diff_items: kopf.Diff,
    scale_backup_metrics: bool,
    logger: logging.Logger,
):
    """
    Suspend or scale a cluster ``name`` back up, according to the given
    ``data_diff_items``.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param old: The old resource body.
    :param data_diff_items: A list of changes made to the individual
        data node specifications.
    :param scale_backup_metrics: Indicates whether backup metrics Deployment
        should be suspended/started as well. This is usually not the case
        for volume expansion operations.
    """
    cratedb = await get_cratedb_resource(namespace, name)
    exposure = (
        cratedb.get("spec", {}).get("cluster", {}).get("exposure", "loadbalancer")
    )
    use_traefik = exposure == "traefik"

    if data_diff_items:
        for _, field_path, old_replicas, new_replicas in data_diff_items:
            if old_replicas < new_replicas:
                # scale the cluster back up

                # Check if service is present, re-create it if not
                if not await is_service_present(core, namespace, name):
                    await recreate_services(
                        namespace, name, cratedb["spec"], cratedb["metadata"], logger
                    )
                if not use_traefik and not await is_lb_service_ready(
                    core, namespace, name
                ):
                    raise TemporaryError(delay=config.BOOTSTRAP_RETRY_DELAY)

                if use_traefik and not await are_traefik_resources_present(
                    namespace, name
                ):
                    await _recreate_traefik_resources(namespace, name, logger)

                index_path, *_ = field_path
                index = int(index_path)
                node_spec = old["spec"]["nodes"]["data"][index]
                node_name = node_spec["name"]
                sts_name = f"crate-data-{node_name}-{name}"
                statefulset = await apps.read_namespaced_stateful_set(
                    namespace=namespace, name=sts_name
                )
                current_replicas = statefulset.spec.replicas
                if current_replicas != new_replicas:
                    logger.info(f"Scale cluster up to {new_replicas} replicas")
                    await update_statefulset_replicas(
                        apps,
                        namespace,
                        sts_name,
                        statefulset,
                        new_replicas,
                    )
                    if scale_backup_metrics:
                        # scale backup-metrics deployment back up
                        backup_metrics_name = BACKUP_METRICS_DEPLOYMENT_NAME.format(
                            name=name
                        )
                        await update_deployment_replicas(
                            apps, namespace, backup_metrics_name, 1
                        )
                    # scale grand central deployment back up if it exists
                    await suspend_or_start_grand_central(
                        apps, namespace, name, suspend=False, logger=logger
                    )
                await send_operation_progress_notification(
                    namespace=namespace,
                    name=name,
                    message=f"Starting cluster. Scaling back up to {new_replicas} "
                    "nodes. Waiting for node(s) to be present.",
                    logger=logger,
                    status=WebhookStatus.IN_PROGRESS,
                    operation=WebhookOperation.UPDATE,
                    action=(
                        WebhookAction.SUSPEND
                        if old_replicas == 0
                        else WebhookAction.SCALE
                    ),
                )

                conn_factory = await _get_connection_factory(core, namespace, name)

                await check_all_data_nodes_present(
                    conn_factory,
                    old_replicas,
                    new_replicas,
                    f"data-{node_name}",
                    logger,
                )
            elif old_replicas > new_replicas:
                # suspend the cluster -> scale down to 0 replicas
                # First check if the cluster is healthy at all,
                # and prevent scaling down if not.
                conn_factory = await _get_connection_factory(core, namespace, name)
                await check_cluster_healthy(name, namespace, apps, conn_factory, logger)
                index_path, *_ = field_path
                index = int(index_path)
                node_spec = old["spec"]["nodes"]["data"][index]
                node_name = node_spec["name"]
                sts_name = f"crate-data-{node_name}-{name}"
                statefulset = await apps.read_namespaced_stateful_set(
                    namespace=namespace, name=sts_name
                )
                current_replicas = statefulset.spec.replicas
                if current_replicas != new_replicas:
                    await update_statefulset_replicas(
                        apps, namespace, sts_name, statefulset, new_replicas
                    )
                    if scale_backup_metrics:
                        # scale backup-metrics deployment down
                        backup_metrics_name = BACKUP_METRICS_DEPLOYMENT_NAME.format(
                            name=name
                        )
                        await update_deployment_replicas(
                            apps, namespace, backup_metrics_name, 0
                        )
                    # scale grand central deployment down if it exists
                    await suspend_or_start_grand_central(
                        apps, namespace, name, suspend=True, logger=logger
                    )
                await send_operation_progress_notification(
                    namespace=namespace,
                    name=name,
                    message="Suspending cluster.",
                    logger=logger,
                    status=WebhookStatus.IN_PROGRESS,
                    operation=WebhookOperation.UPDATE,
                    action=(
                        WebhookAction.SUSPEND
                        if new_replicas == 0
                        else WebhookAction.SCALE
                    ),
                )
                if scale_backup_metrics:
                    await check_backup_metrics_pod_gone(
                        core,
                        namespace,
                        name,
                    )
                await check_all_data_nodes_gone(core, namespace, name, old)

                # Delete the service and Traefik resources (if any) when suspending
                if use_traefik:
                    # Delete Traefik resources (IngressRouteTCPs and MiddlewareTCP)
                    await delete_traefik_resources(namespace, name)
                    # Also delete the ClusterIP service
                    await delete_clusterip_service(core, namespace, name)
                else:
                    # Delete the LoadBalancer service
                    await delete_lb_service(core, namespace, name)


async def suspend_or_start_grand_central(
    apps: AppsV1Api,
    namespace: str,
    name: str,
    suspend: bool,
    logger: logging.Logger,
):
    """
    Scale the grand-central Deployment to 0 (suspend) or 1 (start) and
    manage its routing resources accordingly.

    On suspend, the Deployment is scaled to 0 and the active routing resource
    (HTTPRoute + Middlewares or nginx Ingress) is deleted so it no longer
    routes traffic. On start, the routing resource is recreated if absent
    before the Deployment is scaled back up.

    Does nothing if no grand-central Deployment exists for this cluster.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param suspend: When ``True``, scale down and delete routing resources.
        When ``False``, recreate routing resources and scale back up.
    :param logger: Logger for operation tracking.
    """
    deployment = await read_grand_central_deployment(namespace=namespace, name=name)
    if not deployment:
        return

    # Determine which exposure mode is active
    cratedb = await get_cratedb_resource(namespace, name)
    spec = cratedb["spec"]
    meta = cratedb["metadata"]
    exposure = spec.get("cluster", {}).get("exposure", "loadbalancer")
    use_traefik = exposure == "traefik"

    if suspend:
        # Scale the deployment to 0 first, then remove the routing resource
        await update_deployment_replicas(
            apps,
            namespace,
            f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
            0,
        )
        if use_traefik:
            await delete_grand_central_traefik_resources(namespace, name, logger)
        else:
            await delete_grand_central_ingress(namespace, name, logger)
    else:
        # Recreate the routing resource, then scale back up
        if not await is_grand_central_exposed(namespace, name, use_traefik):
            await create_grand_central_exposure(
                namespace=namespace,
                name=name,
                spec=spec,
                meta=meta,
                logger=logger,
                use_traefik=use_traefik,
            )
        await update_deployment_replicas(
            apps,
            namespace,
            f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
            1,
        )


async def _get_connection_factory(core, namespace: str, name: str):
    """
    Returns a connection factory.
    Requires the load balancer to be ready.
    """
    host = await get_host(core, namespace, name)
    password = await get_system_user_password(core, namespace, name)
    conn_factory = connection_factory(host, password)
    return conn_factory


async def delete_lb_service(core: CoreV1Api, namespace: str, name: str):
    svc: V1Service = await get_lb_service(core, namespace, name)

    if svc:
        svc_name = f"crate-{name}"
        await core.delete_namespaced_service(
            name=svc_name, namespace=namespace, body=V1DeleteOptions()
        )


async def is_lb_service_present(core: CoreV1Api, namespace: str, name: str) -> bool:
    return await get_lb_service(core, namespace, name) is not None


async def is_lb_service_ready(core: CoreV1Api, namespace: str, name: str) -> bool:
    lb = await get_lb_service(core, namespace, name)
    if (
        not lb
        or not lb.status
        or not lb.status.load_balancer
        or not lb.status.load_balancer.ingress
        or not lb.status.load_balancer.ingress[0]
        or (
            not lb.status.load_balancer.ingress[0].ip
            and not lb.status.load_balancer.ingress[0].hostname
        )
    ):
        return False
    else:
        return True


async def get_lb_service(core: CoreV1Api, namespace: str, name: str) -> V1Service:
    """
    Returns true if the load balancer service related to a StatefulSet exists.
    Returns false otherwise.
    """
    svc_name = f"crate-{name}"
    selector = f"metadata.name={svc_name}"
    svc_list: V1ServiceList = await core.list_namespaced_service(
        namespace=namespace, field_selector=selector
    )

    if len(svc_list.items) >= 1:
        return svc_list.items[0]
    else:
        return None


def get_sql_exporter_collectors(sql_exporter_config) -> list:
    # Parse the config yaml file to get the defined collectors and load them
    parsed_sql_exporter_config = yaml.load(
        sql_exporter_config.decode(), Loader=yaml.FullLoader
    )
    return parsed_sql_exporter_config["target"]["collectors"]


def add_sql_exporter_collectors_to_configmap(
    config_map: V1ConfigMap, collectors: list
) -> V1ConfigMap:
    # Add the yaml collectors to the configmap dynamically
    for collector in collectors:
        # Remove the `_collector` suffix from the collector name if present
        if collector.endswith("_collector"):
            collector = collector[:-10]
        yaml_filename = f"{collector}-collector.yaml"  # Notice the `-` instead of `_`!
        collector_config = pkgutil.get_data("crate.operator", f"data/{yaml_filename}")

        if collector_config is None:
            raise FileNotFoundError(f"Could not load config for collector {collector}")
        config_map.data[yaml_filename] = collector_config.decode()

    return config_map


def _has_sql_exporter_config_changed(
    old_config_map: V1ConfigMap, new_collectors: list
) -> bool:
    for collector in new_collectors:
        if collector.endswith("_collector"):
            collector = collector[:-10]
        yaml_filename = f"{collector}-collector.yaml"
        if yaml_filename not in old_config_map.data.keys():
            return True
    return False


def get_crash_pod_name(spec: dict, name: str) -> str:
    """
    Returns the pod name where crash commands should be run.

    :param spec: The CrateDB custom resource definition.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    """
    has_master_nodes = "master" in spec["spec"]["nodes"]
    if has_master_nodes:
        return f"crate-master-{name}-0"
    else:
        node_name = spec["spec"]["nodes"]["data"][0]["name"]
        return f"crate-data-{node_name}-{name}-0"


def get_crash_scheme(spec: dict) -> str:
    """
    Return the host scheme for running crash commands.

    :param spec: The CrateDB custom resource definition.
    """
    return "https" if "ssl" in spec["spec"]["cluster"] else "http"


class RestartSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(config.ROLLING_RESTART_TIMEOUT))
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        old: kopf.Body,
        logger: logging.Logger,
        patch: kopf.Patch,
        status: kopf.Status,
        action: WebhookAction,
        **kwargs: Any,
    ):
        async with GlobalApiClient() as api_client:
            core = CoreV1Api(api_client)
            await restart_cluster(
                core, namespace, name, old, logger, patch, status, action
            )


DISABLE_CRONJOB_HANDLER_ID = "disable_cronjob"
IGNORE_CRONJOB = "ignore_cronjob"
CRONJOB_SUSPENDED = "cronjob_suspended"
CRONJOB_NAME = "cronjob_name"
DELAY_CRONJOB = "delay_cronjob"
DELAY_CRONJOB_START = "delay_cronjob_start"


class BeforeClusterUpdateSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(config.BEFORE_UPDATE_TIMEOUT))
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        status: kopf.Status,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        kopf.register(
            fn=subhandler_partial(
                self._ensure_cronjob_suspended, namespace, name, status, logger
            ),
            id=DISABLE_CRONJOB_HANDLER_ID,
        )
        kopf.register(
            fn=subhandler_partial(
                self._ensure_no_snapshots_in_progress, namespace, name, logger
            ),
            id="ensure_no_snapshots_in_progress",
        )
        kopf.register(
            fn=subhandler_partial(
                self._ensure_no_backup_cronjobs_running, namespace, name, logger
            ),
            id="ensure_no_cronjobs_running",
        )
        kopf.register(
            fn=subhandler_partial(
                self._set_cluster_routing_allocation_setting, namespace, name, logger
            ),
            id="set_cluster_routing_allocation_setting",
        )

    @staticmethod
    async def _ensure_cronjob_suspended(
        namespace: str, name: str, status: kopf.Status, logger: logging.Logger
    ) -> Optional[Dict]:
        async with GlobalApiClient() as api_client:
            batch = BatchV1Api(api_client)

            jobs: V1CronJobList = await batch.list_namespaced_cron_job(namespace)

            for job in jobs.items:
                job_name = job.metadata.name
                labels = job.metadata.labels
                if (
                    labels.get("app.kubernetes.io/component") == "backup"
                    and labels.get("app.kubernetes.io/name") == name
                ):
                    current_suspend_status = job.spec.suspend
                    if (
                        current_suspend_status
                        and status.get(DELAY_CRONJOB, False) is False
                    ):
                        logger.warning(
                            f"Found job {job_name} that is already suspended, ignoring"
                        )
                        return {
                            CRONJOB_NAME: job_name,
                            CRONJOB_SUSPENDED: True,
                            IGNORE_CRONJOB: True,
                        }

                    logger.info(
                        f"Temporarily suspending CronJob {job_name} "
                        f"while cluster update in progress"
                    )
                    update = {"spec": {"suspend": True}}
                    await batch.patch_namespaced_cron_job(job_name, namespace, update)
                    return {
                        CRONJOB_NAME: job_name,
                        CRONJOB_SUSPENDED: True,
                        IGNORE_CRONJOB: False,
                    }

        return None

    async def _ensure_no_snapshots_in_progress(self, namespace, name, logger):
        async with GlobalApiClient() as api_client:
            core = CoreV1Api(api_client)

            conn_factory = await _get_connection_factory(core, namespace, name)

            snapshots_in_progress, statement = await are_snapshots_in_progress(
                conn_factory, logger
            )
            if snapshots_in_progress:
                raise kopf.TemporaryError(
                    "A snapshot is currently in progress, "
                    f"waiting for it to finish: {statement}",
                    delay=5 if config.TESTING else 30,
                )

    async def _ensure_no_backup_cronjobs_running(
        self, namespace: str, name: str, logger: logging.Logger
    ):
        async with GlobalApiClient() as api_client:
            batch = BatchV1Api(api_client)

            jobs: V1JobList = await call_kubeapi(
                batch.list_namespaced_job, logger, namespace=namespace
            )
            for job in jobs.items:
                job_name = job.metadata.name
                labels = job.metadata.labels
                job_status: V1JobStatus = job.status
                if (
                    labels.get("app.kubernetes.io/component") == "backup"
                    and labels.get("app.kubernetes.io/name") == name
                    and job_status.active is not None
                ):
                    await kopf.execute(
                        fns={
                            "notify_backup_running": subhandler_partial(  # type: ignore[dict-item]  # noqa: E501
                                self._notify_backup_running, logger
                            )
                        }
                    )
                    raise kopf.TemporaryError(
                        "A snapshot k8s job is currently running, "
                        f"waiting for it to finish: {job_name}",
                        delay=5 if config.TESTING else 30,
                    )

    async def _notify_backup_running(self, logger):
        await self.send_notification_now(
            logger,
            WebhookEvent.DELAY,
            WebhookTemporaryFailurePayload(reason="A snapshot is in progress"),
            WebhookStatus.TEMPORARY_FAILURE,
        )

    async def _set_cluster_routing_allocation_setting(
        self, namespace: str, name: str, logger: logging.Logger
    ):
        async with GlobalApiClient() as api_client:
            core = CoreV1Api(api_client)

            conn_factory = await _get_connection_factory(core, namespace, name)

            await set_cluster_setting(
                conn_factory,
                logger,
                setting="cluster.routing.allocation.enable",
                value="new_primaries",
                mode="PERSISTENT",
            )


class AfterClusterUpdateSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(config.AFTER_UPDATE_TIMEOUT))
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        status: kopf.Status,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        # If a delay is set, a timer will trigger it, otherwise, it's triggered here
        if status.get(DELAY_CRONJOB, False) is False:
            kopf.register(
                fn=subhandler_partial(
                    ensure_cronjob_reenabled, namespace, name, logger, status
                ),
                id="ensure_cronjob_reenabled",
            )

        kopf.register(
            fn=subhandler_partial(
                self._reset_cluster_routing_allocation_setting, namespace, name, logger
            ),
            id="reset_cluster_routing_allocation_setting",
        )

    async def _reset_cluster_routing_allocation_setting(
        self, namespace: str, name: str, logger: logging.Logger
    ):
        async with GlobalApiClient() as api_client:
            core = CoreV1Api(api_client)

            conn_factory = await _get_connection_factory(core, namespace, name)

            await reset_cluster_setting(
                conn_factory,
                logger,
                setting="cluster.routing.allocation.enable",
            )


async def ensure_cronjob_reenabled(
    namespace: str,
    name: str,
    logger: logging.Logger,
    status: kopf.Status,
):
    disabler_job_status = None
    for key in status.keys():
        if key.endswith(DISABLE_CRONJOB_HANDLER_ID):
            disabler_job_status = status.get(key)
            break

    if disabler_job_status is None:
        logger.info("No cronjob was disabled, so can't re-enable anything.")
        return

    if disabler_job_status.get(IGNORE_CRONJOB, False):
        logger.warning("Will not attempt to re-enable any CronJobs")
        return

    async with GlobalApiClient() as api_client:
        job_name = disabler_job_status[CRONJOB_NAME]

        batch = BatchV1Api(api_client)

        jobs: V1CronJobList = await batch.list_namespaced_cron_job(namespace)

        for job in jobs.items:
            if job.metadata.name == job_name:
                update = {"spec": {"suspend": False}}
                await batch.patch_namespaced_cron_job(job_name, namespace, update)
                logger.info(f"Re-enabled cronjob {job_name}")


async def set_cronjob_delay(patch):
    # Set a tag in the status that will be detected by a timer.
    # The timer will re-enable the cronjobs.
    patch.status[DELAY_CRONJOB] = True
    patch.status[DELAY_CRONJOB_START] = datetime.datetime.now(
        datetime.timezone.utc
    ).timestamp()


async def set_user_jwt(
    cursor: Cursor,
    namespace: str,
    name: str,
    username: str,
    logger: logging.Logger,
) -> None:
    """
    Set JWT auth properties for a given username

    :param cursor: A database cursor object to the CrateDB cluster where the
        user should be added.
    :param namespace: The Kubernetes namespace of the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param username: The name of the user the JWT properties should be set for.
    :param logger: Logger for operation tracking.
    """
    cratedb = await get_cratedb_resource(namespace, name)
    await cursor.execute(
        "SELECT count(*) = 1 FROM sys.users WHERE name = %s", (username,)
    )
    row = await cursor.fetchone()
    user_exists = bool(row[0])

    username_ident = quote_ident(username, cursor._impl)
    iss = cratedb["spec"].get("grandCentral", {}).get("jwkUrl")

    if not (user_exists and iss):
        return

    pod_name = get_crash_pod_name(cratedb, name)
    scheme = get_crash_scheme(cratedb)

    # Step 1: Reset JWT to NULL first
    # This prevents RoleAlreadyExistsException when restoring snapshots
    # where the user might have JWT properties with different aud/iss
    logger.info("Resetting JWT auth properties for user '%s'", username)
    reset_query = f"ALTER USER {username_ident} SET (jwt = NULL)"
    logger.info("... executing query: %s", reset_query)

    reset_result = await execute_sql(
        namespace=namespace,
        name=name,
        pod_name=pod_name,
        scheme=scheme,
        sql=reset_query,
        args=None,
        logger=logger,
    )
    logger.info("... result: %s", reset_result)

    if (reset_result.rowcount or 0) > 0:
        logger.info("... JWT reset successful")
    else:
        logger.info(
            "... JWT reset had no effect (might not have been set). %s", reset_result
        )

    # Step 2: Set new JWT properties with current cluster's aud
    logger.info("Setting JWT auth properties for user '%s'", username)
    set_query = (
        f"ALTER USER {username_ident} SET "
        f"""(jwt = {{"iss" = '{iss}', "username" = '{username}', """
        f""""aud" = '{name}'}})"""
    )
    logger.info("... executing query: %s", set_query)

    result = await execute_sql(
        namespace=namespace,
        name=name,
        pod_name=pod_name,
        scheme=scheme,
        sql=set_query,
        args=None,
        logger=logger,
    )
    logger.info("... result: %s", result)

    if (result.rowcount or 0) > 0:
        logger.info("... success")
    else:
        logger.info("... error. %s", result)
        if (
            not result.error_message
            or "RoleAlreadyExistsException" not in result.error_message
        ):
            raise kopf.TemporaryError(delay=config.BOOTSTRAP_RETRY_DELAY)


class StartClusterSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(config.SCALING_TIMEOUT))
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        spec: kopf.Spec,
        old: kopf.Body,
        diff: kopf.Diff,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        scale_data_diff_items: Optional[List[kopf.DiffItem]] = None

        for operation, field_path, old_value, new_value in diff:
            if field_path == ("spec", "nodes", "data"):
                scale_data_diff_items = []
                for node_spec_idx in range(len(old_value)):
                    new_spec = new_value[node_spec_idx]

                    scale_data_diff_items.append(
                        kopf.DiffItem(
                            kopf.DiffOperation.CHANGE,
                            (str(node_spec_idx), "replicas"),
                            0,
                            new_spec["replicas"],
                        )
                    )
            else:
                logger.info("Ignoring operation %s on field %s", operation, field_path)

        if scale_data_diff_items:
            async with GlobalApiClient() as api_client:
                apps = AppsV1Api(api_client)
                core = CoreV1Api(api_client)

                await suspend_or_start_cluster(
                    apps,
                    core,
                    namespace,
                    name,
                    old,
                    kopf.Diff(scale_data_diff_items),
                    False,
                    logger,
                )


class SuspendClusterSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(config.SCALING_TIMEOUT))
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        spec: kopf.Spec,
        old: kopf.Body,
        diff: kopf.Diff,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        scale_data_diff_items: Optional[List[kopf.DiffItem]] = None

        for operation, field_path, old_value, new_value in diff:
            if field_path == ("spec", "nodes", "data"):
                scale_data_diff_items = []
                for node_spec_idx in range(len(old_value)):
                    old_spec = old_value[node_spec_idx]

                    # scale all data nodes to 0 replicas
                    scale_data_diff_items.append(
                        kopf.DiffItem(
                            kopf.DiffOperation.CHANGE,
                            (str(node_spec_idx), "replicas"),
                            old_spec["replicas"],
                            0,
                        )
                    )
            else:
                logger.info("Ignoring operation %s on field %s", operation, field_path)

        if scale_data_diff_items:
            async with GlobalApiClient() as api_client:
                apps = AppsV1Api(api_client)
                core = CoreV1Api(api_client)

                await suspend_or_start_cluster(
                    apps,
                    core,
                    namespace,
                    name,
                    old,
                    kopf.Diff(scale_data_diff_items),
                    False,
                    logger,
                )


class RestoreUserJWTAuthSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        """
        Restore the admin user's JWT properties.
        Use crash here because the system user is not allowed to ALTER any
        other users.

        :param namespace: The Kubernetes namespace of the CrateDB cluster.
        :param name: The CrateDB custom resource name defining the CrateDB cluster.
        :param logger: the logger on which we're logging
        """
        cratedb = await get_cratedb_resource(namespace, name)
        crate_version = cratedb["spec"]["cluster"]["version"]
        users = cratedb["spec"].get("users")
        gc_config = cratedb["spec"].get("grandCentral", {})
        if crate_version_supports_jwt(crate_version) and users and gc_config:
            async with GlobalApiClient() as api_client:
                core = CoreV1Api(api_client)
                host = await get_host(core, namespace, name)
                password = await get_system_user_password(core, namespace, name)
                async with get_connection(
                    host, password, timeout=CONNECT_TIMEOUT
                ) as conn:
                    async with conn.cursor() as cursor:
                        for user_spec in users:
                            username = user_spec["name"]

                            await set_user_jwt(
                                cursor, namespace, name, username, logger
                            )
