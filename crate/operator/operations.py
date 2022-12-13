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
import logging
from typing import Any, Dict, List, Optional, Tuple, cast

import kopf
from kubernetes_asyncio.client import (
    AppsV1Api,
    BatchV1Api,
    BatchV1beta1Api,
    CoreV1Api,
    CustomObjectsApi,
    V1beta1CronJobList,
    V1JobList,
    V1JobStatus,
    V1PersistentVolumeClaimList,
    V1PodList,
    V1StatefulSet,
    V1StatefulSetList,
)
from kubernetes_asyncio.client.api_client import ApiClient
from psycopg2 import DatabaseError, OperationalError

from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    BACKUP_METRICS_DEPLOYMENT_NAME,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_NODE_NAME,
    LABEL_PART_OF,
    RESOURCE_CRATEDB,
)
from crate.operator.cratedb import (
    are_snapshots_in_progress,
    connection_factory,
    is_cluster_healthy,
    reset_cluster_setting,
    set_cluster_setting,
)
from crate.operator.utils import crate
from crate.operator.utils.kopf import StateBasedSubHandler, subhandler_partial
from crate.operator.utils.kubeapi import (
    call_kubeapi,
    get_host,
    get_system_user_password,
)
from crate.operator.utils.notifications import send_operation_progress_notification
from crate.operator.webhooks import (
    WebhookEvent,
    WebhookOperation,
    WebhookStatus,
    WebhookTemporaryFailurePayload,
)


async def get_desired_nodes_count(namespace: str, name: str) -> int:
    """
    Returns the amount of replicas specified by the StatefulSet
    """
    sts_name = f"crate-data-hot-{name}"
    async with ApiClient() as api_client:
        apps = AppsV1Api(api_client)
        statefulset = await apps.read_namespaced_stateful_set(
            namespace=namespace, name=sts_name
        )
        return statefulset.spec.replicas


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
        LABEL_COMPONENT: "backup",
        LABEL_MANAGED_BY: "crate-operator",
        LABEL_NAME: name,
        LABEL_PART_OF: "cratedb",
    }
    label_selector = ",".join(f"{k}={v}" for k, v in labels.items())

    all_pods: V1PodList = await core.list_namespaced_pod(
        namespace=namespace, label_selector=label_selector
    )
    return [{"uid": p.metadata.uid, "name": p.metadata.name} for p in all_pods.items]


async def get_cratedb_resource(namespace: str, name: str) -> dict:
    """
    Return the CrateDB custom resource.

    :param namespace: The Kubernetes namespace where to look up the CrateDB
        cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    """
    async with ApiClient() as api_client:
        coapi = CustomObjectsApi(api_client)
        return await coapi.get_namespaced_custom_object(
            group=API_GROUP,
            version="v1",
            plural=RESOURCE_CRATEDB,
            namespace=namespace,
            name=name,
        )


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
    async with ApiClient() as api_client:
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
        await send_operation_progress_notification(
            namespace=namespace,
            name=name,
            message="Waiting for node "
            f"{int(next_pod_name[next_pod_name.rindex('-')+1:])+1}/{len(all_pod_uids)}"
            " to be terminated...",
            logger=logger,
            status=WebhookStatus.IN_PROGRESS,
            operation=WebhookOperation.UPDATE,
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
        await send_operation_progress_notification(
            namespace=namespace,
            name=name,
            message="Waiting for node "
            f"{int(next_pod_name[next_pod_name.rindex('-')+1:])+1}/{len(all_pod_uids)}"
            " to be restarted...",
            logger=logger,
            status=WebhookStatus.IN_PROGRESS,
            operation=WebhookOperation.UPDATE,
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
            raise kopf.TemporaryError(
                "Cluster is not healthy yet.", delay=config.HEALTH_CHECK_RETRY_DELAY
            )
    else:
        raise kopf.TemporaryError(
            "Scheduling rerun because there are pods to be restarted", delay=15
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
    Suspend or scale a cluster ``name``  back up, according to the given
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
    spec = old["spec"]

    host = await get_host(core, namespace, name)
    password = await get_system_user_password(core, namespace, name)
    conn_factory = connection_factory(host, password)

    if data_diff_items:
        for _, field_path, old_replicas, new_replicas in data_diff_items:
            if old_replicas < new_replicas:
                # scale the cluster back up
                index_path, *_ = field_path
                index = int(index_path)
                node_spec = spec["nodes"]["data"][index]
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
                await send_operation_progress_notification(
                    namespace=namespace,
                    name=name,
                    message=f"Starting cluster. Scaling back up to {new_replicas} "
                    "nodes. Waiting for node(s) to be present.",
                    logger=logger,
                    status=WebhookStatus.IN_PROGRESS,
                    operation=WebhookOperation.UPDATE,
                )
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
                await check_cluster_healthy(name, namespace, apps, conn_factory, logger)
                index_path, *_ = field_path
                index = int(index_path)
                node_spec = spec["nodes"]["data"][index]
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
                await send_operation_progress_notification(
                    namespace=namespace,
                    name=name,
                    message="Suspending cluster.",
                    logger=logger,
                    status=WebhookStatus.IN_PROGRESS,
                    operation=WebhookOperation.UPDATE,
                )
                if scale_backup_metrics:
                    await check_backup_metrics_pod_gone(
                        core,
                        namespace,
                        name,
                    )
                await check_all_data_nodes_gone(core, namespace, name, old)


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
        **kwargs: Any,
    ):
        async with ApiClient() as api_client:
            core = CoreV1Api(api_client)
            await restart_cluster(core, namespace, name, old, logger, patch, status)


DISABLE_CRONJOB_HANDLER_ID = "disable_cronjob"
IGNORE_CRONJOB = "ignore_cronjob"
CRONJOB_SUSPENDED = "cronjob_suspended"
CRONJOB_NAME = "cronjob_name"


class BeforeClusterUpdateSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        kopf.register(
            fn=subhandler_partial(
                self._ensure_cronjob_suspended, namespace, name, logger
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
        namespace: str, name: str, logger: logging.Logger
    ) -> Optional[Dict]:
        async with ApiClient() as api_client:
            batch = BatchV1beta1Api(api_client)

            jobs: V1beta1CronJobList = await batch.list_namespaced_cron_job(namespace)

            for job in jobs.items:
                job_name = job.metadata.name
                labels = job.metadata.labels
                if (
                    labels.get("app.kubernetes.io/component") == "backup"
                    and labels.get("app.kubernetes.io/name") == name
                ):
                    current_suspend_status = job.spec.suspend
                    if current_suspend_status:
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
        async with ApiClient() as api_client:
            core = CoreV1Api(api_client)

            host = await get_host(core, namespace, name)
            password = await get_system_user_password(core, namespace, name)
            conn_factory = connection_factory(host, password)

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
        async with ApiClient() as api_client:
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
                            "notify_backup_running": subhandler_partial(
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
        async with ApiClient() as api_client:
            core = CoreV1Api(api_client)

            host = await get_host(core, namespace, name)
            password = await get_system_user_password(core, namespace, name)
            conn_factory = connection_factory(host, password)

            await set_cluster_setting(
                conn_factory,
                logger,
                setting="cluster.routing.allocation.enable",
                value="new_primaries",
                mode="PERSISTENT",
            )


class AfterClusterUpdateSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        status: kopf.Status,
        **kwargs: Any,
    ):
        kopf.register(
            fn=subhandler_partial(
                self._ensure_cronjob_reenabled, namespace, name, logger, status
            ),
            id="ensure_cronjob_reenabled",
        )
        kopf.register(
            fn=subhandler_partial(
                self._reset_cluster_routing_allocation_setting, namespace, name, logger
            ),
            id="reset_cluster_routing_allocation_setting",
        )

    async def _ensure_cronjob_reenabled(
        self, namespace: str, name: str, logger: logging.Logger, status: kopf.Status
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

        async with ApiClient() as api_client:
            job_name = disabler_job_status[CRONJOB_NAME]

            batch = BatchV1beta1Api(api_client)

            jobs: V1beta1CronJobList = await batch.list_namespaced_cron_job(namespace)

            for job in jobs.items:
                if job.metadata.name == job_name:
                    update = {"spec": {"suspend": False}}
                    await batch.patch_namespaced_cron_job(job_name, namespace, update)
                    logger.info(f"Re-enabled cronjob {job_name}")

    async def _reset_cluster_routing_allocation_setting(
        self, namespace: str, name: str, logger: logging.Logger
    ):
        async with ApiClient() as api_client:
            core = CoreV1Api(api_client)

            host = await get_host(core, namespace, name)
            password = await get_system_user_password(core, namespace, name)
            conn_factory = connection_factory(host, password)

            await reset_cluster_setting(
                conn_factory,
                logger,
                setting="cluster.routing.allocation.enable",
            )


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
            async with ApiClient() as api_client:
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
            async with ApiClient() as api_client:
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
