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
from typing import Any, Dict, List, Optional, Tuple, cast

import kopf
from kubernetes_asyncio.client import (
    BatchV1Api,
    BatchV1beta1Api,
    CoreV1Api,
    V1beta1CronJobList,
    V1JobList,
    V1JobStatus,
    V1PodList,
)
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.config import config
from crate.operator.constants import (
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_NODE_NAME,
    LABEL_PART_OF,
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
from crate.operator.webhooks import (
    WebhookEvent,
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
            delay=15,
        )
    elif next_pod_name in all_pod_names:
        total_nodes = get_total_nodes_count(old["spec"]["nodes"], "all")
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
                "Cluster is not healthy yet.", delay=config.HEALTH_CHECK_RETRY_DELAY
            )
    else:
        raise kopf.TemporaryError(
            "Scheduling rerun because there are pods to be restarted", delay=15
        )


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

        await self.send_notifications(logger)


DISABLE_CRONJOB_HANDLER_ID = "disable_cronjob"
IGNORE_CRONJOB = "ignore_cronjob"
CRONJOB_SUSPENDED = "cronjob_suspended"
CRONJOB_NAME = "cronjob_name"


class BeforeClusterUpdateSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(config.SCALING_TIMEOUT))
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
                    return {CRONJOB_NAME: job_name, CRONJOB_SUSPENDED: True}

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
        self.schedule_notification(
            WebhookEvent.DELAY,
            WebhookTemporaryFailurePayload(reason="A snapshot is in progress"),
            WebhookStatus.TEMPORARY_FAILURE,
        )
        await self.send_notifications(logger)

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
