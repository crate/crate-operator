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
import re
import sys
from typing import Any, Dict, List, Optional

import kopf
from aiopg import Cursor
from kopf.structs.diffs import diff as calc_diff
from kubernetes_asyncio.client import (
    AppsV1Api,
    CoreV1Api,
    V1Container,
    V1StatefulSet,
    V1StatefulSetList,
)
from kubernetes_asyncio.client.api_client import ApiClient
from kubernetes_asyncio.stream import WsApiClient

from crate.operator.config import config
from crate.operator.cratedb import connection_factory, is_cluster_healthy
from crate.operator.operations import get_total_nodes_count
from crate.operator.utils import quorum
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.kubeapi import get_host, get_system_user_password
from crate.operator.utils.version import CrateVersion
from crate.operator.webhooks import (
    WebhookEvent,
    WebhookScaleNodePayload,
    WebhookScalePayload,
    WebhookStatus,
)


def parse_replicas(r: str) -> int:
    """
    Parse the ``number_of_replicas`` CrateDB table setting into an integer.

    The input string could be in one of these forms:

    * ``'n'`` (e.g. ``'3'``)
    * ``'m-n'`` (e.g. ``'1-3'``)
    * ``'n-all'`` (e.g. ``'1-all'``)
    * ``'all'``

    When ``all`` is given, it's treated as :data:`sys.maxsize`.

    :param r: The value for ``number_of_replicas``.
    :return: The maximum number of replicas for a table.
    """
    if "-" in r:
        r = r.split("-", 1)[0]
    if r == "all":
        return sys.maxsize
    return int(r)


def patch_command(old_command: List[str], total_nodes: int) -> List[str]:
    """
    Iterate through the ``old_command`` items and update them with
    ``total_nodes`` where required.

    Return the list making up the new CrateDB command.

    :param old_command: The command used to start-up CrateDB inside a
        Kubernetes container. This consists of the path to the Docker
        entrypoint script, the ``crate`` command argument and any additional
        settings.
    :param total_nodes: The number of nodes that will be in the CrateDB
        cluster. From that, the quorum is derived as well.
    :return: The list forming the new CrateDB command.
    """
    new_command: List[str] = []
    for item in old_command:
        if (
            match := re.search("^(-Cgateway.recover_after(_data)?_nodes=)", item)
        ) is not None:
            item = f"{match.group(1)}{quorum(total_nodes)}"
        elif (
            match := re.search("^(-Cgateway.expected(_data)?_nodes=)", item)
        ) is not None:
            item = f"{match.group(1)}{total_nodes}"
        new_command.append(item)
    return new_command


def get_container(statefulset: V1StatefulSet) -> V1Container:
    """
    Return the ``'crate'`` container inside a StatefulSet

    :param statefulset: The StatefulSet where to lookup the CrateDB container
        in.
    :return: The CrateDB container in the provided StatefulSet.
    """
    containers = [
        c for c in statefulset.spec.template.spec.containers if c.name == "crate"
    ]
    assert len(containers) == 1, "Only a single crate container must exist!"
    return containers[0]


async def update_statefulset(
    apps: AppsV1Api,
    namespace: Optional[str],
    sts_name: Optional[str],
    statefulset: Optional[V1StatefulSet],
    replicas: Optional[int],
    total_nodes: int,
):
    """
    Call the Kubernetes API and update the container command and the replicas.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param sts_name: The name for the Kubernetes StatefulSet to update.
    :param replicas: The new number of replicas for the StatefulSet. Can be
        ``None`` to not change the number of replicas for the StatefulSet.
    :param total_nodes: The number of nodes that will be in the CrateDB
        cluster.
    """
    statefulset = statefulset or await apps.read_namespaced_stateful_set(
        namespace=namespace, name=sts_name
    )
    crate_container = get_container(statefulset)
    new_command = patch_command(crate_container.command, total_nodes)
    body: Dict[str, Any] = {
        "spec": {
            "template": {
                "spec": {"containers": [{"name": "crate", "command": new_command}]}
            },
        }
    }
    if replicas is not None:
        body["spec"]["replicas"] = replicas
    await apps.patch_namespaced_stateful_set(
        namespace=namespace, name=sts_name, body=body
    )


async def check_nodes_present_or_gone(
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
    :raises: A :class:`kopf.TemporaryError` when nodes are missing (scale up)
        or still available (scale down).
    """
    full_node_list = [
        f"{node_prefix}-{i}" for i in range(max(old_replicas, new_replicas))
    ]
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
                        f"Waiting for nodes {missing_nodes} to be present.", delay=15
                    )
            elif old_replicas > new_replicas:
                # scale down
                if candidate_node_names.issubset(available_nodes):
                    excess_nodes = ", ".join(sorted(candidate_node_names))
                    raise kopf.TemporaryError(
                        f"Waiting for nodes {excess_nodes} to be gone.", delay=15
                    )
            else:
                logger.info(
                    "No need to wait for nodes with prefix '%s', since the "
                    "number of replicas didn't change.",
                    node_prefix,
                )


async def check_for_deallocation(
    cursor: Cursor, node_names: List[str], logger: logging.Logger
):
    """
    Wait until the nodes ``node_names`` have no more shards.

    :param cursor: A database cursor to a current and open database connection.
    :param node_names: A list of CrateDB node names. These are the names that
        are known to CrateDB, e.g. ``data-hot-2`` or ``master-1``.
    """
    logger.info(
        "Waiting for deallocation of CrateDB nodes %s ...", ", ".join(node_names)
    )
    # We select the node names and the number of shards for all nodes that
    # will be torn down. As long as the rowcount is > 0 the nodes in question
    # still have shards allocated and thus can't be decommissioned.
    await cursor.execute(
        """
        SELECT node['name'], count(*)
        FROM sys.shards
        WHERE node['name'] = ANY(%s)
        GROUP BY 1
        """,
        (node_names,),
    )
    rows = await cursor.fetchall()
    if rows:
        allocations = ", ".join(f"{row[0]}={row[1]}" for row in rows) or "None"
        logger.info("Current pending allocation %s", allocations)
        raise kopf.TemporaryError("Pending allocation")


async def deallocate_nodes(
    conn_factory,
    new_num_data_nodes: int,
    excess_nodes: List[str],
    logger: logging.Logger,
) -> None:
    """
    Trigger deallocation of data from nodes ``excess_nodes``.

    When the function exits cleanly, the nodes are deallocated. WHen the
    function raises a :class:`kopf.TemporaryError`, some shards still need to
    be relocated.

    :param conn_factory: A function that establishes a database connection to
        the CrateDB cluster used for SQL queries.
    :param new_num_data_nodes: The total number of data nodes in the CrateDB
        cluster *without* the ``excess_nodes``.
    :param excess_nodes: The list of CrateDB nodes to deallocate.
    :raises: A :class:`kopf.TemporaryError` when shards need to be relocated.
    """
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            # 1. Check that there are not too many replicas for some tables.
            await cursor.execute(
                """
                SELECT DISTINCT number_of_replicas
                FROM information_schema.tables
                WHERE number_of_shards IS NOT NULL
                """,
            )
            rows = await cursor.fetchall()
            if rows:
                max_replicas = max(parse_replicas(r[0]) for r in rows)
            else:
                max_replicas = 0
            # A table has 1 (primary) + n replicas. If that number is larger
            # than the number of data nodes in the cluster then there are not
            # enough nodes to have all tables fully replicated. Thus, failing
            # the scaling.
            if max_replicas + 1 > new_num_data_nodes:
                raise kopf.TemporaryError(
                    "Some tables have too many replicas to scale down"
                )

            # 2. Exclude current-new nodes from allocating shards
            # A list of CrateDB node names that will be removed.
            await cursor.execute(
                """
                SET GLOBAL "cluster.routing.allocation.exclude._name" = %s
                """,
                (",".join(excess_nodes),),
            )

            # 3. Wait for nodes to be deallocated
            await check_for_deallocation(cursor, excess_nodes, logger)


async def scale_cluster_patch_total_nodes(
    apps: AppsV1Api, namespace: str, name: str, spec: kopf.Spec, total_nodes: int
) -> None:
    """
    Update all StatefulSets to ensure intermittent node restarts will not reset
    the desired total nodes and quorum in a cluster.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param spec: The ``spec`` field from the new CrateDB cluster custom object.
    :param total_nodes: The total number of nodes in the CrateDB cluster
        *after* scaling all StatefulSets.
    """
    updates = []
    if "master" in spec["nodes"]:
        updates.append(
            update_statefulset(
                apps, namespace, f"crate-master-{name}", None, None, total_nodes
            )
        )
    updates.extend(
        [
            update_statefulset(
                apps,
                namespace,
                f"crate-data-{node_spec['name']}-{name}",
                None,
                None,
                total_nodes,
            )
            for node_spec in spec["nodes"]["data"]
        ]
    )
    await asyncio.gather(*updates)


async def reset_allocation(namespace: str, pod_name: str, has_ssl: bool) -> None:
    """
    Reset all temporary node deallocations to none.

    .. note::

       Ideally, we'd be using the system user to reset the allocation
       exclusions. However, `due to a bug
       <https://github.com/crate/crate/pull/10083>`_, this isn't possible in
       CrateDB <= 4.1.6. We therefore fall back to the "exec-in-container"
       approach that we also use during cluster bootstrapping.

    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param pod_name: The pod name of one of the eligible master nodes in
        the cluster. Used to ``exec`` into.
    :param has_ssl: When ``True``, ``crash`` will establish a connection to
        the CrateDB cluster from inside the ``crate`` container using SSL/TLS.
        This must match how the cluster is configured, otherwise ``crash``
        won't be able to connect, since non-encrypted connections are forbidden
        when SSL/TLS is enabled, and encrypted connections aren't possible when
        no SSL/TLS is configured.
    """

    # async with conn_factory() as conn:
    #     async with conn.cursor() as cursor:
    #         await cursor.execute(
    #             """
    #             RESET GLOBAL "cluster.routing.allocation.exclude._name"
    #             """,
    #         )

    scheme = "https" if has_ssl else "http"
    command_grant = [
        "crash",
        "--verify-ssl=false",
        f"--host={scheme}://localhost:4200",
        "-c",
        'RESET GLOBAL "cluster.routing.allocation.exclude._name";',
    ]
    async with WsApiClient() as ws_api_client:
        core_ws = CoreV1Api(ws_api_client)
        await core_ws.connect_get_namespaced_pod_exec(
            namespace=namespace,
            name=pod_name,
            command=command_grant,
            container="crate",
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )


async def scale_cluster(
    apps: AppsV1Api,
    core: CoreV1Api,
    namespace: str,
    name: str,
    old: kopf.Body,
    master_diff_item: Optional[kopf.DiffItem],
    data_diff_items: Optional[kopf.Diff],
    logger: logging.Logger,
):
    """
    Scale cluster ``name`` according to the given ``master_diff_item`` and
    ``data_diff_items``.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param old: The old resource body.
    :param master_diff_item: An optional change indicating how many master
        nodes a cluster should have.
    :param data_diff_items: An optional list of changes made to the individual
        data node specifications.
    """
    spec = old["spec"]
    crate_version = spec["cluster"]["version"]
    total_number_of_nodes = get_total_nodes_count(spec["nodes"], "all")
    if CrateVersion(crate_version) >= CrateVersion(
        config.GATEWAY_SETTINGS_DATA_NODES_VERSION
    ):
        # for scaling only the number of (data) nodes are updated
        total_number_of_nodes = get_total_nodes_count(spec["nodes"], "data")

    host = await get_host(core, namespace, name)
    password = await get_system_user_password(core, namespace, name)
    conn_factory = connection_factory(host, password)

    num_master_nodes = 0
    if "master" in spec["nodes"]:
        num_master_nodes = spec["nodes"]["master"]["replicas"]

    if master_diff_item:
        _, _, old_replicas, new_replicas = master_diff_item
        if (CrateVersion(crate_version)) < CrateVersion(
            config.GATEWAY_SETTINGS_DATA_NODES_VERSION
        ):
            # only for deprecated settings the number of master nodes affects the
            # settings' values
            total_number_of_nodes = total_number_of_nodes + new_replicas - old_replicas
        num_master_nodes = new_replicas
        sts_name = f"crate-master-{name}"
        statefulset = await apps.read_namespaced_stateful_set(
            namespace=namespace, name=sts_name
        )
        current_replicas = statefulset.spec.replicas
        if current_replicas != new_replicas:
            await update_statefulset(
                apps,
                namespace,
                sts_name,
                statefulset,
                new_replicas,
                total_number_of_nodes,
            )
            await scale_cluster_patch_total_nodes(
                apps, namespace, name, spec, total_number_of_nodes
            )

        await check_nodes_present_or_gone(
            conn_factory,
            old_replicas,
            new_replicas,
            "master",
            logger,
        )

    if data_diff_items:
        for _, field_path, old_replicas, new_replicas in data_diff_items:
            if old_replicas < new_replicas:
                # scale up
                # changes in number of data nodes are treated the same before
                # and after 4.7
                total_number_of_nodes = (
                    total_number_of_nodes + new_replicas - old_replicas
                )
                index, *_ = field_path
                index = int(index)
                node_spec = spec["nodes"]["data"][index]
                node_name = node_spec["name"]
                sts_name = f"crate-data-{node_name}-{name}"
                statefulset = await apps.read_namespaced_stateful_set(
                    namespace=namespace, name=sts_name
                )
                current_replicas = statefulset.spec.replicas
                if current_replicas != new_replicas:
                    await update_statefulset(
                        apps,
                        namespace,
                        sts_name,
                        statefulset,
                        new_replicas,
                        total_number_of_nodes,
                    )
                    await scale_cluster_patch_total_nodes(
                        apps, namespace, name, spec, total_number_of_nodes
                    )

                await check_nodes_present_or_gone(
                    conn_factory,
                    old_replicas,
                    new_replicas,
                    f"data-{node_name}",
                    logger,
                )

        for _, field_path, old_replicas, new_replicas in data_diff_items:
            if old_replicas > new_replicas:
                # scale down
                # First check if the cluster is healthy at all,
                # and prevent scaling down if not.
                await _ensure_cluster_healthy(
                    name, namespace, apps, conn_factory, logger
                )

                total_number_of_nodes = (
                    total_number_of_nodes + new_replicas - old_replicas
                )
                index, *_ = field_path
                index = int(index)
                node_spec = spec["nodes"]["data"][index]
                node_name = node_spec["name"]
                sts_name = f"crate-data-{node_name}-{name}"
                statefulset = await apps.read_namespaced_stateful_set(
                    namespace=namespace, name=sts_name
                )
                current_replicas = statefulset.spec.replicas
                if current_replicas != new_replicas:
                    excess_nodes = [
                        f"data-{node_name}-{i}"
                        for i in range(new_replicas, old_replicas)
                    ]
                    new_num_data_nodes = total_number_of_nodes
                    if CrateVersion(crate_version) < CrateVersion(
                        config.GATEWAY_SETTINGS_DATA_NODES_VERSION
                    ):
                        new_num_data_nodes = total_number_of_nodes - num_master_nodes

                    await deallocate_nodes(
                        conn_factory,
                        new_num_data_nodes,
                        excess_nodes,
                        logger,
                    )

                    await update_statefulset(
                        apps,
                        namespace,
                        sts_name,
                        statefulset,
                        new_replicas,
                        total_number_of_nodes,
                    )
                    await scale_cluster_patch_total_nodes(
                        apps, namespace, name, spec, total_number_of_nodes
                    )

                await check_nodes_present_or_gone(
                    conn_factory,
                    old_replicas,
                    new_replicas,
                    f"data-{node_name}",
                    logger,
                )

    # Reset the deallocation
    if "master" in spec["nodes"]:
        reset_pod_name = f"crate-master-{name}-0"
    else:
        reset_pod_name = f"crate-data-{spec['nodes']['data'][0]['name']}-{name}-0"
    await reset_allocation(namespace, reset_pod_name, "ssl" in spec["cluster"])

    # Acknowledge all node checks that state that the expected number of nodes
    # doesn't line up. The StatefulSets have been adjusted, and once a pod
    # restarts, the node will know about it.
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE sys.node_checks SET acknowledged = TRUE WHERE id = 1
                """
            )


class ScaleSubHandler(StateBasedSubHandler):
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
        scale_master_diff_item: Optional[kopf.DiffItem] = None
        scale_data_diff_items: Optional[List[kopf.DiffItem]] = None

        for operation, field_path, old_value, new_value in diff:
            if field_path == ("spec", "nodes", "master", "replicas"):
                scale_master_diff_item = kopf.DiffItem(
                    operation, field_path, old_value, new_value
                )
            elif field_path == ("spec", "nodes", "data"):
                # TODO: check for data node order, added or removed types, ...
                if len(old_value) != len(new_value):
                    raise kopf.PermanentError(
                        "Adding and removing node specs is not supported at this time."
                    )
                scale_data_diff_items = []
                for node_spec_idx in range(len(old_value)):
                    old_spec = old_value[node_spec_idx]
                    new_spec = new_value[node_spec_idx]
                    inner_diff = calc_diff(old_spec, new_spec)
                    for (
                        inner_operation,
                        inner_field_path,
                        inner_old_value,
                        inner_new_value,
                    ) in inner_diff:
                        if inner_field_path == ("replicas",):
                            scale_data_diff_items.append(
                                kopf.DiffItem(
                                    inner_operation,
                                    (str(node_spec_idx),) + inner_field_path,
                                    inner_old_value,
                                    inner_new_value,
                                )
                            )
                        else:
                            logger.info(
                                "Ignoring operation %s on field %s",
                                operation,
                                field_path + (str(node_spec_idx),) + inner_field_path,
                            )
            else:
                logger.info("Ignoring operation %s on field %s", operation, field_path)

        async with ApiClient() as api_client:
            apps = AppsV1Api(api_client)
            core = CoreV1Api(api_client)

            await scale_cluster(
                apps,
                core,
                namespace,
                name,
                old,
                scale_master_diff_item,
                (kopf.Diff(scale_data_diff_items) if scale_data_diff_items else None),
                logger,
            )

        self.schedule_notification(
            WebhookEvent.SCALE,
            WebhookScalePayload(
                old_data_replicas=[
                    WebhookScaleNodePayload(
                        name=item["name"], replicas=item["replicas"]
                    )
                    for item in old["spec"]["nodes"]["data"]
                ],
                new_data_replicas=[
                    WebhookScaleNodePayload(
                        name=item["name"], replicas=item["replicas"]
                    )
                    for item in spec["nodes"]["data"]
                ],
                old_master_replicas=old["spec"]["nodes"]
                .get("master", {})
                .get("replicas"),
                new_master_replicas=spec["nodes"].get("master", {}).get("replicas"),
            ),
            WebhookStatus.SUCCESS,
        )
        await self.send_notifications(logger)


async def _ensure_cluster_healthy(
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

    if not await is_cluster_healthy(conn_factory, expected_number_of_nodes, logger):
        raise kopf.TemporaryError("Waiting for cluster to be healthy.", delay=15)
