# CrateDB Kubernetes Operator
# Copyright (C) 2020 Crate.io AT GmbH
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
import sys
from typing import Any, Dict, List, Optional, Tuple

import kopf
from aiopg import Cursor
from kubernetes_asyncio.client import AppsV1Api, CoreV1Api, V1Container, V1StatefulSet
from kubernetes_asyncio.stream import WsApiClient

from crate.operator.constants import BACKOFF_TIME
from crate.operator.cratedb import connection_factory, wait_for_healthy_cluster
from crate.operator.utils import quorum
from crate.operator.utils.kubeapi import get_host, get_system_user_password


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
        r = r.split("-", 1)[1]
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
        if item.startswith("-Cgateway.recover_after_nodes="):
            item = f"-Cgateway.recover_after_nodes={quorum(total_nodes)}"
        elif item.startswith("-Cgateway.expected_nodes="):
            item = f"-Cgateway.expected_nodes={total_nodes}"
        new_command.append(item)
    return new_command


async def update_statefulset(
    apps: AppsV1Api,
    namespace: str,
    sts_name: str,
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
    statefulset = await apps.read_namespaced_stateful_set(
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
        namespace=namespace, name=sts_name, body=body,
    )


async def get_current_statefulset_replicas(
    apps: AppsV1Api, namespace: str, sts_name: str,
) -> int:
    """
    Call the Kubernetes API and return the number of replicas for the
    StatefulSet ``sts_name``.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param sts_name: The name for the Kubernetes StatefulSet to update.
    :return: The number of replicas configured on a StatefulSet.
    """
    statefulset = await apps.read_namespaced_stateful_set(
        namespace=namespace, name=sts_name
    )
    return statefulset.spec.replicas


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


async def wait_for_deallocation(
    cursor: Cursor, node_names: List[str], logger: logging.Logger,
):
    """
    Wait until the nodes ``node_names`` have no more shards.

    :param cursor: A database cursor to a current and open database connection.
    :param node_names: A list of CrateDB node names. These are the names that
        are known to CrateDB, e.g. ``data-hot-2`` or ``master-1``.
    """
    while True:
        logger.info(
            "Waiting for deallocation of CrateDB nodes %s ...", ", ".join(node_names)
        )
        # We select the node names and the number of shards for all nodes that
        # will be torn down. As long as the rowcount is > 0 (see the backoff
        # predicate above), the nodes in question still have shards allocated
        # and thus can't be decommissioned. Thus, backoff will retry again.
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
            await asyncio.sleep(BACKOFF_TIME / 2.0)
        else:
            logger.info("No more pending allocations")
            break


async def scale_up_statefulset(
    apps: AppsV1Api,
    namespace: str,
    sts_name: str,
    node_spec: Dict[str, Any],
    conn_factory,
    old_total_nodes: int,
    num_add_nodes: int,
    logger: logging.Logger,
) -> int:
    """
    Scale the StatefulSet ``sts_name`` up to the desired number of replicas.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param sts_name: The name for the Kubernetes StatefulSet to update.
    :param node_spec: A node specification from the custom resource. Either
        ``spec.nodes.master`` or ``spec.nodes.data.*``.
    :param conn_factory: A function that establishes a database connection to
        the CrateDB cluster used for SQL queries.
    :param old_total_nodes: The total number of nodes in the CrateDB cluster
        *before* scaling up this StatefulSet.
    :param num_add_nodes: The number of *additional* nodes to add to the
        StatefulSet and CrateDB cluster.
    :return: The total number of nodes in the CrateDB cluster *after* scaling
        up this StatefulSet.
    """
    new_total_nodes = old_total_nodes + num_add_nodes

    replicas = node_spec["replicas"]
    if await get_current_statefulset_replicas(apps, namespace, sts_name) == replicas:
        # short-circuit here when nothing needs to be done on the stateful set.
        return new_total_nodes

    await update_statefulset(
        apps, namespace, sts_name, replicas, new_total_nodes,
    )

    await wait_for_healthy_cluster(conn_factory, new_total_nodes, logger)

    return new_total_nodes


async def scale_down_statefulset(
    apps: AppsV1Api,
    namespace: str,
    sts_name: str,
    node_spec: Dict[str, Any],
    conn_factory,
    old_total_nodes: int,
    num_excess_nodes: int,
    num_master_nodes: int,
    logger: logging.Logger,
) -> int:
    """
    Scale the StatefulSet ``sts_name`` down to the desired number of replicas.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param sts_name: The name for the Kubernetes StatefulSet to update.
    :param node_spec: A node specification from the custom resource. Either
        ``spec.nodes.master`` or ``spec.nodes.data.*``.
    :param conn_factory: A function that establishes a database connection to
        the CrateDB cluster used for SQL queries.
    :param old_total_nodes: The total number of nodes in the CrateDB cluster
        *before* scaling down this StatefulSet.
    :param num_excess_nodes: The number of *excess* nodes to remove from the
        StatefulSet and CrateDB cluster.
    :param num_master_nodes: The number of **dedicated** master nodes in the
        CrateDB cluster. This is required to determine the number of data nodes
        available in the cluster at a given time.
    :return: The total number of nodes in the CrateDB cluster *after* scaling
        down this StatefulSet.
    """
    new_total_nodes = old_total_nodes - num_excess_nodes

    replicas = node_spec["replicas"]
    if await get_current_statefulset_replicas(apps, namespace, sts_name) == replicas:
        # short-circuit here when nothing needs to be done on the stateful set.
        return new_total_nodes

    node_name = node_spec["name"]
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
            # A table has 1 (primary) + n replicas.
            # If that number is larger than the number of data nodes in the
            # cluster (total nodes - *dedicated* master nodes) then there are
            # not enough nodes to have all tables fully replicated. Thus,
            # failing the scaling.
            if max_replicas + 1 > new_total_nodes - num_master_nodes:
                raise kopf.TemporaryError(
                    "Some tables have too many replicas to scale down"
                )

            # 2. Exclude current-new nodes from allocating shards
            # A list of CrateDB node names that will be removed.
            to_remove_node_names = [
                f"data-{node_name}-{x}"
                for x in range(replicas, replicas + num_excess_nodes)
            ]
            await cursor.execute(
                """
                    SET GLOBAL "cluster.routing.allocation.exclude._name" = %s
                    """,
                (",".join(to_remove_node_names),),
            )

            # 3. Wait for nodes to be deallocated
            await wait_for_deallocation(cursor, to_remove_node_names, logger)

    # 4. Patch statefulset with new number of nodes. This will kill excess
    # pods.
    await update_statefulset(
        apps, namespace, sts_name, replicas, new_total_nodes,
    )

    await wait_for_healthy_cluster(conn_factory, new_total_nodes, logger)

    return new_total_nodes


async def scale_cluster_data_nodes(
    apps: AppsV1Api,
    namespace: str,
    name: str,
    spec: kopf.Spec,
    diff: kopf.Diff,
    conn_factory,
    old_total_nodes: int,
    logger: logging.Logger,
) -> int:
    """
    Scale all data node StatefulSets according to their ``diff``.

    The function first works out which StatefulSets need to be scaled up and
    which need to be scaled down. It will then first perform all scale up
    operations and then the more time consuming scale down operations. This is
    so that there are enough data nodes around where shards can be migrated to
    before terminating excess nodes.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param spec: The ``spec`` field from the new CrateDB cluster custom object.
    :param diff: A list of changes made to the individual
         data node specifications.
    :param conn_factory: A function that establishes a database connection to
        the CrateDB cluster used for SQL queries.
    :param old_total_nodes: The total number of nodes in the CrateDB cluster
        *before* scaling any StatefulSet.
    :return: The total number of nodes in the CrateDB cluster *after* scaling
        all data node StatefulSets.
    """

    # The StatefulSets that will be scaled up (index, num additional nodes)
    scale_up_sts: List[Tuple[int, int]] = []
    # The StatefulSets that will be scaled down (index, num removed nodes)
    scale_down_sts: List[Tuple[int, int]] = []
    for _, field_path, old_value, new_value in diff:
        index, *_ = field_path
        index = int(index)
        if old_value < new_value:
            scale_up_sts.append((index, new_value - old_value))
        else:
            scale_down_sts.append((index, old_value - new_value))

    new_total_nodes = old_total_nodes

    for index, num_add_nodes in scale_up_sts:
        # We iterate over all StatefulSets that are growing and will update
        # each one individually. Each time, we increase the number of expected
        # nodes in the cluster according to that StatefulSet's change.
        # Later, we'll update all StatefulSets in the cluster with the new
        # total number of nodes.
        node_spec = spec["nodes"]["data"][index]
        node_name = node_spec["name"]
        new_total_nodes = await scale_up_statefulset(
            apps,
            namespace,
            f"crate-data-{node_name}-{name}",
            spec["nodes"]["data"][index],
            conn_factory,
            new_total_nodes,
            num_add_nodes,
            logger,
        )

    # Number of dedicated master nodes. Required to determine the available
    # number of data nodes.
    num_master_nodes = 0
    if "master" in spec["nodes"]:
        num_master_nodes = spec["nodes"]["master"]["replicas"]

    for index, num_rem_nodes in scale_down_sts:
        node_spec = spec["nodes"]["data"][index]
        node_name = node_spec["name"]
        new_total_nodes = await scale_down_statefulset(
            apps,
            namespace,
            f"crate-data-{node_name}-{name}",
            spec["nodes"]["data"][index],
            conn_factory,
            new_total_nodes,
            num_rem_nodes,
            num_master_nodes,
            logger,
        )

    if scale_down_sts:
        # Reset the deallocation
        if "master" in spec["nodes"]:
            reset_pod_name = f"crate-master-{name}-0"
        else:
            reset_pod_name = f"crate-data-{spec['nodes']['data'][0]['name']}-{name}-0"
        await reset_allocation(namespace, reset_pod_name, "ssl" in spec["cluster"])

    return new_total_nodes


async def reset_allocation(namespace: str, pod_name: str, has_ssl: bool) -> None:
    """
    Reset all temporary node deallocations to none.

    .. note::

       Ideally, we'd be using the system user to reset the allocation
       exclusions. However, `due to a bug
       <https://github.com/crate/crate/pull/10083>`_, this isn't possible in
       CrateDB <= 4.1.6. We therefore fall back to the "exce-in-container"
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
    core_ws = CoreV1Api(api_client=WsApiClient())
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


async def scale_cluster_master_nodes(
    apps: AppsV1Api,
    namespace: str,
    name: str,
    spec: kopf.Spec,
    diff: kopf.DiffItem,
    conn_factory,
    old_total_nodes: int,
    logger: logging.Logger,
) -> int:
    """
    Scale a dedicated master node StatefulSet to the desired number of replicas.

    Since dedicated master nodes don't have an data, it's fine to "just" scale
    them, without dealing with CrateDB directly. We'll only wait for a cluster
    to recover at the end to ensure nothing else broke in the mean time.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param spec: The ``spec`` field from the new CrateDB cluster custom object.
    :param diff: The change made to the the master node specification.
    :param conn_factory: A function that establishes a database connection to
        the CrateDB cluster used for SQL queries.
    :param old_total_nodes: The total number of nodes in the CrateDB cluster
        *before* scaling the StatefulSet.
    :return: The total number of nodes in the CrateDB cluster *after* scaling
        the master node StatefulSet.
    """
    _, _, old_value, new_value = diff
    sts_name = f"crate-master-{name}"
    nodes_count_delta = new_value - old_value
    new_total_nodes = old_total_nodes + nodes_count_delta

    if await get_current_statefulset_replicas(apps, namespace, sts_name) == new_value:
        # short-circuit here when nothing needs to be done on the stateful set.
        return new_total_nodes

    await update_statefulset(
        apps,
        namespace,
        f"crate-master-{name}",
        spec["nodes"]["master"]["replicas"],
        new_total_nodes,
    )

    await wait_for_healthy_cluster(conn_factory, new_total_nodes, logger)

    return new_total_nodes


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
                apps, namespace, f"crate-master-{name}", None, total_nodes
            )
        )
    updates.extend(
        [
            update_statefulset(
                apps,
                namespace,
                f"crate-data-{node_spec['name']}-{name}",
                None,
                total_nodes,
            )
            for node_spec in spec["nodes"]["data"]
        ]
    )
    await asyncio.gather(*updates)


async def scale_cluster(
    apps: AppsV1Api,
    namespace: str,
    name: str,
    do_scale_data: bool,
    do_scale_master: bool,
    old_total_nodes: int,
    spec: kopf.Spec,
    master_diff_item: Optional[kopf.DiffItem],
    data_diff_items: Optional[kopf.Diff],
    logger: logging.Logger,
):
    """
    Scale cluster ``name`` according to the given ``master_diff_item`` and
    ``data_diff_items``.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    :param do_scale_data: ``True``, if data nodes need to be scaled.
    :param do_scale_master: ``True``, if master nodes need to be scaled.
    :param old_total_nodes: The total number of nodes in the CrateDB cluster
        *before* scaling the StatefulSet.
    :param spec: The ``spec`` field from the new CrateDB cluster custom object.
    :param master_diff_item: An optional change indicating how many master
        nodes a cluster should have.
    :param data_diff_items: An optional list of changes made to the individual
        data node specifications.
    """
    core = CoreV1Api()

    host = await get_host(core, namespace, name)
    password = await get_system_user_password(namespace, name)
    conn_factory = connection_factory(host, password)

    total_nodes = old_total_nodes
    if do_scale_master:
        total_nodes = await scale_cluster_master_nodes(
            apps,
            namespace,
            name,
            spec,
            master_diff_item,
            conn_factory,
            total_nodes,
            logger,
        )

    if do_scale_data:
        total_nodes = await scale_cluster_data_nodes(
            apps,
            namespace,
            name,
            spec,
            data_diff_items,
            conn_factory,
            total_nodes,
            logger,
        )

    await scale_cluster_patch_total_nodes(apps, namespace, name, spec, total_nodes)

    # Acknowledge all node checks that state that the expected number of nodes
    # doesn't line up. The StatefulSets have been adjusted, and once a pod
    # restarts, the node will know about it.
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """UPDATE sys.node_checks SET acknowledged = TRUE WHERE id = 1"""
            )
