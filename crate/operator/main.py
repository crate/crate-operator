import asyncio
import enum
import logging
from typing import Any, Dict, List

import kopf
from kubernetes_asyncio.client import (
    AppsV1Api,
    BatchV1beta1Api,
    CoreV1Api,
    V1LocalObjectReference,
    V1OwnerReference,
)

from crate.operator.backup import create_backups
from crate.operator.bootstrap import (
    bootstrap_license,
    bootstrap_system_user,
    bootstrap_users,
)
from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_PART_OF,
    RESOURCE_CRATEDB,
)
from crate.operator.create import (
    create_debug_volume,
    create_services,
    create_sql_exporter_config,
    create_statefulset,
    create_system_user,
)
from crate.operator.kube_auth import configure_kubernetes_client
from crate.operator.operations import get_total_nodes_count, restart_cluster
from crate.operator.upgrade import upgrade_cluster

logger = logging.getLogger(__name__)

NO_VALUE = object()


class Port(enum.Enum):
    HTTP = 4200
    JMX = 6666
    PROMETHEUS = 7071
    POSTGRES = 5432
    TRANSPORT = 4300


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


@kopf.on.startup()
async def startup(**kwargs):
    config.load()
    await configure_kubernetes_client()


@kopf.on.create(API_GROUP, "v1", RESOURCE_CRATEDB)
async def cluster_create(namespace, meta, spec, **kwargs):
    name = meta["name"]
    base_labels = {
        LABEL_MANAGED_BY: "crate-operator",
        LABEL_NAME: name,
        LABEL_PART_OF: "cratedb",
    }
    cratedb_labels = base_labels.copy()
    cratedb_labels[LABEL_COMPONENT] = "cratedb"
    cratedb_labels.update(meta.get("labels", {}))

    apps = AppsV1Api()
    batchv1_beta1 = BatchV1beta1Api()
    core = CoreV1Api()

    owner_references = [
        V1OwnerReference(
            api_version=f"{API_GROUP}/v1",
            block_owner_deletion=True,
            controller=True,
            kind="CrateDB",
            name=name,
            uid=meta["uid"],
        )
    ]

    image_pull_secrets = (
        [V1LocalObjectReference(name=secret) for secret in config.IMAGE_PULL_SECRETS]
        if config.IMAGE_PULL_SECRETS
        else None
    )

    ports_spec = spec.get("ports", {})
    http_port = ports_spec.get("http", Port.HTTP.value)
    jmx_port = ports_spec.get("jmx", Port.JMX.value)
    postgres_port = ports_spec.get("postgres", Port.POSTGRES.value)
    prometheus_port = ports_spec.get("prometheus", Port.PROMETHEUS.value)
    transport_port = ports_spec.get("transport", Port.TRANSPORT.value)

    master_nodes = get_master_nodes_names(spec["nodes"])
    total_nodes_count = get_total_nodes_count(spec["nodes"])
    crate_image = spec["cluster"]["imageRegistry"] + ":" + spec["cluster"]["version"]
    has_master_nodes = "master" in spec["nodes"]
    # The first StatefulSet we create references a set of master nodes. These
    # can either be explicit CrateDB master nodes, or implicit ones, which
    # would be the first set of nodes from the data nodes list.
    #
    # After the first StatefulSet was created, we set `treat_as_master` to
    # `False` to indicate that all remaining StatefulSets are neither explicit
    # nor implicit master nodes.
    treat_as_master = True
    sts = []
    if has_master_nodes:
        sts.append(
            create_statefulset(
                apps,
                owner_references,
                namespace,
                name,
                cratedb_labels,
                treat_as_master,
                False,
                "master",
                "master-",
                spec["nodes"]["master"],
                master_nodes,
                total_nodes_count,
                http_port,
                jmx_port,
                postgres_port,
                prometheus_port,
                transport_port,
                crate_image,
                spec["cluster"].get("ssl"),
                spec["cluster"].get("settings"),
                image_pull_secrets,
            )
        )
        treat_as_master = False
    for node_spec in spec["nodes"]["data"]:
        node_name = node_spec["name"]
        sts.append(
            create_statefulset(
                apps,
                owner_references,
                namespace,
                name,
                cratedb_labels,
                treat_as_master,
                True,
                node_name,
                f"data-{node_name}-",
                node_spec,
                master_nodes,
                total_nodes_count,
                http_port,
                jmx_port,
                postgres_port,
                prometheus_port,
                transport_port,
                crate_image,
                spec["cluster"].get("ssl"),
                spec["cluster"].get("settings"),
                image_pull_secrets,
            )
        )
        treat_as_master = False

    await asyncio.gather(
        create_sql_exporter_config(
            core, owner_references, namespace, name, cratedb_labels
        ),
        *create_debug_volume(core, owner_references, namespace, name, cratedb_labels),
        create_system_user(core, owner_references, namespace, name, cratedb_labels),
        *sts,
        *create_services(
            core,
            owner_references,
            namespace,
            name,
            cratedb_labels,
            http_port,
            postgres_port,
            transport_port,
            spec.get("cluster", {}).get("externalDNS"),
        ),
    )

    if has_master_nodes:
        master_node_pod = f"crate-master-{name}-0"
    else:
        node_name = spec["nodes"]["data"][0]["name"]
        master_node_pod = f"crate-data-{node_name}-{name}-0"

    if "license" in spec["cluster"]:
        # We first need to set the license, in case the CrateDB cluster
        # contains more nodes than available in the free license.
        await bootstrap_license(
            core,
            namespace,
            master_node_pod,
            "ssl" in spec["cluster"],
            spec["cluster"]["license"],
        )

    await bootstrap_system_user(
        core, namespace, name, master_node_pod, "ssl" in spec["cluster"]
    )

    if "users" in spec:
        await bootstrap_users(core, namespace, name, spec["users"])

    if "backups" in spec:
        backup_metrics_labels = base_labels.copy()
        backup_metrics_labels[LABEL_COMPONENT] = "backup"
        backup_metrics_labels.update(meta.get("labels", {}))
        await asyncio.gather(
            *create_backups(
                apps,
                batchv1_beta1,
                owner_references,
                namespace,
                name,
                backup_metrics_labels,
                http_port,
                prometheus_port,
                spec["backups"],
                image_pull_secrets,
                "ssl" in spec["cluster"],
            )
        )


@kopf.on.update(API_GROUP, "v1", RESOURCE_CRATEDB)
async def cluster_update(
    diff: kopf.Diff, namespace: str, name: str, body: kopf.Body, **kwargs,
):
    """
    Implement any updates to a cluster. The handler will sort out the logic of
    what to update in which order and when to trigger a restart of a cluster.
    """
    # Map fields to (handlers, a weight (bigger is important), requires restart)
    handlers = {
        ("spec", "cluster", "imageRegistry"): (upgrade_cluster, 10, True),
        ("spec", "cluster", "version"): (upgrade_cluster, 10, True),
    }
    run_handlers = set()

    requires_restart = False
    for operation, field_path, old_value, new_value in diff:
        if field_path in handlers:
            run_handlers.add(handlers[field_path])
        else:
            logger.info("Ignoring operation %s on field %s", operation, field_path)

    # Run through all required handlers in the order of their weight (bigger
    # weight means running earlier).
    sorted_handlers = reversed(sorted(run_handlers, key=lambda e: e[1]))
    for handler, _, restart in sorted_handlers:
        await handler(namespace, name, body)
        requires_restart = requires_restart or restart

    if requires_restart:
        try:
            timeout = config.ROLLING_RESTART_TIMEOUT
            awaitable = restart_cluster(namespace, name)
            if timeout > 0:
                awaitable = asyncio.wait_for(awaitable, timeout=timeout)  # type: ignore
            await awaitable
        except asyncio.TimeoutError:
            raise kopf.PermanentError(
                f"Failed to restart cluster {namespace}/{name} after "
                f"{config.ROLLING_RESTART_TIMEOUT} seconds."
            ) from None
