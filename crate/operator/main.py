import asyncio
import enum
import logging
from typing import Any, Dict, List

import kopf
from kubernetes_asyncio.client import AppsV1Api, BatchV1beta1Api, CoreV1Api
from kubernetes_asyncio.client.models import V1DeleteOptions, V1LocalObjectReference

# Perform Kubernetes authentication during Kopf startup. This also triggers the
# login for the Kopf framework through PyKube
import crate.operator.kube_auth  # noqa
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
    create_backups,
    create_configs,
    create_debug_volume,
    create_services,
    create_statefulset,
    create_system_user,
)

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


def get_total_nodes_count(nodes: Dict[str, Any]) -> int:
    """
    Calculate the total number nodes a CrateDB cluster should have on startup.

    When starting CrateDB it's importent to know the expected number of nodes
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
    batchv1beta1 = BatchV1beta1Api()
    core = CoreV1Api()

    image_pull_secrets = (
        [V1LocalObjectReference(name=secret) for secret in config.IMAGE_PULL_SECRETS]
        if config.IMAGE_PULL_SECRETS
        else None
    )

    http_port = spec.get("ports", {}).get("http", Port.HTTP.value)
    jmx_port = spec.get("ports", {}).get("jmx", Port.JMX.value)
    postgres_port = spec.get("ports", {}).get("postgres", Port.POSTGRES.value)
    prometheus_port = spec.get("ports", {}).get("prometheus", Port.PROMETHEUS.value)
    transport_port = spec.get("ports", {}).get("transport", Port.TRANSPORT.value)

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
                spec.get("users"),
                spec["cluster"].get("license"),
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
                spec.get("users"),
                spec["cluster"].get("license"),
                spec["cluster"].get("ssl"),
                spec["cluster"].get("settings"),
                image_pull_secrets,
            )
        )
        treat_as_master = False

    await asyncio.gather(
        *create_configs(
            core, namespace, name, cratedb_labels, jmx_port, prometheus_port
        ),
        *create_debug_volume(core, namespace, name, cratedb_labels),
        create_system_user(core, namespace, name, cratedb_labels),
        *sts,
        *create_services(
            core,
            namespace,
            name,
            cratedb_labels,
            http_port,
            postgres_port,
            transport_port,
            spec.get("cluster", {}).get("externalDNS"),
        ),
    )

    if "backups" in spec:
        backup_metrics_labels = base_labels.copy()
        backup_metrics_labels[LABEL_COMPONENT] = "backup"
        backup_metrics_labels.update(meta.get("labels", {}))
        await asyncio.gather(
            *create_backups(
                apps,
                batchv1beta1,
                namespace,
                name,
                backup_metrics_labels,
                spec["backups"],
                image_pull_secrets,
            )
        )


@kopf.on.delete(API_GROUP, "v1", RESOURCE_CRATEDB)
async def delete_cluster(namespace, meta, spec, **_):
    apps = AppsV1Api()
    batchv1beta1 = BatchV1beta1Api()
    core = CoreV1Api()

    name = meta["name"]

    to_delete = []
    to_delete.append(
        batchv1beta1.delete_namespaced_cron_job(
            namespace=namespace, name=f"create-snapshot-{name}", body=V1DeleteOptions(),
        )
    )
    to_delete.append(
        apps.delete_namespaced_deployment(
            namespace=namespace, name=f"backup-metrics-{name}", body=V1DeleteOptions(),
        )
    )
    master_node_spec = spec["nodes"].get("master")
    if master_node_spec:
        to_delete.append(
            apps.delete_namespaced_stateful_set(
                namespace=namespace, name=f"crate-master-{name}"
            )
        )
    to_delete.extend(
        [
            apps.delete_namespaced_stateful_set(
                namespace=namespace,
                name=f"crate-data-{node_spec['name']}-{name}",
                body=V1DeleteOptions(),
            )
            for node_spec in spec["nodes"]["data"]
        ]
    )
    to_delete.extend(
        [
            core.delete_namespaced_service(
                namespace=namespace,
                name=f"crate-discovery-{name}",
                body=V1DeleteOptions(),
            ),
            core.delete_namespaced_service(
                namespace=namespace, name=f"crate-{name}", body=V1DeleteOptions(),
            ),
        ]
    )
    to_delete.extend(
        [
            core.delete_namespaced_config_map(
                namespace=namespace, name=config_map_name, body=V1DeleteOptions(),
            )
            for config_map_name in (
                f"crate-{name}",
                f"crate-log4j2-{name}",
                f"crate-sql-exporter-{name}",
            )
        ]
    )
    to_delete.append(
        core.delete_namespaced_secret(
            namespace=namespace, name=f"user-system-{name}", body=V1DeleteOptions(),
        )
    )

    if master_node_spec:
        to_delete.extend(
            [
                core.delete_namespaced_persistent_volume_claim(
                    namespace=namespace,
                    name=f"data{i}-crate-master-{name}-{replica}",
                    body=V1DeleteOptions(),
                )
                for replica in range(master_node_spec["replicas"])
                for i in range(master_node_spec["resources"]["disk"]["count"])
            ]
        )
    to_delete.extend(
        [
            core.delete_namespaced_persistent_volume_claim(
                namespace=namespace,
                name=f"data{i}-crate-data-{node_spec['name']}-{name}-{replica}",
                body=V1DeleteOptions(),
            )
            for node_spec in spec["nodes"]["data"]
            for replica in range(node_spec["replicas"])
            for i in range(node_spec["resources"]["disk"]["count"])
        ]
    )
    to_delete.append(
        core.delete_namespaced_persistent_volume_claim(
            namespace=namespace, name=f"local-resource-{name}", body=V1DeleteOptions(),
        )
    )
    to_delete.append(
        core.delete_persistent_volume(
            name=f"temp-pv-{namespace}-{name}", body=V1DeleteOptions()
        )
    )

    await asyncio.gather(*to_delete, return_exceptions=True)
