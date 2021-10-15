import logging

import kopf
from kubernetes_asyncio.client import V1LocalObjectReference, V1OwnerReference

from crate.operator.backup import create_backups
from crate.operator.bootstrap import bootstrap_cluster
from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_PART_OF,
    Port,
)
from crate.operator.create import (
    create_debug_volume,
    create_services,
    create_sql_exporter_config,
    create_statefulset,
    create_system_user,
)
from crate.operator.operations import get_master_nodes_names, get_total_nodes_count
from crate.operator.utils.kopf import subhandler_partial


async def create_cratedb(
    namespace: str, meta: kopf.Meta, spec: kopf.Spec, logger: logging.Logger
):
    name = meta["name"]
    base_labels = {
        LABEL_MANAGED_BY: "crate-operator",
        LABEL_NAME: name,
        LABEL_PART_OF: "cratedb",
    }
    cratedb_labels = base_labels.copy()
    cratedb_labels[LABEL_COMPONENT] = "cratedb"
    cratedb_labels.update(meta.get("labels", {}))

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
    total_nodes_count = get_total_nodes_count(spec["nodes"], "all")
    data_nodes_count = get_total_nodes_count(spec["nodes"], "data")
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
    cluster_name = spec["cluster"]["name"]
    source_ranges = spec["cluster"].get("allowedCIDRs", None)

    kopf.register(
        fn=subhandler_partial(
            create_sql_exporter_config,
            owner_references,
            namespace,
            name,
            cratedb_labels,
            logger,
        ),
        id="sql_exporter_config",
    )

    kopf.register(
        fn=subhandler_partial(
            create_debug_volume,
            owner_references,
            namespace,
            name,
            cratedb_labels,
            logger,
        ),
        id="debug_volume",
    )

    kopf.register(
        fn=subhandler_partial(
            create_system_user,
            owner_references,
            namespace,
            name,
            cratedb_labels,
            logger,
        ),
        id="system_user",
    )

    kopf.register(
        fn=subhandler_partial(
            create_services,
            owner_references,
            namespace,
            name,
            cratedb_labels,
            http_port,
            postgres_port,
            transport_port,
            spec.get("cluster", {}).get("externalDNS"),
            logger,
            source_ranges,
        ),
        id="services",
    )

    if has_master_nodes:
        kopf.register(
            fn=subhandler_partial(
                create_statefulset,
                owner_references,
                namespace,
                name,
                cratedb_labels,
                treat_as_master,
                False,
                cluster_name,
                "master",
                "master-",
                spec["nodes"]["master"],
                master_nodes,
                total_nodes_count,
                data_nodes_count,
                http_port,
                jmx_port,
                postgres_port,
                prometheus_port,
                transport_port,
                crate_image,
                spec["cluster"].get("ssl"),
                spec["cluster"].get("settings"),
                image_pull_secrets,
                logger,
            ),
            id="statefulset_master",
        )
        treat_as_master = False

    for node_spec in spec["nodes"]["data"]:
        node_name = node_spec["name"]
        kopf.register(
            fn=subhandler_partial(
                create_statefulset,
                owner_references,
                namespace,
                name,
                cratedb_labels,
                treat_as_master,
                True,
                cluster_name,
                node_name,
                f"data-{node_name}-",
                node_spec,
                master_nodes,
                total_nodes_count,
                data_nodes_count,
                http_port,
                jmx_port,
                postgres_port,
                prometheus_port,
                transport_port,
                crate_image,
                spec["cluster"].get("ssl"),
                spec["cluster"].get("settings"),
                image_pull_secrets,
                logger,
            ),
            id=f"statefulset_data_{node_name}",
        )
        treat_as_master = False

    if has_master_nodes:
        master_node_pod = f"crate-master-{name}-0"
    else:
        node_name = spec["nodes"]["data"][0]["name"]
        master_node_pod = f"crate-data-{node_name}-{name}-0"

    kopf.register(
        fn=subhandler_partial(
            bootstrap_cluster,
            namespace,
            name,
            master_node_pod,
            spec["cluster"].get("license"),
            "ssl" in spec["cluster"],
            spec.get("users"),
            logger,
        ),
        id="bootstrap",
        timeout=config.BOOTSTRAP_TIMEOUT,
        backoff=config.BOOTSTRAP_RETRY_DELAY,
    )

    if "backups" in spec:
        if config.CLUSTER_BACKUP_IMAGE is None:
            logger.info(
                "Not deploying backup tools because no backup image is defined."
            )
        else:
            backup_metrics_labels = base_labels.copy()
            backup_metrics_labels[LABEL_COMPONENT] = "backup"
            backup_metrics_labels.update(meta.get("labels", {}))
            kopf.register(
                fn=subhandler_partial(
                    create_backups,
                    owner_references,
                    namespace,
                    name,
                    backup_metrics_labels,
                    http_port,
                    prometheus_port,
                    spec["backups"],
                    image_pull_secrets,
                    "ssl" in spec["cluster"],
                    logger,
                ),
                id="backup",
            )
