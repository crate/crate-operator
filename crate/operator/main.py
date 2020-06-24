import asyncio
import enum
import logging
from typing import Any, Awaitable, Dict, List, Optional

import kopf
from kopf.structs.diffs import diff as calc_diff
from kubernetes_asyncio.client import (
    AppsV1Api,
    BatchV1beta1Api,
    CoreV1Api,
    V1LocalObjectReference,
    V1OwnerReference,
)

from crate.operator.backup import create_backups
from crate.operator.bootstrap import bootstrap_cluster
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
from crate.operator.scale import scale_cluster
from crate.operator.upgrade import upgrade_cluster
from crate.operator.webhooks import (
    WebhookScaleNodePayload,
    WebhookScalePayload,
    WebhookStatus,
    WebhookUpgradePayload,
    webhook_client,
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


async def with_timeout(awaitable: Awaitable, timeout: int, error: str) -> None:
    """
    Wait up to ``timeout`` seconds for ``awaitable`` to finish before failing.

    When ``timeout`` is ``<= 0``, no timeout will be applied.

    :raises kopf.PermanentError: When the timeout is reached, raises an error
        with message ``errorr``.
    """
    try:
        if timeout > 0:
            awaitable = asyncio.wait_for(awaitable, timeout=timeout)  # type: ignore
        await awaitable
    except asyncio.TimeoutError:
        raise kopf.PermanentError(error) from None


@kopf.on.startup()
async def startup(**kwargs):
    config.load()
    await configure_kubernetes_client()
    if (
        config.WEBHOOK_PASSWORD is not None
        and config.WEBHOOK_URL is not None
        and config.WEBHOOK_USERNAME is not None
    ):
        webhook_client.configure(
            config.WEBHOOK_URL, config.WEBHOOK_USERNAME, config.WEBHOOK_PASSWORD
        )


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

    await with_timeout(
        bootstrap_cluster(
            core,
            namespace,
            name,
            master_node_pod,
            spec["cluster"].get("license"),
            "ssl" in spec["cluster"],
            spec.get("users"),
        ),
        config.BOOTSTRAP_TIMEOUT,
        (
            f"Failed to bootstrap cluster {namespace}/{name} after "
            f"{config.BOOTSTRAP_TIMEOUT} seconds."
        ),
    )

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
    namespace: str,
    name: str,
    body: kopf.Body,
    spec: kopf.Spec,
    diff: kopf.Diff,
    old: kopf.Body,
    **kwargs,
):
    """
    Implement any updates to a cluster. The handler will sort out the logic of
    what to update, in which order, and when to trigger a restart of a cluster.
    """
    apps = AppsV1Api()
    do_scale_master = False
    do_scale_data = False
    do_upgrade = False
    requires_restart = False
    scale_master_diff_item: Optional[kopf.DiffItem] = None
    scale_data_diff_items: Optional[List[kopf.DiffItem]] = None

    for operation, field_path, old_value, new_value in diff:
        if field_path in {
            ("spec", "cluster", "imageRegistry"),
            ("spec", "cluster", "version",),
        }:
            do_upgrade = True
        elif field_path == ("spec", "nodes", "master", "replicas"):
            do_scale_master = True
            scale_master_diff_item = kopf.DiffItem(
                operation, field_path, old_value, new_value
            )
        elif field_path == ("spec", "nodes", "data"):
            # TODO: check for data node order, added or removed types, ...
            if len(old_value) != len(new_value):
                raise kopf.PermanentError(
                    "Cannot handle changes to the number of node specs."
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
                        do_scale_data = True
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

    if do_upgrade:
        webhook_upgrade_payload = WebhookUpgradePayload(
            old_registry=old["spec"]["cluster"]["imageRegistry"],
            new_registry=body.spec["cluster"]["imageRegistry"],
            old_version=old["spec"]["cluster"]["version"],
            new_version=body.spec["cluster"]["version"],
        )
        await upgrade_cluster(namespace, name, body)
        requires_restart = True

    if requires_restart:
        try:
            # We need to derive the desired number of nodes from the old spec,
            # since the new could have a different total number of nodes if a
            # scaling operation is in progress as well.
            expected_nodes = get_total_nodes_count(old["spec"]["nodes"])
            await with_timeout(
                restart_cluster(namespace, name, expected_nodes),
                config.ROLLING_RESTART_TIMEOUT,
                (
                    f"Failed to restart cluster {namespace}/{name} after "
                    f"{config.ROLLING_RESTART_TIMEOUT} seconds."
                ),
            )
        except Exception:
            logger.exception("Failed to restart cluster")
            await webhook_client.send_upgrade_notification(
                WebhookStatus.FAILURE, namespace, name, webhook_upgrade_payload
            )
            raise
        else:
            logger.info("Cluster restarted")
            if do_upgrade:
                await webhook_client.send_upgrade_notification(
                    WebhookStatus.SUCCESS, namespace, name, webhook_upgrade_payload
                )

    if do_scale_master or do_scale_data:
        webhook_scale_payload = WebhookScalePayload(
            old_data_replicas=[
                WebhookScaleNodePayload(name=item["name"], replicas=item["replicas"])
                for item in old["spec"]["nodes"]["data"]
            ],
            new_data_replicas=[
                WebhookScaleNodePayload(name=item["name"], replicas=item["replicas"])
                for item in body.spec["nodes"]["data"]
            ],
            old_master_replicas=old["spec"]["nodes"].get("master", {}).get("replicas"),
            new_master_replicas=body.spec["nodes"].get("master", {}).get("replicas"),
        )
        try:
            await with_timeout(
                scale_cluster(
                    apps,
                    namespace,
                    name,
                    do_scale_data,
                    do_scale_master,
                    get_total_nodes_count(old["spec"]["nodes"]),
                    spec,
                    scale_master_diff_item,
                    kopf.Diff(scale_data_diff_items) if scale_data_diff_items else None,
                ),
                config.SCALING_TIMEOUT,
                (
                    f"Failed to scale cluster {namespace}/{name} after "
                    f"{config.SCALING_TIMEOUT} seconds."
                ),
            )
        except Exception:
            logger.exception("Failed to scale cluster")
            await webhook_client.send_scale_notification(
                WebhookStatus.FAILURE, namespace, name, webhook_scale_payload
            )
            raise
        else:
            logger.info("Cluster scaled")
            await webhook_client.send_scale_notification(
                WebhookStatus.SUCCESS, namespace, name, webhook_scale_payload
            )
