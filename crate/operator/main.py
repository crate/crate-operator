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
import enum
import logging
import warnings
from typing import Any, Awaitable, Dict, List

import kopf
from kubernetes_asyncio.client import (
    CoreV1Api,
    CustomObjectsApi,
    V1LocalObjectReference,
    V1OwnerReference,
)
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.backup import create_backups
from crate.operator.bootstrap import bootstrap_cluster
from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_PART_OF,
    LABEL_USER_PASSWORD,
    RESOURCE_CRATEDB,
)
from crate.operator.create import (
    create_debug_volume,
    create_services,
    create_sql_exporter_config,
    create_statefulset,
    create_system_user,
)
from crate.operator.kube_auth import login_via_kubernetes_asyncio
from crate.operator.operations import RestartSubHandler, get_total_nodes_count
from crate.operator.scale import ScaleSubHandler
from crate.operator.update_user_password import update_user_password
from crate.operator.upgrade import UpgradeSubHandler
from crate.operator.utils.kopf import subhandler_partial
from crate.operator.utils.kubeapi import ensure_user_password_label, get_host
from crate.operator.utils.state import Context, State
from crate.operator.webhooks import webhook_client

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
async def startup(settings: kopf.OperatorSettings, **kwargs):
    config.load()
    if (
        config.WEBHOOK_PASSWORD is not None
        and config.WEBHOOK_URL is not None
        and config.WEBHOOK_USERNAME is not None
    ):
        webhook_client.configure(
            config.WEBHOOK_URL, config.WEBHOOK_USERNAME, config.WEBHOOK_PASSWORD
        )

    warnings.warn(
        "The 'kopf.zalando.org/*' annotations and the "
        "'kopf.zalando.org/KopfFinalizerMarker' finalizer are deprecated and will be "
        "removed in version 2.0.",
        DeprecationWarning,
    )
    # TODO: In version 2.0 change to:
    # settings.persistence.diffbase_storage = kopf.AnnotationsDiffBaseStorage(
    #     prefix=f"operator.{API_GROUP}", key="last", v1=False
    # )
    settings.persistence.diffbase_storage = kopf.MultiDiffBaseStorage(
        [
            kopf.AnnotationsDiffBaseStorage(
                prefix=f"operator.{API_GROUP}", key="last", v1=False
            ),
            kopf.AnnotationsDiffBaseStorage(),  # For backwards compatibility
        ]
    )
    settings.persistence.finalizer = f"operator.{API_GROUP}/finalizer"
    # TODO: In version 2.0 change to:
    # settings.persistence.progress_storage = (
    #     kopf.AnnotationsProgressStorage(prefix=f"operator.{API_GROUP}", v1=False),
    # )
    settings.persistence.progress_storage = kopf.MultiProgressStorage(
        [
            kopf.AnnotationsProgressStorage(prefix=f"operator.{API_GROUP}", v1=False),
            kopf.AnnotationsProgressStorage(),  # For backwards compatibility
        ]
    )

    # Timeout passed along to the Kubernetes API as timeoutSeconds=x
    settings.watching.server_timeout = 300
    # Total number of seconds for a whole watch request per aiohttp:
    # https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientTimeout.total
    settings.watching.client_timeout = 300
    # Timeout for attempting to connect to the peer per aiohttp:
    # https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientTimeout.sock_connect
    settings.watching.connect_timeout = 30
    # Wait for that many seconds between watching events
    settings.watching.reconnect_backoff = 1


@kopf.on.login()
async def login(**kwargs):
    return await login_via_kubernetes_asyncio(**kwargs)


@kopf.on.create(API_GROUP, "v1", RESOURCE_CRATEDB)
async def cluster_create(
    namespace: str, meta: kopf.Meta, spec: kopf.Spec, logger: logging.Logger, **kwargs
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
    cluster_name = spec["cluster"]["name"]

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


@kopf.on.update(API_GROUP, "v1", RESOURCE_CRATEDB)
async def cluster_update(
    namespace: str,
    name: str,
    patch: kopf.Patch,
    status: kopf.Status,
    diff: kopf.Diff,
    logger,
    **kwargs,
):
    context = Context.deserialize(status.get("updateContext"))
    new_cycle = False
    if context.state_machine.done:
        new_cycle = True

    do_upgrade = False
    do_restart = False
    do_scale = False
    for _, field_path, *_ in diff:
        if field_path in {
            ("spec", "cluster", "imageRegistry"),
            ("spec", "cluster", "version"),
        }:
            do_upgrade = True
            do_restart = True
        elif field_path == ("spec", "nodes", "master", "replicas"):
            do_scale = True
        elif field_path == ("spec", "nodes", "data"):
            do_scale = True

    if do_upgrade:
        if new_cycle:
            context.state_machine.add(State.UPGRADE)
        kopf.register(fn=UpgradeSubHandler(namespace, name, context)(), id="upgrade")

    if do_restart:
        if new_cycle:
            context.state_machine.add(State.RESTART)
        kopf.register(
            fn=RestartSubHandler(namespace, name, context)(),
            id="restart",
            timeout=config.ROLLING_RESTART_TIMEOUT,
        )

    if do_scale:
        if new_cycle:
            context.state_machine.add(State.SCALE)
        kopf.register(
            fn=ScaleSubHandler(namespace, name, context)(),
            id="scale",
            timeout=config.SCALING_TIMEOUT,
        )

    patch.status["updateContext"] = context.serialize()


@kopf.on.resume(API_GROUP, "v1", RESOURCE_CRATEDB)
async def update_cratedb_resource(
    namespace: str,
    name: str,
    spec: kopf.Spec,
    **kwargs,
):
    if "users" in spec:
        async with ApiClient() as api_client:
            for user_spec in spec["users"]:
                core = CoreV1Api(api_client)

                secret_name = user_spec["password"]["secretKeyRef"]["name"]
                secret = await core.read_namespaced_secret(
                    namespace=namespace, name=secret_name
                )
                if (
                    secret.metadata.labels is None
                    or LABEL_USER_PASSWORD not in secret.metadata.labels
                ):
                    await ensure_user_password_label(
                        core, namespace, user_spec["password"]["secretKeyRef"]["name"]
                    )


@kopf.on.update("", "v1", "secrets", labels={LABEL_USER_PASSWORD: "true"})
async def secret_update(
    namespace: str,
    name: str,
    diff: kopf.Diff,
    logger: logging.Logger,
    **kwargs,
):
    async with ApiClient() as api_client:
        coapi = CustomObjectsApi(api_client)
        core = CoreV1Api(api_client)

        for operation, field_path, old_value, new_value in diff:
            custom_objects = await coapi.list_namespaced_custom_object(
                namespace=namespace,
                group=API_GROUP,
                version="v1",
                plural=RESOURCE_CRATEDB,
            )

            for crate_custom_object in custom_objects["items"]:
                host = await get_host(
                    core, namespace, crate_custom_object["metadata"]["name"]
                )

                for user_spec in crate_custom_object["spec"]["users"]:
                    expected_field_path = (
                        "data",
                        user_spec["password"]["secretKeyRef"]["key"],
                    )
                    if (
                        user_spec["password"]["secretKeyRef"]["name"] == name
                        and field_path == expected_field_path
                    ):
                        kopf.register(
                            fn=subhandler_partial(
                                update_user_password,
                                host,
                                user_spec["name"],
                                old_value,
                                new_value,
                                logger,
                            ),
                            id=f"update-{crate_custom_object['metadata']['name']}-{user_spec['name']}",  # noqa
                            timeout=config.BOOTSTRAP_TIMEOUT,
                        )
