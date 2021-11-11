# CrateDB Kubernetes Operator
# Copyright (C) 2021 Crate.IO GmbH
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

import hashlib
import logging

import kopf
from kubernetes_asyncio.client import V1LocalObjectReference, V1OwnerReference

from crate.operator.backup import CreateBackupsSubHandler
from crate.operator.bootstrap import BootstrapClusterSubHandler
from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    CLUSTER_CREATE_ID,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_PART_OF,
    Port,
)
from crate.operator.create import (
    CreateServicesSubHandler,
    CreateSqlExporterConfigSubHandler,
    CreateStatefulsetSubHandler,
    CreateSystemUserSubHandler,
)
from crate.operator.operations import get_master_nodes_names, get_total_nodes_count


async def create_cratedb(
    namespace: str,
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
):
    context = status.get(CLUSTER_CREATE_ID)
    hash = hashlib.md5(str(spec).encode("utf-8")).hexdigest()
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
        fn=CreateSqlExporterConfigSubHandler(namespace, name, hash, context)(
            owner_references=owner_references, cratedb_labels=cratedb_labels
        ),
        id="sql_exporter_config",
    )

    kopf.register(
        fn=CreateSystemUserSubHandler(namespace, name, hash, context)(
            cratedb_labels=cratedb_labels, owner_references=owner_references
        ),
        id="system_user",
    )

    kopf.register(
        fn=CreateServicesSubHandler(namespace, name, hash, context)(
            owner_references=owner_references,
            cratedb_labels=cratedb_labels,
            http_port=http_port,
            postgres_port=postgres_port,
            transport_port=transport_port,
            dns_record=spec.get("cluster", {}).get("externalDNS"),
            source_ranges=source_ranges,
        ),
        id="services",
    )

    if has_master_nodes:
        kopf.register(
            fn=CreateStatefulsetSubHandler(namespace, name, hash, context)(
                owner_references=owner_references,
                cratedb_labels=cratedb_labels,
                treat_as_master=treat_as_master,
                treat_as_data=False,
                cluster_name=cluster_name,
                node_name="master",
                node_name_prefix="master-",
                node_spec=spec["nodes"]["master"],
                master_nodes=master_nodes,
                total_nodes_count=total_nodes_count,
                data_nodes_count=data_nodes_count,
                http_port=http_port,
                jmx_port=jmx_port,
                postgres_port=postgres_port,
                prometheus_port=prometheus_port,
                transport_port=transport_port,
                crate_image=crate_image,
                ssl=spec["cluster"].get("ssl"),
                cluster_settings=spec["cluster"].get("settings"),
                image_pull_secrets=image_pull_secrets,
            ),
            id="statefulset_master",
        )
        treat_as_master = False

    for node_spec in spec["nodes"]["data"]:
        node_name = node_spec["name"]
        kopf.register(
            fn=CreateStatefulsetSubHandler(namespace, name, hash, context)(
                owner_references=owner_references,
                cratedb_labels=cratedb_labels,
                treat_as_master=treat_as_master,
                treat_as_data=True,
                cluster_name=cluster_name,
                node_name=node_name,
                node_name_prefix=f"data-{node_name}-",
                node_spec=node_spec,
                master_nodes=master_nodes,
                total_nodes_count=total_nodes_count,
                data_nodes_count=data_nodes_count,
                http_port=http_port,
                jmx_port=jmx_port,
                postgres_port=postgres_port,
                prometheus_port=prometheus_port,
                transport_port=transport_port,
                crate_image=crate_image,
                ssl=spec["cluster"].get("ssl"),
                cluster_settings=spec["cluster"].get("settings"),
                image_pull_secrets=image_pull_secrets,
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
        fn=BootstrapClusterSubHandler(namespace, name, hash, context)(
            master_node_pod=master_node_pod,
            license=spec["cluster"].get("license"),
            has_ssl="ssl" in spec["cluster"],
            users=spec.get("users"),
        ),
        id="bootstrap",
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
                fn=CreateBackupsSubHandler(namespace, name, hash, context)(
                    owner_references=owner_references,
                    backup_metrics_labels=backup_metrics_labels,
                    http_port=http_port,
                    prometheus_port=prometheus_port,
                    backups=spec["backups"],
                    image_pull_secrets=image_pull_secrets,
                    has_ssl="ssl" in spec["cluster"],
                ),
                id="backup",
            )
    patch.status[CLUSTER_CREATE_ID] = context
