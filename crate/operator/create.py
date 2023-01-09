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

import logging
import math
import pkgutil
import warnings
from typing import Any, Dict, List, Optional

import bitmath
import yaml
from kubernetes_asyncio.client import (
    AppsV1Api,
    CoreV1Api,
    PolicyV1beta1Api,
    V1Affinity,
    V1beta1PodDisruptionBudget,
    V1beta1PodDisruptionBudgetSpec,
    V1ConfigMap,
    V1ConfigMapVolumeSource,
    V1Container,
    V1ContainerPort,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1EnvVarSource,
    V1HTTPGetAction,
    V1KeyToPath,
    V1LabelSelector,
    V1LabelSelectorRequirement,
    V1LocalObjectReference,
    V1NodeAffinity,
    V1NodeSelector,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
    V1ObjectMeta,
    V1OwnerReference,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PodAffinityTerm,
    V1PodAntiAffinity,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Probe,
    V1ResourceRequirements,
    V1Secret,
    V1SecretKeySelector,
    V1SecretVolumeSource,
    V1SecurityContext,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
    V1StatefulSet,
    V1StatefulSetSpec,
    V1StatefulSetUpdateStrategy,
    V1Toleration,
    V1TopologySpreadConstraint,
    V1Volume,
    V1VolumeMount,
)
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.config import config
from crate.operator.constants import (
    DATA_PVC_NAME_PREFIX,
    LABEL_COMPONENT,
    LABEL_NAME,
    LABEL_NODE_NAME,
    SHARED_NODE_SELECTOR_KEY,
    SHARED_NODE_SELECTOR_VALUE,
    SHARED_NODE_TOLERATION_EFFECT,
    SHARED_NODE_TOLERATION_KEY,
    SHARED_NODE_TOLERATION_VALUE,
    CloudProvider,
    Port,
)
from crate.operator.utils import crate, quorum
from crate.operator.utils.formatting import b64encode, format_bitmath
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.kubeapi import call_kubeapi
from crate.operator.utils.secrets import gen_password
from crate.operator.utils.typing import LabelType
from crate.operator.utils.version import CrateVersion


def get_sql_exporter_config(
    owner_references: Optional[List[V1OwnerReference]], name: str, labels: LabelType
) -> V1ConfigMap:

    sql_exporter_config = pkgutil.get_data("crate.operator", "data/sql-exporter.yaml")

    if sql_exporter_config:
        # Parse the config yaml file to get the defined collectors and load them
        parsed_sql_exporter_config = yaml.load(
            sql_exporter_config.decode(), Loader=yaml.FullLoader
        )
        collectors = parsed_sql_exporter_config["target"]["collectors"]

        result = V1ConfigMap(
            metadata=V1ObjectMeta(
                name=f"crate-sql-exporter-{name}",
                labels=labels,
                owner_references=owner_references,
            ),
            data={
                "sql-exporter.yaml": sql_exporter_config.decode(),
            },
        )

        # Add the yaml collectors to the configmap dynamically
        for collector in collectors:
            # Remove the `_collector` suffix from the collector name if present
            if collector.endswith("_collector"):
                collector = collector[:-10]
            yaml_filename = (
                f"{collector}-collector.yaml"  # Notice the `-` instead of `_`!
            )
            collector_config = pkgutil.get_data(
                "crate.operator", f"data/{yaml_filename}"
            )

            if collector_config is None:
                raise FileNotFoundError(
                    f"Could not load config for collector {collector}"
                )
            result.data[yaml_filename] = collector_config.decode()

        return result
    else:
        warnings.warn(
            "Cannot load or missing SQL Exporter or Responsivity Collector config!"
        )


async def create_sql_exporter_config(
    owner_references: Optional[List[V1OwnerReference]],
    namespace: str,
    name: str,
    labels: LabelType,
    logger: logging.Logger,
) -> None:
    async with ApiClient() as api_client:
        core = CoreV1Api(api_client)
        await call_kubeapi(
            core.create_namespaced_config_map,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_sql_exporter_config(owner_references, name, labels),
        )


def get_statefulset_affinity(
    name: str, logger: logging.Logger, node_spec: Dict[str, Any]
) -> Optional[V1Affinity]:
    if config.TESTING:
        logger.warning("Deploying cluster %s without any pod anti-affinity!", name)
        return None

    if is_shared_resources_cluster(node_spec):
        return V1Affinity(
            pod_anti_affinity={"$patch": "delete"},
            node_affinity=V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=V1NodeSelector(
                    node_selector_terms=[
                        V1NodeSelectorTerm(
                            match_expressions=[
                                V1NodeSelectorRequirement(
                                    key=SHARED_NODE_SELECTOR_KEY,
                                    operator="In",
                                    values=[SHARED_NODE_SELECTOR_VALUE],
                                )
                            ]
                        )
                    ]
                )
            ),
        )
    else:
        return V1Affinity(
            node_affinity={"$patch": "delete"},
            pod_anti_affinity=V1PodAntiAffinity(
                required_during_scheduling_ignored_during_execution=[
                    V1PodAffinityTerm(
                        label_selector=V1LabelSelector(
                            match_expressions=[
                                V1LabelSelectorRequirement(
                                    key=LABEL_COMPONENT,
                                    operator="In",
                                    values=["cratedb"],
                                ),
                                V1LabelSelectorRequirement(
                                    key=LABEL_NAME, operator="In", values=[name]
                                ),
                            ],
                        ),
                        topology_key="kubernetes.io/hostname",
                    ),
                ],
            ),
        )


def get_tolerations(
    name: str, logger: logging.Logger, node_spec: Dict[str, Any]
) -> Optional[List[V1Toleration]]:
    if config.TESTING:
        logger.warning("Deploying cluster %s without any tolerations!", name)
        return None

    if is_shared_resources_cluster(node_spec):
        return [
            V1Toleration(
                effect=SHARED_NODE_TOLERATION_EFFECT,
                key=SHARED_NODE_TOLERATION_KEY,
                operator="Equal",
                value=SHARED_NODE_TOLERATION_VALUE,
            )
        ]

    # Since we cannot easily remove tolerations (or haven't figured out how to),
    # we are instead configuring a "tolerate any" toleration which is effectively
    # a no-op.
    return [
        V1Toleration(
            effect=SHARED_NODE_TOLERATION_EFFECT,
            key=SHARED_NODE_TOLERATION_KEY,
            operator="Equal",
            value="any",
        )
    ]


def get_topology_spread(
    name: str, logger: logging.Logger
) -> Optional[List[V1TopologySpreadConstraint]]:
    if config.TESTING:
        logger.warning("Deploying cluster %s without any pod topology spread!", name)
        return None

    topology_spread = None
    if config.CLOUD_PROVIDER in {CloudProvider.AWS, CloudProvider.AZURE}:
        topology_spread = [
            V1TopologySpreadConstraint(
                max_skew=1,
                topology_key="topology.kubernetes.io/zone",
                when_unsatisfiable="DoNotSchedule",
                label_selector=V1LabelSelector(
                    match_expressions=[
                        V1LabelSelectorRequirement(
                            key=LABEL_COMPONENT, operator="In", values=["cratedb"]
                        ),
                        V1LabelSelectorRequirement(
                            key=LABEL_NAME, operator="In", values=[name]
                        ),
                    ],
                ),
            )
        ]
    return topology_spread


def get_statefulset_containers(
    node_spec: Dict[str, Any],
    http_port: int,
    jmx_port: int,
    postgres_port: int,
    prometheus_port: int,
    transport_port: int,
    crate_image: str,
    crate_command: List[str],
    crate_env: List[V1EnvVar],
    crate_volume_mounts: List[V1VolumeMount],
) -> List[V1Container]:
    sql_exporter_image = config.SQL_EXPORTER_IMAGE
    return [
        V1Container(
            command=[
                "/bin/sql_exporter",
                "-config.file=/config/sql-exporter.yaml",
                "-web.listen-address=:9399",
                "-web.metrics-path=/metrics",
            ],
            image=sql_exporter_image,
            name="sql-exporter",
            ports=[V1ContainerPort(container_port=9399, name="sql-exporter")],
            volume_mounts=[
                V1VolumeMount(
                    mount_path="/config", name="crate-sql-exporter", read_only=True
                ),
            ],
        ),
        V1Container(
            command=crate_command,
            env=crate_env,
            image=crate_image,
            name="crate",
            ports=[
                V1ContainerPort(container_port=http_port, name="http"),
                V1ContainerPort(container_port=jmx_port, name="jmx"),
                V1ContainerPort(container_port=postgres_port, name="postgres"),
                V1ContainerPort(container_port=prometheus_port, name="prometheus"),
                V1ContainerPort(container_port=transport_port, name="transport"),
            ],
            readiness_probe=V1Probe(
                http_get=V1HTTPGetAction(path="/ready", port=prometheus_port),
                initial_delay_seconds=10 if config.TESTING else 30,
                period_seconds=5 if config.TESTING else 10,
            ),
            resources=V1ResourceRequirements(
                limits={
                    "cpu": str(
                        get_cluster_resource_limits(
                            node_spec, resource_type="cpu", fallback_key="cpus"
                        )
                    ),
                    "memory": format_bitmath(
                        bitmath.parse_string_unsafe(
                            get_cluster_resource_limits(
                                node_spec, resource_type="memory"
                            )
                        )
                    ),
                },
                requests={
                    "cpu": str(
                        get_cluster_resource_requests(
                            node_spec, resource_type="cpu", fallback_key="cpus"
                        )
                    ),
                    "memory": format_bitmath(
                        bitmath.parse_string_unsafe(
                            get_cluster_resource_requests(
                                node_spec, resource_type="memory"
                            )
                        )
                    ),
                },
            ),
            volume_mounts=crate_volume_mounts,
        ),
    ]


def get_statefulset_crate_command(
    *,
    namespace: str,
    name: str,
    master_nodes: List[str],
    total_nodes_count: int,
    data_nodes_count: int,
    crate_node_name_prefix: str,
    cluster_name: str,
    node_name: str,
    node_spec: Dict[str, Any],
    cluster_settings: Optional[Dict[str, str]],
    has_ssl: bool,
    is_master: bool,
    is_data: bool,
    crate_version: str,
) -> List[str]:

    expected_nodes_setting_name = "gateway.expected_nodes"
    recover_after_nodes_setting_name = "gateway.recover_after_nodes"
    expected_nodes_setting_value = total_nodes_count
    if CrateVersion(crate_version) >= CrateVersion(
        config.GATEWAY_SETTINGS_DATA_NODES_VERSION
    ):
        expected_nodes_setting_name = "gateway.expected_data_nodes"
        recover_after_nodes_setting_name = "gateway.recover_after_data_nodes"
        expected_nodes_setting_value = data_nodes_count

    settings = {
        "-Cstats.enabled": "true",
        "-Ccluster.name": cluster_name,
        # This is a clever way of doing string split in SH and picking the last
        # item. Here's how it works:
        #
        # `hostname` is e.g. `crate-data-hot-11111111-1111-1111-1111-111111111111-12`
        # for a StatefulSet that has at least 13 replicas (hence the 12 a the
        # end). What we want now is to get the `12` from the end. In Bash, one
        # would do `${$(hostname)##*-}` to do a greedy prefix removal. However,
        # such string manipulations don't exist in SH.
        # We can, however, make use of the `cut` command that allows splitting
        # a string at an arbitrary delimiter and allows picking a field.
        # However, fields can only be picked from the beginning; there's no
        # negative indexing to get the last field.
        # Now, by reversing the hostname, then taking the first field, we get
        # `21`. We can again reverse that to get what we want.
        #
        # https://stackoverflow.com/a/9125818
        "-Cnode.name": f"{crate_node_name_prefix}$(hostname | rev | cut -d- -f1 | rev)",
        "-Ccluster.initial_master_nodes": ",".join(master_nodes),
        "-Cdiscovery.seed_providers": "srv",
        "-Cdiscovery.srv.query": f"_cluster._tcp.crate-discovery-{name}.{namespace}.svc.cluster.local",  # noqa
        f"-C{recover_after_nodes_setting_name}": str(
            quorum(expected_nodes_setting_value)
        ),
        f"-C{expected_nodes_setting_name}": str(expected_nodes_setting_value),
        "-Cauth.host_based.enabled": "true",
        "-Cauth.host_based.config.0.user": "crate",
        "-Cauth.host_based.config.0.address": "_local_",
        "-Cauth.host_based.config.0.method": "trust",
        "-Cauth.host_based.config.99.method": "password",
        "-Cpath.data": ",".join(
            f"/data/data{i}" for i in range(node_spec["resources"]["disk"]["count"])
        ),
        "-Cprocessors": str(
            math.ceil(
                get_cluster_resource_limits(
                    node_spec, resource_type="cpu", fallback_key="cpus"
                )
            )
        ),
        "-Cnode.master": "true" if is_master else "false",
        "-Cnode.data": "true" if is_data else "false",
        "-Cnode.attr.node_name": node_name,
    }

    if has_ssl:
        settings.update(
            {
                "-Cssl.http.enabled": "true",
                "-Cssl.psql.enabled": "true",
                "-Cssl.keystore_filepath": "/var/lib/crate/ssl/keystore.jks",
                "-Cssl.keystore_password": "${KEYSTORE_PASSWORD}",
                "-Cssl.keystore_key_password": "${KEYSTORE_KEY_PASSWORD}",
                "-Cauth.host_based.config.99.ssl": "on",
            }
        )

    if config.CLOUD_PROVIDER == CloudProvider.AWS:
        url = "http://169.254.169.254/latest/meta-data/placement/availability-zone"
        settings["-Cnode.attr.zone"] = f"$(curl -s '{url}')"
    elif config.CLOUD_PROVIDER == CloudProvider.AZURE:
        url = "http://169.254.169.254/metadata/instance/compute/zone?api-version=2020-06-01&format=text"  # noqa
        settings["-Cnode.attr.zone"] = f"$(curl -s '{url}' -H 'Metadata: true')"

    if cluster_settings:
        for k, v in cluster_settings.items():
            settings[f"-C{k}"] = v

    node_settings = node_spec.get("settings", {})
    for k, v in node_settings.items():
        settings[f"-C{k}"] = v

    return ["/docker-entrypoint.sh", "crate"] + [
        f"{k}={v}" for k, v in settings.items()
    ]


def get_statefulset_crate_env_java_opts(
    jmx_port: int, prometheus_port: int
) -> List[str]:
    return [
        f"-Dcom.sun.management.jmxremote.port={jmx_port}",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.local.only=false",
        f"-Dcom.sun.management.jmxremote.rmi.port={jmx_port}",
        "-Djava.rmi.server.hostname=127.0.0.1",
        f"-javaagent:/var/lib/crate/crate-jmx-exporter-{config.JMX_EXPORTER_VERSION}.jar={prometheus_port}",  # noqa
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:HeapDumpPath=/resource/heapdump",
        "-Dlog4j2.formatMsgNoLookups=true",
    ]


def get_statefulset_env_crate_heap(memory: str, heap_ratio: float) -> V1EnvVar:
    """Generates the environment variable with the explicit heap size in bytes"""
    return V1EnvVar(
        name="CRATE_HEAP_SIZE",
        value=str(int(bitmath.parse_string_unsafe(memory).bytes * heap_ratio)),
    )


def get_statefulset_crate_env(
    node_spec: Dict[str, Any],
    jmx_port: int,
    prometheus_port: int,
    ssl: Optional[Dict[str, Any]],
) -> List[V1EnvVar]:
    crate_env = [
        get_statefulset_env_crate_heap(
            memory=get_cluster_resource_limits(node_spec, resource_type="memory"),
            heap_ratio=node_spec["resources"]["heapRatio"],
        ),
        V1EnvVar(
            name="CRATE_JAVA_OPTS",
            value=" ".join(
                get_statefulset_crate_env_java_opts(jmx_port, prometheus_port)
            ),
        ),
    ]

    if ssl is not None:
        crate_env.extend(
            [
                V1EnvVar(
                    name="KEYSTORE_KEY_PASSWORD",
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            key=ssl["keystoreKeyPassword"]["secretKeyRef"]["key"],
                            name=ssl["keystoreKeyPassword"]["secretKeyRef"]["name"],
                        )
                    ),
                ),
                V1EnvVar(
                    name="KEYSTORE_PASSWORD",
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            key=ssl["keystorePassword"]["secretKeyRef"]["key"],
                            name=ssl["keystorePassword"]["secretKeyRef"]["name"],
                        )
                    ),
                ),
            ]
        )

    return crate_env


def get_statefulset_crate_volume_mounts(
    node_spec: Dict[str, Any], ssl: Optional[Dict[str, Any]]
) -> List[V1VolumeMount]:
    volume_mounts = [
        V1VolumeMount(
            mount_path=f"/var/lib/crate/crate-jmx-exporter-{config.JMX_EXPORTER_VERSION}.jar",  # noqa
            name="jmxdir",
            sub_path=f"crate-jmx-exporter-{config.JMX_EXPORTER_VERSION}.jar",
        ),
        V1VolumeMount(mount_path="/resource", name="debug"),
    ]
    volume_mounts.extend(
        [
            V1VolumeMount(
                mount_path=f"/data/data{i}", name=f"{DATA_PVC_NAME_PREFIX}{i}"
            )
            for i in range(node_spec["resources"]["disk"]["count"])
        ]
    )

    if ssl is not None:
        # We want CrateDB to be able to reload the keystore when it changes. We
        # therefore need to mount the entire volume and not some subpath which
        # would result in numerous symlinks that mitigate CrateDB's detection
        # for changed keystores.
        volume_mounts.append(
            V1VolumeMount(
                mount_path="/var/lib/crate/ssl/", name="keystore", read_only=True
            ),
        )

    return volume_mounts


def get_statefulset_init_containers(crate_image: str) -> List[V1Container]:
    return [
        V1Container(
            # We need to do this in an init container because of the required
            # security context. We don't want to run CrateDB with that context,
            # thus doing it before.
            command=["sysctl", "-w", "vm.max_map_count=262144"],
            image="busybox:1.35.0",
            image_pull_policy="IfNotPresent",
            name="init-sysctl",
            security_context=V1SecurityContext(privileged=True),
        ),
        V1Container(
            command=[
                "wget",
                "-O",
                f"/jmxdir/crate-jmx-exporter-{config.JMX_EXPORTER_VERSION}.jar",
                f"https://repo1.maven.org/maven2/io/crate/crate-jmx-exporter/{config.JMX_EXPORTER_VERSION}/crate-jmx-exporter-{config.JMX_EXPORTER_VERSION}.jar",  # noqa
            ],
            image="busybox:1.35.0",
            image_pull_policy="IfNotPresent",
            name="fetch-jmx-exporter",
            volume_mounts=[V1VolumeMount(name="jmxdir", mount_path="/jmxdir")],
        ),
        V1Container(
            command=[
                "sh",
                "-c",
                "mkdir -pv /resource/heapdump ; chown -R crate:crate /resource",
            ],
            image=crate_image,
            image_pull_policy="IfNotPresent",
            name="mkdir-heapdump",
            volume_mounts=[V1VolumeMount(name="debug", mount_path="/resource")],
        ),
    ]


def get_statefulset_pvc(
    owner_references: Optional[List[V1OwnerReference]], node_spec: Dict[str, Any]
) -> List[V1PersistentVolumeClaim]:
    size = format_bitmath(
        bitmath.parse_string_unsafe(node_spec["resources"]["disk"]["size"])
    )
    storage_class_name = node_spec["resources"]["disk"]["storageClass"]

    pvcs = [
        V1PersistentVolumeClaim(
            metadata=V1ObjectMeta(
                name=f"{DATA_PVC_NAME_PREFIX}{i}", owner_references=owner_references
            ),
            spec=V1PersistentVolumeClaimSpec(
                access_modes=["ReadWriteOnce"],
                resources=V1ResourceRequirements(requests={"storage": size}),
                storage_class_name=storage_class_name,
            ),
        )
        for i in range(node_spec["resources"]["disk"]["count"])
    ]

    pvcs.append(
        V1PersistentVolumeClaim(
            metadata=V1ObjectMeta(name="debug", owner_references=owner_references),
            spec=V1PersistentVolumeClaimSpec(
                access_modes=["ReadWriteOnce"],
                resources=V1ResourceRequirements(
                    requests={"storage": format_bitmath(config.DEBUG_VOLUME_SIZE)}
                ),
                storage_class_name=config.DEBUG_VOLUME_STORAGE_CLASS,
            ),
        )
    )

    return pvcs


def get_statefulset_volumes(name: str, ssl: Optional[Dict[str, Any]]) -> List[V1Volume]:
    volumes = [
        V1Volume(
            config_map=V1ConfigMapVolumeSource(name=f"crate-sql-exporter-{name}"),
            name="crate-sql-exporter",
        ),
        V1Volume(name="jmxdir", empty_dir=V1EmptyDirVolumeSource()),
    ]

    if ssl is not None:
        volumes.append(
            V1Volume(
                name="keystore",
                secret=V1SecretVolumeSource(
                    secret_name=ssl["keystore"]["secretKeyRef"]["name"],
                    items=[
                        V1KeyToPath(
                            key=ssl["keystore"]["secretKeyRef"]["key"],
                            path="keystore.jks",
                        )
                    ],
                ),
            ),
        )

    return volumes


def get_statefulset(
    owner_references: Optional[List[V1OwnerReference]],
    namespace: str,
    name: str,
    labels: LabelType,
    treat_as_master: bool,
    treat_as_data: bool,
    cluster_name: str,
    node_name: str,
    node_name_prefix: str,
    node_spec: Dict[str, Any],
    master_nodes: List[str],
    total_nodes_count: int,
    data_nodes_count: int,
    http_port: int,
    jmx_port: int,
    postgres_port: int,
    prometheus_port: int,
    transport_port: int,
    crate_image: str,
    ssl: Optional[Dict[str, Any]],
    cluster_settings: Optional[Dict[str, str]],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    logger: logging.Logger,
) -> V1StatefulSet:
    node_annotations = node_spec.get("annotations", {})
    node_annotations.update(
        {"prometheus.io/port": str(prometheus_port), "prometheus.io/scrape": "true"}
    )
    node_labels = labels.copy()
    node_labels.update(node_spec.get("labels", {}))
    # This is to identify pods of the same cluster but with a different node type
    node_labels[LABEL_NODE_NAME] = node_name
    full_pod_name_prefix = f"crate-{node_name_prefix}{name}"
    image_registry, version = crate_image.rsplit(":", 1)

    containers = get_statefulset_containers(
        node_spec,
        http_port,
        jmx_port,
        postgres_port,
        prometheus_port,
        transport_port,
        crate_image,
        get_statefulset_crate_command(
            namespace=namespace,
            name=name,
            master_nodes=master_nodes,
            total_nodes_count=total_nodes_count,
            data_nodes_count=data_nodes_count,
            crate_node_name_prefix=node_name_prefix,
            cluster_name=cluster_name,
            node_name=node_name,
            node_spec=node_spec,
            cluster_settings=cluster_settings,
            has_ssl=bool(ssl),
            is_master=treat_as_master,
            is_data=treat_as_data,
            crate_version=version,
        ),
        get_statefulset_crate_env(node_spec, jmx_port, prometheus_port, ssl),
        get_statefulset_crate_volume_mounts(node_spec, ssl),
    )

    return V1StatefulSet(
        metadata=V1ObjectMeta(
            annotations=node_spec.get("annotations"),
            labels=node_labels,
            name=full_pod_name_prefix,
            owner_references=owner_references,
        ),
        spec=V1StatefulSetSpec(
            pod_management_policy="Parallel",
            replicas=node_spec["replicas"],
            selector=V1LabelSelector(
                match_labels={
                    LABEL_COMPONENT: "cratedb",
                    LABEL_NAME: name,
                    LABEL_NODE_NAME: node_name,
                }
            ),
            service_name="cratedb",
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(
                    annotations=node_annotations,
                    labels=node_labels,
                ),
                spec=V1PodSpec(
                    affinity=get_statefulset_affinity(name, logger, node_spec),
                    topology_spread_constraints=get_topology_spread(name, logger),
                    containers=containers,
                    image_pull_secrets=image_pull_secrets,
                    init_containers=get_statefulset_init_containers(crate_image),
                    volumes=get_statefulset_volumes(name, ssl),
                    tolerations=get_tolerations(name, logger, node_spec),
                ),
            ),
            update_strategy=V1StatefulSetUpdateStrategy(type="OnDelete"),
            volume_claim_templates=get_statefulset_pvc(owner_references, node_spec),
        ),
    )


async def create_statefulset(
    owner_references: Optional[List[V1OwnerReference]],
    namespace: str,
    name: str,
    labels: LabelType,
    treat_as_master: bool,
    treat_as_data: bool,
    cluster_name: str,
    node_name: str,
    node_name_prefix: str,
    node_spec: Dict[str, Any],
    master_nodes: List[str],
    total_nodes_count: int,
    data_nodes_count: int,
    http_port: int,
    jmx_port: int,
    postgres_port: int,
    prometheus_port: int,
    transport_port: int,
    crate_image: str,
    ssl: Optional[Dict[str, Any]],
    cluster_settings: Optional[Dict[str, str]],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    logger: logging.Logger,
) -> None:
    async with ApiClient() as api_client:
        apps = AppsV1Api(api_client)
        await call_kubeapi(
            apps.create_namespaced_stateful_set,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_statefulset(
                owner_references,
                namespace,
                name,
                labels,
                treat_as_master,
                treat_as_data,
                cluster_name,
                node_name,
                node_name_prefix,
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
                ssl,
                cluster_settings,
                image_pull_secrets,
                logger,
            ),
        )
        policy = PolicyV1beta1Api(api_client)
        pdb = V1beta1PodDisruptionBudget(
            metadata=V1ObjectMeta(
                name=f"crate-{name}",
                owner_references=owner_references,
            ),
            spec=V1beta1PodDisruptionBudgetSpec(
                max_unavailable=1,
                selector=V1LabelSelector(
                    match_labels={
                        LABEL_COMPONENT: "cratedb",
                        LABEL_NAME: name,
                        LABEL_NODE_NAME: node_name,
                    }
                ),
            ),
        )
        """
           A Pod Distruption Budget ensures that when performing Kubernetes cluster
           maintenance (i.e. upgrades), we make sure to not disrupt more than
           1 pod in a StatefulSet at a time.
        """
        await call_kubeapi(
            policy.create_namespaced_pod_disruption_budget,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=pdb,
        )


def get_data_service(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    http_port: int,
    postgres_port: int,
    dns_record: Optional[str],
    source_ranges: Optional[List[str]] = None,
    prefix: str = None,
) -> V1Service:
    annotations = {}
    if config.CLOUD_PROVIDER == CloudProvider.AWS:
        annotations.update(
            {
                # https://kubernetes.io/docs/concepts/services-networking/service/#connection-draining-on-aws
                "service.beta.kubernetes.io/aws-load-balancer-connection-draining-enabled": "true",  # noqa
                "service.beta.kubernetes.io/aws-load-balancer-connection-draining-timeout": "1800",  # noqa
                # Default idle timeout is 60s, which kills the connection on long-running queries # noqa
                "service.beta.kubernetes.io/aws-load-balancer-connection-idle-timeout": "3600",  # noqa
                "service.beta.kubernetes.io/aws-load-balancer-type": "nlb",  # noqa
            }
        )
    elif config.CLOUD_PROVIDER == CloudProvider.AZURE:
        # https://docs.microsoft.com/en-us/azure/aks/load-balancer-standard#additional-customizations-via-kubernetes-annotations
        # https://docs.microsoft.com/en-us/azure/load-balancer/load-balancer-tcp-reset
        annotations.update(
            {
                "service.beta.kubernetes.io/azure-load-balancer-disable-tcp-reset": "false",  # noqa
                "service.beta.kubernetes.io/azure-load-balancer-tcp-idle-timeout": "30",  # noqa
            }
        )

    if dns_record:
        annotations.update({"external-dns.alpha.kubernetes.io/hostname": dns_record})

    service_name = f"crate-{prefix}-{name}" if prefix else f"crate-{name}"

    return V1Service(
        metadata=V1ObjectMeta(
            annotations=annotations,
            labels=labels,
            name=service_name,
            owner_references=owner_references,
        ),
        spec=V1ServiceSpec(
            ports=[
                V1ServicePort(name="http", port=http_port, target_port=Port.HTTP.value),
                V1ServicePort(
                    name="psql", port=postgres_port, target_port=Port.POSTGRES.value
                ),
            ],
            selector={LABEL_COMPONENT: "cratedb", LABEL_NAME: name},
            type="LoadBalancer",
            external_traffic_policy="Local",
            load_balancer_source_ranges=source_ranges if source_ranges else None,
        ),
    )


def get_discovery_service(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    transport_port: int,
    http_port: int,
    postgres_port: int,
) -> V1Service:
    return V1Service(
        metadata=V1ObjectMeta(
            name=f"crate-discovery-{name}",
            labels=labels,
            owner_references=owner_references,
        ),
        spec=V1ServiceSpec(
            # Headless service
            cluster_ip="None",
            ports=[
                V1ServicePort(name="cluster", port=transport_port),
                V1ServicePort(name="http", port=http_port, target_port=Port.HTTP.value),
                V1ServicePort(
                    name="psql", port=postgres_port, target_port=Port.POSTGRES.value
                ),
            ],
            selector={LABEL_COMPONENT: "cratedb", LABEL_NAME: name},
        ),
    )


async def create_services(
    owner_references: Optional[List[V1OwnerReference]],
    namespace: str,
    name: str,
    labels: LabelType,
    http_port: int,
    postgres_port: int,
    transport_port: int,
    dns_record: Optional[str],
    logger: logging.Logger,
    source_ranges: Optional[List[str]] = None,
) -> None:
    async with ApiClient() as api_client:
        core = CoreV1Api(api_client)
        await call_kubeapi(
            core.create_namespaced_service,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_data_service(
                owner_references,
                name,
                labels,
                http_port,
                postgres_port,
                dns_record,
                source_ranges,
            ),
        )
        await call_kubeapi(
            core.create_namespaced_service,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_discovery_service(
                owner_references, name, labels, transport_port, http_port, postgres_port
            ),
        )


def get_system_user_secret(
    owner_references: Optional[List[V1OwnerReference]], name: str, labels: LabelType
) -> V1Secret:
    return V1Secret(
        data={"password": b64encode(gen_password(50))},
        metadata=V1ObjectMeta(
            name=f"user-system-{name}",
            labels=labels,
            owner_references=owner_references,
        ),
        type="Opaque",
    )


async def create_system_user(
    owner_references: Optional[List[V1OwnerReference]],
    namespace: str,
    name: str,
    labels: LabelType,
    logger: logging.Logger,
) -> None:
    """
    The *CrateDB Operator* will need to perform operations on the CrateDB
    cluster. For that, it will use a ``system`` user who's credentials are
    created here.
    """
    async with ApiClient() as api_client:
        core = CoreV1Api(api_client)
        await call_kubeapi(
            core.create_namespaced_secret,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_system_user_secret(owner_references, name, labels),
        )


def is_shared_resources_cluster(node_spec: Dict[str, Any]) -> bool:
    try:
        cpu_request = node_spec["resources"].get("requests", {}).get("cpu")
        cpu_limit = node_spec["resources"].get("limits", {}).get("cpu")
        memory_request = node_spec["resources"].get("requests", {}).get("memory")
        memory_limit = node_spec["resources"].get("limits", {}).get("memory")
        if not (cpu_request or memory_request):
            return False
        return cpu_request != cpu_limit or memory_request != memory_limit
    except KeyError:
        return False


def get_cluster_resource_requests(
    node_spec: Dict[str, Any], *, resource_type: str, fallback_key: Optional[str] = None
):
    fallback_key = fallback_key or resource_type
    return (
        node_spec["resources"]
        .get("requests", {})
        .get(
            resource_type,
            get_cluster_resource_limits(
                node_spec, resource_type=resource_type, fallback_key=fallback_key
            ),
        )
    )


def get_cluster_resource_limits(
    node_spec: Dict[str, Any], *, resource_type: str, fallback_key: Optional[str] = None
):
    fallback_key = fallback_key or resource_type
    return (
        node_spec["resources"]
        .get("limits", {})
        .get(resource_type, node_spec["resources"].get(fallback_key))
    )


class CreateSqlExporterConfigSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_create_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        owner_references: Optional[List[V1OwnerReference]],
        cratedb_labels: LabelType,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        await create_sql_exporter_config(
            owner_references, namespace, name, cratedb_labels, logger
        )


class CreateSystemUserSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_create_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        owner_references: Optional[List[V1OwnerReference]],
        cratedb_labels: LabelType,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        await create_system_user(
            owner_references, namespace, name, cratedb_labels, logger
        )


class CreateServicesSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_create_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        owner_references: Optional[List[V1OwnerReference]],
        cratedb_labels: LabelType,
        http_port: int,
        postgres_port: int,
        transport_port: int,
        dns_record: Optional[str],
        logger: logging.Logger,
        source_ranges: Optional[List[str]] = None,
        **kwargs: Any,
    ):
        await create_services(
            owner_references,
            namespace,
            name,
            cratedb_labels,
            http_port,
            postgres_port,
            transport_port,
            dns_record,
            logger,
            source_ranges,
        )


class CreateStatefulsetSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_create_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        owner_references: Optional[List[V1OwnerReference]],
        cratedb_labels: LabelType,
        treat_as_master: bool,
        treat_as_data: bool,
        cluster_name: str,
        node_name: str,
        node_name_prefix: str,
        node_spec: Dict[str, Any],
        master_nodes: List[str],
        total_nodes_count: int,
        data_nodes_count: int,
        http_port: int,
        jmx_port: int,
        postgres_port: int,
        prometheus_port: int,
        transport_port: int,
        crate_image: str,
        ssl: Optional[Dict[str, Any]],
        cluster_settings: Optional[Dict[str, str]],
        image_pull_secrets: Optional[List[V1LocalObjectReference]],
        logger: logging.Logger,
        **kwargs: Any,
    ):

        await create_statefulset(
            owner_references,
            namespace,
            name,
            cratedb_labels,
            treat_as_master,
            treat_as_data,
            cluster_name,
            node_name,
            node_name_prefix,
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
            ssl,
            cluster_settings,
            image_pull_secrets,
            logger,
        )
