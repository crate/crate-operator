import logging
import math
import pkgutil
import textwrap
import warnings
from typing import Any, Awaitable, Dict, List, Optional, Tuple

import bitmath
import yaml
from kubernetes_asyncio.client import (
    AppsV1Api,
    CoreV1Api,
    V1Affinity,
    V1ConfigMap,
    V1ConfigMapVolumeSource,
    V1Container,
    V1ContainerPort,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1EnvVarSource,
    V1HostPathVolumeSource,
    V1HTTPGetAction,
    V1KeyToPath,
    V1LabelSelector,
    V1LabelSelectorRequirement,
    V1LocalObjectReference,
    V1ObjectMeta,
    V1PersistentVolume,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PersistentVolumeClaimVolumeSource,
    V1PersistentVolumeSpec,
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
    V1Volume,
    V1VolumeMount,
)

from crate.operator.config import config
from crate.operator.constants import (
    LABEL_COMPONENT,
    LABEL_NAME,
    LABEL_NODE_NAME,
    SYSTEM_USERNAME,
)
from crate.operator.utils import quorum
from crate.operator.utils.formatting import b64encode, format_bitmath
from crate.operator.utils.kubeapi import call_kubeapi
from crate.operator.utils.secrets import gen_password
from crate.operator.utils.typing import LabelType

logger = logging.getLogger(__name__)


def get_debug_persistent_volume(
    namespace: str, name: str, labels: LabelType,
) -> V1PersistentVolume:
    return V1PersistentVolume(
        metadata=V1ObjectMeta(name=f"temp-pv-{namespace}-{name}", labels=labels),
        spec=V1PersistentVolumeSpec(
            access_modes=["ReadWriteOnce"],
            capacity={"storage": format_bitmath(config.DEBUG_VOLUME_SIZE)},
            host_path=V1HostPathVolumeSource(path=f"/mnt/resource/{namespace}-{name}"),
            storage_class_name=config.DEBUG_VOLUME_STORAGE_CLASS,
        ),
    )


def get_debug_persistent_volume_claim(
    name: str, labels: LabelType
) -> V1PersistentVolumeClaim:
    return V1PersistentVolumeClaim(
        metadata=V1ObjectMeta(name=f"local-resource-{name}", labels=labels),
        spec=V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            resources=V1ResourceRequirements(
                requests={"storage": format_bitmath(config.DEBUG_VOLUME_SIZE)}
            ),
            storage_class_name=config.DEBUG_VOLUME_STORAGE_CLASS,
        ),
    )


def create_debug_volume(
    core: CoreV1Api, namespace: str, name: str, labels: LabelType,
) -> Tuple[Awaitable[V1PersistentVolume], Awaitable[V1PersistentVolumeClaim]]:
    """
    Creates a ``PersistentVolume`` and ``PersistentVolumeClaim`` to be used for
    exporting Java Heapdumps from CrateDB. The volume can be configured
    with the :attr:`~crate.operator.config.Config.DEBUG_VOLUME_SIZE` and
    :attr:`~crate.operator.config.Config.DEBUG_VOLUME_STORAGE_CLASS` settings.
    """
    return (
        call_kubeapi(
            core.create_persistent_volume,
            logger,
            continue_on_conflict=True,
            body=get_debug_persistent_volume(namespace, name, labels),
        ),
        call_kubeapi(
            core.create_namespaced_persistent_volume_claim,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_debug_persistent_volume_claim(name, labels),
        ),
    )


def get_sql_exporter_config(name: str, labels: LabelType) -> V1ConfigMap:
    sql_exporter_config = pkgutil.get_data("crate.operator", "data/sql-exporter.yaml")
    responsivity_collector_config = pkgutil.get_data(
        "crate.operator", "data/responsivity-collector.yaml",
    )
    if sql_exporter_config and responsivity_collector_config:
        return V1ConfigMap(
            metadata=V1ObjectMeta(name=f"crate-sql-exporter-{name}", labels=labels),
            data={
                "sql-exporter.yaml": sql_exporter_config.decode(),
                "responsivity-collector.yaml": responsivity_collector_config.decode(),
            },
        )
    else:
        warnings.warn(
            "Cannot load or missing SQL Exporter or Responsivity Collector config!"
        )


def create_sql_exporter_config(
    core: CoreV1Api, namespace: str, name: str, labels: LabelType,
) -> Awaitable[V1ConfigMap]:
    return call_kubeapi(
        core.create_namespaced_config_map,
        logger,
        continue_on_conflict=True,
        namespace=namespace,
        body=get_sql_exporter_config(name, labels),
    )


def get_statefulset_affinity(name: str) -> V1Affinity:
    return V1Affinity(
        pod_anti_affinity=V1PodAntiAffinity(
            required_during_scheduling_ignored_during_execution=[
                V1PodAffinityTerm(
                    label_selector=V1LabelSelector(
                        match_expressions=[
                            V1LabelSelectorRequirement(
                                key=LABEL_COMPONENT, operator="In", values=["cratedb"],
                            ),
                            V1LabelSelectorRequirement(
                                key=LABEL_NAME, operator="In", values=[name],
                            ),
                        ],
                    ),
                    topology_key="kubernetes.io/hostname",
                ),
            ]
        ),
    )


def get_statefulset_bootstrap_container(
    name: str, users: Optional[List[Dict[str, Any]]], license: Optional[Dict[str, Any]],
) -> V1Container:
    cluster_bootstrap_spec: Dict[str, List] = {
        "statements": [],
        "users": [
            {
                "name_var": "USERNAME_SYSTEM",
                "password_var": "PASSWORD_SYSTEM",
                "privileges": [{"grants": ["ALL"]}],
            }
        ],
    }
    cluster_bootstrap_env = [
        V1EnvVar(
            name="PASSWORD_SYSTEM",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    key="password", name=f"user-system-{name}",
                )
            ),
        ),
        V1EnvVar(name="USERNAME_SYSTEM", value=SYSTEM_USERNAME),
    ]

    if users:
        for i, user in enumerate(users):
            cluster_bootstrap_spec["users"].append(
                {
                    "name_var": f"USERNAME_{i}",
                    "password_var": f"PASSWORD_{i}",
                    "privileges": [{"grants": ["ALL"]}],
                }
            )
            cluster_bootstrap_env.extend(
                [
                    V1EnvVar(
                        name=f"PASSWORD_{i}",
                        value_from=V1EnvVarSource(
                            secret_key_ref=V1SecretKeySelector(
                                key=user["password"]["secretKeyRef"]["key"],
                                name=user["password"]["secretKeyRef"]["name"],
                            )
                        ),
                    ),
                    V1EnvVar(name=f"USERNAME_{i}", value=user["name"]),
                ]
            )

    if license:
        cluster_bootstrap_env.append(
            V1EnvVar(
                name="CRATE_LICENSE_KEY",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        key=license["secretKeyRef"]["key"],
                        name=license["secretKeyRef"]["name"],
                    )
                ),
            )
        )
        cluster_bootstrap_spec["statements"].append(
            {"sql": "SET LICENSE %s", "param_vars": ["CRATE_LICENSE_KEY"]}
        )
    return V1Container(
        command=[
            "sh",
            "-c",
            textwrap.dedent(
                # First dedent so every line starts w/o any spaces and then
                # inject the formatted YAML blob. Otherwise, the YAML would be
                # syntax would be screwed up.
                """
                    cat <<EOF > spec.yaml
                    {content}
                    EOF
                    bootstrap --hosts localhost --ports 5432 --spec spec.yaml
                    tail -f /dev/null
                """
            ).format(content=yaml.safe_dump(cluster_bootstrap_spec)),
        ],
        env=cluster_bootstrap_env,
        image=config.CLUSTER_BOOTSTRAP_IMAGE,
        name="database-bootstrap",
    )


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
    return [
        V1Container(
            command=[
                "/bin/sql_exporter",
                "-config.file=/config/sql-exporter.yaml",
                "-web.listen-address=:9399",
                "-web.metrics-path=/metrics",
            ],
            # There is no official release of 0.6, so let's use our own build
            # from commit 1498107
            # https://github.com/free/sql_exporter/commit/1498107
            image="cloud.registry.cr8.net/crate/sql-exporter:1498107",
            name="sql-exporter",
            ports=[V1ContainerPort(container_port=9399, name="sql-exporter")],
            volume_mounts=[
                V1VolumeMount(
                    mount_path="/config", name="crate-sql-exporter", read_only=True,
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
                http_get=V1HTTPGetAction(path="/ready", port="prometheus"),
                initial_delay_seconds=30,
                period_seconds=10,
            ),
            resources=V1ResourceRequirements(
                limits={
                    "cpu": str(node_spec["resources"]["cpus"]),
                    "memory": format_bitmath(
                        bitmath.parse_string_unsafe(node_spec["resources"]["memory"])
                    ),
                },
                requests={
                    "cpu": str(node_spec["resources"]["cpus"]),
                    "memory": format_bitmath(
                        bitmath.parse_string_unsafe(node_spec["resources"]["memory"])
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
    crate_node_name_prefix: str,
    node_name: str,
    node_spec: Dict[str, Any],
    cluster_settings: Optional[Dict[str, str]],
    has_ssl: bool,
    is_master: bool,
    is_data: bool,
) -> List[str]:
    settings = {
        "-Cstats.enabled": "true",
        "-Ccluster.name": name,
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
        "-Cgateway.recover_after_nodes": str(quorum(total_nodes_count)),
        "-Cgateway.expected_nodes": str(total_nodes_count),
        "-Cauth.host_based.enabled": "true",
        "-Cauth.host_based.config.0.user": "crate",
        "-Cauth.host_based.config.0.address": "_local_",
        "-Cauth.host_based.config.0.method": "trust",
        "-Cauth.host_based.config.99.method": "password",
        "-Cpath.data": ",".join(
            f"/data/data{i}" for i in range(node_spec["resources"]["disk"]["count"])
        ),
        "-Cprocessors": str(math.ceil(node_spec["resources"]["cpus"])),
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
    ]


def get_statefulset_crate_env(
    node_spec: Dict[str, Any],
    jmx_port: int,
    prometheus_port: int,
    ssl: Optional[Dict[str, Any]],
) -> List[V1EnvVar]:
    crate_env = [
        V1EnvVar(
            name="CRATE_HEAP_SIZE",
            value=str(
                int(
                    bitmath.parse_string_unsafe(node_spec["resources"]["memory"]).bytes
                    * node_spec["resources"]["heapRatio"]
                )
            ),
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
    node_spec: Dict[str, Any], ssl: Optional[Dict[str, Any]],
) -> List[V1VolumeMount]:
    volume_mounts = [
        V1VolumeMount(
            mount_path=f"/var/lib/crate/crate-jmx-exporter-{config.JMX_EXPORTER_VERSION}.jar",  # noqa
            name="jmxdir",
            sub_path=f"crate-jmx-exporter-{config.JMX_EXPORTER_VERSION}.jar",
        ),
        V1VolumeMount(mount_path="/resource", name="resource"),
    ]
    volume_mounts.extend(
        [
            V1VolumeMount(mount_path=f"/data/data{i}", name=f"data{i}")
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
                mount_path="/var/lib/crate/ssl/", name="keystore", read_only=True,
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
            image="busybox",
            name="init-sysctl",
            security_context=V1SecurityContext(privileged=True),
        ),
        V1Container(
            command=[
                "wget",
                "-O",
                f"/jmxdir/crate-jmx-exporter-{config.JMX_EXPORTER_VERSION}.jar",
                f"https://dl.bintray.com/crate/crate/io/crate/crate-jmx-exporter/{config.JMX_EXPORTER_VERSION}/crate-jmx-exporter-{config.JMX_EXPORTER_VERSION}.jar",  # noqa
            ],
            image="busybox",
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
            name="mkdir-heapdump",
            volume_mounts=[V1VolumeMount(name="resource", mount_path="/resource")],
        ),
    ]


def get_statefulset_pvc(node_spec: Dict[str, Any]) -> List[V1PersistentVolumeClaim]:
    size = format_bitmath(
        bitmath.parse_string_unsafe(node_spec["resources"]["disk"]["size"])
    )
    storage_class_name = node_spec["resources"]["disk"]["storageClass"]
    return [
        V1PersistentVolumeClaim(
            metadata=V1ObjectMeta(name=f"data{i}"),
            spec=V1PersistentVolumeClaimSpec(
                access_modes=["ReadWriteOnce"],
                resources=V1ResourceRequirements(requests={"storage": size}),
                storage_class_name=storage_class_name,
            ),
        )
        for i in range(node_spec["resources"]["disk"]["count"])
    ]


def get_statefulset_volumes(name: str, ssl: Optional[Dict[str, Any]]) -> List[V1Volume]:
    volumes = [
        V1Volume(
            config_map=V1ConfigMapVolumeSource(name=f"crate-sql-exporter-{name}"),
            name="crate-sql-exporter",
        ),
        V1Volume(
            name="resource",
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                claim_name=f"local-resource-{name}",
            ),
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
    namespace: str,
    name: str,
    labels: LabelType,
    treat_as_master: bool,
    treat_as_data: bool,
    node_name: str,
    node_name_prefix: str,
    node_spec: Dict[str, Any],
    master_nodes: List[str],
    total_nodes_count: int,
    http_port: int,
    jmx_port: int,
    postgres_port: int,
    prometheus_port: int,
    transport_port: int,
    crate_image: str,
    users: Optional[List[Dict[str, Any]]],
    license: Optional[Dict[str, Any]],
    ssl: Optional[Dict[str, Any]],
    cluster_settings: Optional[Dict[str, str]],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
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
            crate_node_name_prefix=node_name_prefix,
            node_name=node_name,
            node_spec=node_spec,
            cluster_settings=cluster_settings,
            has_ssl=bool(ssl),
            is_master=treat_as_master,
            is_data=treat_as_data,
        ),
        get_statefulset_crate_env(node_spec, jmx_port, prometheus_port, ssl),
        get_statefulset_crate_volume_mounts(node_spec, ssl),
    )

    if treat_as_master:
        containers.append(get_statefulset_bootstrap_container(name, users, license))

    return V1StatefulSet(
        metadata=V1ObjectMeta(
            annotations=node_spec.get("annotations"),
            labels=node_labels,
            name=full_pod_name_prefix,
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
                    annotations=node_annotations, labels=node_labels,
                ),
                spec=V1PodSpec(
                    affinity=get_statefulset_affinity(name),
                    containers=containers,
                    image_pull_secrets=image_pull_secrets,
                    init_containers=get_statefulset_init_containers(crate_image),
                    volumes=get_statefulset_volumes(name, ssl),
                ),
            ),
            update_strategy=V1StatefulSetUpdateStrategy(type="OnDelete"),
            volume_claim_templates=get_statefulset_pvc(node_spec),
        ),
    )


def create_statefulset(
    apps: AppsV1Api,
    namespace: str,
    name: str,
    labels: LabelType,
    treat_as_master: bool,
    treat_as_data: bool,
    node_name: str,
    node_name_prefix: str,
    node_spec: Dict[str, Any],
    master_nodes: List[str],
    total_nodes_count: int,
    http_port: int,
    jmx_port: int,
    postgres_port: int,
    prometheus_port: int,
    transport_port: int,
    crate_image: str,
    users: Optional[List[Dict[str, Any]]],
    license: Optional[Dict[str, Any]],
    ssl: Optional[Dict[str, Any]],
    cluster_settings: Optional[Dict[str, str]],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
) -> Awaitable[V1StatefulSet]:
    return call_kubeapi(
        apps.create_namespaced_stateful_set,
        logger,
        continue_on_conflict=True,
        namespace=namespace,
        body=get_statefulset(
            namespace,
            name,
            labels,
            treat_as_master,
            treat_as_data,
            node_name,
            node_name_prefix,
            node_spec,
            master_nodes,
            total_nodes_count,
            http_port,
            jmx_port,
            postgres_port,
            prometheus_port,
            transport_port,
            crate_image,
            users,
            license,
            ssl,
            cluster_settings,
            image_pull_secrets,
        ),
    )


def get_data_service(
    name: str,
    labels: LabelType,
    http_port: int,
    postgres_port: int,
    dns_record: Optional[str],
) -> V1Service:
    if dns_record:
        annotations = {"external-dns.alpha.kubernetes.io/hostname": dns_record}
    else:
        annotations = {}
    return V1Service(
        metadata=V1ObjectMeta(
            annotations=annotations, labels=labels, name=f"crate-{name}",
        ),
        spec=V1ServiceSpec(
            ports=[
                V1ServicePort(name="http", port=http_port, target_port=4200),
                V1ServicePort(name="psql", port=postgres_port, target_port=5432),
            ],
            selector={LABEL_COMPONENT: "cratedb", LABEL_NAME: name},
            type="LoadBalancer",
        ),
    )


def get_discovery_service(
    name: str, labels: LabelType, transport_port: int,
) -> V1Service:
    return V1Service(
        metadata=V1ObjectMeta(name=f"crate-discovery-{name}", labels=labels),
        spec=V1ServiceSpec(
            ports=[V1ServicePort(name="cluster", port=transport_port)],
            selector={LABEL_COMPONENT: "cratedb", LABEL_NAME: name},
        ),
    )


def create_services(
    core: CoreV1Api,
    namespace: str,
    name: str,
    labels: LabelType,
    http_port: int,
    postgres_port: int,
    transport_port: int,
    dns_record: Optional[str],
) -> Tuple[Awaitable[V1Service], Awaitable[V1Service]]:
    return (
        call_kubeapi(
            core.create_namespaced_service,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_data_service(name, labels, http_port, postgres_port, dns_record),
        ),
        call_kubeapi(
            core.create_namespaced_service,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_discovery_service(name, labels, transport_port),
        ),
    )


def get_system_user_secret(name: str, labels: LabelType) -> V1Secret:
    return V1Secret(
        data={"password": b64encode(gen_password(50))},
        metadata=V1ObjectMeta(name=f"user-system-{name}", labels=labels),
        type="Opaque",
    )


def create_system_user(
    core: CoreV1Api, namespace: str, name: str, labels: LabelType,
) -> Awaitable[V1Secret]:
    """
    The *CrateDB Operator* will need to perform operations on the CrateDB
    cluster. For that, it will use a ``system`` user who's credentials are
    created here.
    """
    return call_kubeapi(
        core.create_namespaced_secret,
        logger,
        continue_on_conflict=True,
        namespace=namespace,
        body=get_system_user_secret(name, labels),
    )
