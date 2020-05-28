import logging
import math
import textwrap
from typing import Any, Awaitable, Dict, List, Optional, Tuple, Union

import bitmath
import yaml
from kubernetes_asyncio.client import (
    AppsV1Api,
    BatchV1beta1Api,
    CoreV1Api,
    V1Affinity,
    V1beta1CronJob,
    V1beta1CronJobSpec,
    V1beta1JobTemplateSpec,
    V1ConfigMap,
    V1ConfigMapKeySelector,
    V1ConfigMapVolumeSource,
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1EnvVarSource,
    V1HostPathVolumeSource,
    V1HTTPGetAction,
    V1JobSpec,
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
from crate.operator.utils.typing import LABEL_TYPE

logger = logging.getLogger(__name__)


def get_backup_cronjob(
    name: str,
    labels: LABEL_TYPE,
    backup_aws: Dict[str, Any],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
) -> V1beta1CronJob:
    containers = [
        V1Container(
            command=["backup", "-vv"],
            env=[
                V1EnvVar(
                    name="AWS_ACCESS_KEY_ID",
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            key=backup_aws["accessKeyId"]["secretKeyRef"]["key"],
                            name=backup_aws["accessKeyId"]["secretKeyRef"]["name"],
                        ),
                    ),
                ),
                V1EnvVar(
                    name="AWS_SECRET_ACCESS_KEY",
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            key=backup_aws["secretAccessKey"]["secretKeyRef"]["key"],
                            name=backup_aws["secretAccessKey"]["secretKeyRef"]["name"],
                        ),
                    ),
                ),
                V1EnvVar(name="BASE_PATH", value=backup_aws.get("basePath", "")),
                V1EnvVar(
                    name="BUCKET",
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            key=backup_aws["s3Bucket"]["secretKeyRef"]["key"],
                            name=backup_aws["s3Bucket"]["secretKeyRef"]["name"],
                        ),
                    ),
                ),
                V1EnvVar(name="CLUSTER_ID", value=name),
                V1EnvVar(name="EXPORTER_PORT", value="9102"),
                V1EnvVar(name="HOSTS", value=f"https://crate-{name}:4200"),
                V1EnvVar(
                    name="PASSWORD",
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            key="password", name=f"user-system-{name}",
                        ),
                    ),
                ),
                V1EnvVar(
                    name="PYTHONWARNINGS", value="ignore:Unverified HTTPS request",
                ),
                V1EnvVar(
                    name="REGION",
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            key=backup_aws["region"]["secretKeyRef"]["key"],
                            name=backup_aws["region"]["secretKeyRef"]["name"],
                        ),
                    ),
                ),
                V1EnvVar(name="REPOSITORY_PREFIX", value="system_backup"),
                V1EnvVar(name="USERNAME", value=SYSTEM_USERNAME),
            ],
            image=config.CLUSTER_BACKUP_IMAGE,
            name="backup",
        ),
    ]
    return V1beta1CronJob(
        metadata=V1ObjectMeta(name=f"create-snapshot-{name}", labels=labels,),
        spec=V1beta1CronJobSpec(
            concurrency_policy="Forbid",
            failed_jobs_history_limit=1,
            job_template=V1beta1JobTemplateSpec(
                metadata=V1ObjectMeta(labels=labels, name=f"create-snapshot-{name}",),
                spec=V1JobSpec(
                    template=V1PodTemplateSpec(
                        metadata=V1ObjectMeta(
                            labels=labels, name=f"create-snapshot-{name}",
                        ),
                        spec=V1PodSpec(
                            containers=containers,
                            image_pull_secrets=image_pull_secrets,
                            restart_policy="Never",
                        ),
                    ),
                ),
            ),
            schedule=backup_aws["cron"],
            successful_jobs_history_limit=1,
        ),
    )


def get_backup_metrics_exporter(
    name: str,
    labels: LABEL_TYPE,
    backup_aws: Dict[str, Any],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
) -> V1Deployment:
    containers = [
        V1Container(
            command=["metrics-exporter", "-vv"],
            env=[
                V1EnvVar(name="BASE_PATH", value=backup_aws.get("basePath", "")),
                V1EnvVar(
                    name="BUCKET",
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            key=backup_aws["s3Bucket"]["secretKeyRef"]["key"],
                            name=backup_aws["s3Bucket"]["secretKeyRef"]["name"],
                        ),
                    ),
                ),
                V1EnvVar(name="EXPORTER_PORT", value="9102"),
                V1EnvVar(name="HOSTS", value=f"https://crate-{name}:4200"),
                V1EnvVar(
                    name="PASSWORD",
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            key="password", name=f"user-system-{name}",
                        ),
                    ),
                ),
                V1EnvVar(
                    name="PYTHONWARNINGS", value="ignore:Unverified HTTPS request",
                ),
                V1EnvVar(
                    name="REGION",
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            key=backup_aws["region"]["secretKeyRef"]["key"],
                            name=backup_aws["region"]["secretKeyRef"]["name"],
                        ),
                    ),
                ),
                V1EnvVar(name="REPOSITORY_PREFIX", value="system_backup"),
                V1EnvVar(name="USERNAME", value=SYSTEM_USERNAME),
            ],
            image=config.CLUSTER_BACKUP_IMAGE,
            name="metrics-exporter",
            ports=[V1ContainerPort(container_port=9102, name="backup-metrics")],
        )
    ]
    return V1Deployment(
        metadata=V1ObjectMeta(name=f"backup-metrics-{name}", labels=labels),
        spec=V1DeploymentSpec(
            replicas=1,
            selector=V1LabelSelector(
                match_labels={LABEL_COMPONENT: "backup", LABEL_NAME: name}
            ),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(
                    annotations={
                        "prometheus.io/port": "9102",
                        "prometheus.io/scrape": "true",
                    },
                    labels=labels,
                    name=f"backup-metrics-{name}",
                ),
                spec=V1PodSpec(
                    containers=containers,
                    image_pull_secrets=image_pull_secrets,
                    restart_policy="Always",
                ),
            ),
        ),
    )


def create_backups(
    apps: AppsV1Api,
    batchv1beta1: BatchV1beta1Api,
    namespace: str,
    name: str,
    labels: LABEL_TYPE,
    backups: Dict[str, Any],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
) -> Union[Tuple[Awaitable[V1beta1CronJob], Awaitable[V1Deployment]], Tuple]:
    backup_aws = backups.get("aws")
    if backup_aws:
        return (
            call_kubeapi(
                batchv1beta1.create_namespaced_cron_job,
                logger,
                continue_on_conflict=True,
                namespace=namespace,
                body=get_backup_cronjob(name, labels, backup_aws, image_pull_secrets),
            ),
            call_kubeapi(
                apps.create_namespaced_deployment,
                logger,
                continue_on_conflict=True,
                namespace=namespace,
                body=get_backup_metrics_exporter(
                    name, labels, backup_aws, image_pull_secrets
                ),
            ),
        )
    return ()


def get_crate_config(
    name: str, labels: LABEL_TYPE, jmx_port: int, prometheus_port: int,
) -> V1ConfigMap:
    return V1ConfigMap(
        metadata=V1ObjectMeta(name=f"crate-{name}", labels=labels),
        data={
            "crate.java_opts": " ".join(
                [
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
            ),
        },
    )


def get_log4j2_config(name: str, labels: LABEL_TYPE) -> V1ConfigMap:
    return V1ConfigMap(
        metadata=V1ObjectMeta(name=f"crate-log4j2-{name}", labels=labels),
        data={
            "log4j2.properties": textwrap.dedent(
                """
                    # Crate uses log4j as internal logging abstraction.
                    # Configure log4j as you need it to behave by setting the log4j prefixes in
                    # this file.
                    status = error

                    rootLogger.level = info
                    rootLogger.appenderRefs = stdout, stderr
                    rootLogger.appenderRef.stdout.ref = STDOUT
                    rootLogger.appenderRef.stderr.ref = STDERR

                    # log action execution errors for easier debugging
                    # logger.action.name = org.crate.action.sql
                    # logger.action.level = debug

                    #  Peer shard recovery
                    # logger.indices_recovery.name: indices.recovery
                    # logger.indices_recovery.level: DEBUG

                    #  Discovery
                    #  Crate will discover the other nodes within its own cluster.
                    #  If you want to log the discovery process, set the following:
                    # logger.discovery.name: discovery
                    # logger.discovery.level: TRACE

                    # mute amazon s3 client logging a bit
                    logger.aws.name = com.amazonaws
                    logger.aws.level = warn

                    # Define your appenders here.
                    # Like mentioned above, use the log4j prefixes to configure for example the
                    # type or layout.
                    # For all available settings, take a look at the log4j documentation.
                    # http://logging.apache.org/log4j/2.x/
                    # http://logging.apache.org/log4j/2.x/manual/appenders.html

                    # configure stdout
                    appender.consoleOut.type = Console
                    appender.consoleOut.name = STDOUT
                    appender.consoleOut.target = System.out
                    appender.consoleOut.direct = true
                    appender.consoleOut.layout.type = PatternLayout
                    appender.consoleOut.layout.pattern = [%d{ISO8601}][%-5p][%-25c{1.}] [%node_name] %marker%m%n
                    appender.consoleOut.filter.threshold.type = ThresholdFilter
                    appender.consoleOut.filter.threshold.level = warn
                    appender.consoleOut.filter.threshold.onMatch = DENY
                    appender.consoleOut.filter.threshold.onMisMatch = ACCEPT

                    # configure stderr
                    appender.consoleErr.type = Console
                    appender.consoleErr.name = STDERR
                    appender.consoleErr.target = SYSTEM_ERR
                    appender.consoleErr.direct = true
                    appender.consoleErr.layout.type = PatternLayout
                    appender.consoleErr.layout.pattern = [%d{ISO8601}][%-5p][%-25c{1.}] [%node_name] %marker%m%n
                    appender.consoleErr.filter.threshold.type = ThresholdFilter
                    appender.consoleErr.filter.threshold.level = warn
                    appender.consoleErr.filter.threshold.onMatch = ACCEPT
                    appender.consoleErr.filter.threshold.onMisMatch = DENY
                """,  # noqa
            ),
        },
    )


def get_sql_exporter_config(name: str, labels: LABEL_TYPE) -> V1ConfigMap:
    return V1ConfigMap(
        metadata=V1ObjectMeta(name=f"crate-sql-exporter-{name}", labels=labels),
        data={
            "crate_sql_exporter_conf.yaml": textwrap.dedent(
                """
                    # Global settings and defaults.
                    global:
                    # Subtracted from Prometheus' scrape_timeout to give us some headroom and prevent Prometheus from
                    # timing out first.
                    scrape_timeout_offset: 500ms
                    # Minimum interval between collector runs: by default (0s) collectors are executed on every scrape.
                    min_interval: 0s
                    # Maximum number of open connections to any one target. Metric queries will run concurrently on
                    # multiple connections.
                    max_connections: 1
                    # Maximum number of idle connections to any one target.
                    max_idle_connections: 1

                    target:
                    data_source_name: "postgres://crate@localhost:5432/?connect_timeout=5"
                    collectors: [responsivity_collector]

                    collector_files:
                    - "*_collector.yaml"
                """  # noqa
            ),
            "responsivity_collector.yaml": textwrap.dedent(
                """
                    collector_name: responsivity_collector

                    # This metric is intended to alert us to when CrateDB Cloud clusters are unresponsive.
                    # This uses a query which involves communication with other nodes to test
                    # that the communication between the nodes work.
                    # (In this case, all that contain any shards)
                    #
                    # If nodes are not reachable the query would fail.
                    # (Other queries like on `sys.cluster` or `information_schema` can be served
                    # by a single node *without* communication with other nodes)
                    metrics:
                    - metric_name: responsivity
                        type: gauge
                        help: "Indicates whether the CrateDB node is responding to queries. Will not return if the node is stuck."
                        value_label: responsive
                        values: [responsive, states]
                        query: |
                        SELECT count(state) as states, 1 AS responsive
                        FROM sys.shards;
                """  # noqa
            ),
        },
    )


def create_configs(
    core: CoreV1Api,
    namespace: str,
    name: str,
    labels: LABEL_TYPE,
    jmx_port: int,
    prometheus_port: int,
) -> Tuple[Awaitable[V1ConfigMap], Awaitable[V1ConfigMap], Awaitable[V1ConfigMap]]:
    return (
        call_kubeapi(
            core.create_namespaced_config_map,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_crate_config(name, labels, jmx_port, prometheus_port),
        ),
        call_kubeapi(
            core.create_namespaced_config_map,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_log4j2_config(name, labels),
        ),
        call_kubeapi(
            core.create_namespaced_config_map,
            logger,
            continue_on_conflict=True,
            namespace=namespace,
            body=get_sql_exporter_config(name, labels),
        ),
    )


def get_debug_persistent_volume(
    namespace: str, name: str, labels: LABEL_TYPE
) -> V1PersistentVolume:
    return V1PersistentVolume(
        metadata=V1ObjectMeta(name=f"temp-pv-{namespace}-{name}", labels=labels,),
        spec=V1PersistentVolumeSpec(
            access_modes=["ReadWriteOnce"],
            capacity={"storage": format_bitmath(config.DEBUG_VOLUME_SIZE)},
            host_path=V1HostPathVolumeSource(path=f"/mnt/resource/{namespace}-{name}"),
            storage_class_name=config.DEBUG_VOLUME_STORAGE_CLASS,
        ),
    )


def get_debug_persistent_volume_claim(
    name: str, labels: LABEL_TYPE
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
    core: CoreV1Api, namespace: str, name: str, labels: LABEL_TYPE
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


def get_statefulset_affinity(name: str, node_name: str) -> V1Affinity:
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
                            # TODO: BEGIN REMOVE
                            V1LabelSelectorRequirement(
                                key=LABEL_NODE_NAME, operator="In", values=[node_name],
                            ),
                            # END REMOVE
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
    namespace: str,
    name: str,
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
                "-config.file=/config/crate_sql_exporter_conf.yaml",
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
                    mount_path="/config/crate_sql_exporter_conf.yaml",
                    name="crate-sql-exporter",
                    sub_path="crate_sql_exporter_conf.yaml",
                ),
                V1VolumeMount(
                    mount_path="/config/responsivity_collector.yaml",
                    name="crate-sql-exporter",
                    sub_path="responsivity_collector.yaml",
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
                "-Cssl.keystore_filepath": "/etc/cratedb/certificate.jks",
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

    return ["/docker-entrypoint.sh"] + [f"{k}={v}" for k, v in settings.items()]


def get_statefulset_crate_env(
    name: str, node_spec: Dict[str, Any], ssl: Optional[Dict[str, Any]],
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
            value_from=V1EnvVarSource(
                config_map_key_ref=V1ConfigMapKeySelector(
                    key="crate.java_opts", name=f"crate-{name}"
                ),
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
            mount_path="/crate/config/log4j2.properties",
            name="crate-log4j2",
            sub_path="log4j2.properties",
        ),
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
        volume_mounts.append(
            V1VolumeMount(mount_path="/etc/cratedb/", name="keystore"),
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
            config_map=V1ConfigMapVolumeSource(
                name=f"crate-log4j2-{name}",
                items=[V1KeyToPath(key="log4j2.properties", path="log4j2.properties")],
            ),
            name="crate-log4j2",
        ),
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
                secret=V1SecretVolumeSource(
                    secret_name=ssl["keystore"]["secretKeyRef"]["name"],
                    items=[
                        V1KeyToPath(
                            key=ssl["keystore"]["secretKeyRef"]["key"],
                            path="certificate.jks",
                        )
                    ],
                ),
                name="keystore",
            ),
        )

    return volumes


def get_statefulset(
    namespace: str,
    name: str,
    labels: LABEL_TYPE,
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
        namespace,
        name,
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
        get_statefulset_crate_env(name, node_spec, ssl),
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
                    affinity=get_statefulset_affinity(name, node_name),
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
    labels: LABEL_TYPE,
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
    labels: LABEL_TYPE,
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
    name: str, labels: LABEL_TYPE, transport_port: int,
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
    labels: LABEL_TYPE,
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


def get_system_user_secret(name: str, labels: LABEL_TYPE) -> V1Secret:
    return V1Secret(
        data={"password": b64encode(gen_password(50))},
        metadata=V1ObjectMeta(name=f"user-system-{name}", labels=labels),
        type="Opaque",
    )


def create_system_user(
    core: CoreV1Api, namespace: str, name: str, labels: LABEL_TYPE,
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
