import logging
from typing import Any, Awaitable, Dict, List, Optional, Tuple, Union

from kubernetes_asyncio.client import (
    AppsV1Api,
    BatchV1beta1Api,
    V1beta1CronJob,
    V1beta1CronJobSpec,
    V1beta1JobTemplateSpec,
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1EnvVar,
    V1EnvVarSource,
    V1JobSpec,
    V1LabelSelector,
    V1LocalObjectReference,
    V1ObjectMeta,
    V1OwnerReference,
    V1PodSpec,
    V1PodTemplateSpec,
    V1SecretKeySelector,
)

from crate.operator.config import config
from crate.operator.constants import LABEL_COMPONENT, LABEL_NAME, SYSTEM_USERNAME
from crate.operator.utils.kubeapi import call_kubeapi
from crate.operator.utils.typing import LabelType

logger = logging.getLogger(__name__)


def get_backup_env(
    name: str, http_port: int, backup_aws: Dict[str, Any], has_ssl: bool,
) -> List[V1EnvVar]:
    schema = "https" if has_ssl else "http"
    return [
        V1EnvVar(name="BASE_PATH", value=backup_aws.get("basePath", "")),
        V1EnvVar(
            name="BUCKET",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    key=backup_aws["bucket"]["secretKeyRef"]["key"],
                    name=backup_aws["bucket"]["secretKeyRef"]["name"],
                ),
            ),
        ),
        V1EnvVar(name="HOSTS", value=f"{schema}://crate-{name}:{http_port}"),
        V1EnvVar(
            name="PASSWORD",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    key="password", name=f"user-system-{name}",
                ),
            ),
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
        V1EnvVar(name="USERNAME", value=SYSTEM_USERNAME),
    ]


def get_backup_cronjob(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    http_port: int,
    backup_aws: Dict[str, Any],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    has_ssl: bool,
) -> V1beta1CronJob:
    env = [
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
        V1EnvVar(name="CLUSTER_ID", value=name),
        V1EnvVar(name="PYTHONWARNINGS", value="ignore:Unverified HTTPS request"),
        V1EnvVar(name="REPOSITORY_PREFIX", value="system_backup"),
    ] + get_backup_env(name, http_port, backup_aws, has_ssl)
    return V1beta1CronJob(
        metadata=V1ObjectMeta(
            name=f"create-snapshot-{name}",
            labels=labels,
            owner_references=owner_references,
        ),
        spec=V1beta1CronJobSpec(
            concurrency_policy="Forbid",
            failed_jobs_history_limit=1,
            job_template=V1beta1JobTemplateSpec(
                metadata=V1ObjectMeta(labels=labels, name=f"create-snapshot-{name}"),
                spec=V1JobSpec(
                    template=V1PodTemplateSpec(
                        metadata=V1ObjectMeta(
                            labels=labels, name=f"create-snapshot-{name}",
                        ),
                        spec=V1PodSpec(
                            containers=[
                                V1Container(
                                    command=["backup", "-vv"],
                                    env=env,
                                    image=config.CLUSTER_BACKUP_IMAGE,
                                    name="backup",
                                )
                            ],
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
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    http_port: int,
    prometheus_port: int,
    backup_aws: Dict[str, Any],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    has_ssl: bool,
) -> V1Deployment:
    env = [
        V1EnvVar(name="EXPORTER_PORT", value=str(prometheus_port)),
        V1EnvVar(name="PYTHONWARNINGS", value="ignore:Unverified HTTPS request"),
        V1EnvVar(name="REPOSITORY_PREFIX", value="system_backup"),
    ] + get_backup_env(name, http_port, backup_aws, has_ssl)
    return V1Deployment(
        metadata=V1ObjectMeta(
            name=f"backup-metrics-{name}",
            labels=labels,
            owner_references=owner_references,
        ),
        spec=V1DeploymentSpec(
            replicas=1,
            selector=V1LabelSelector(
                match_labels={LABEL_COMPONENT: "backup", LABEL_NAME: name}
            ),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(
                    annotations={
                        "prometheus.io/port": str(prometheus_port),
                        "prometheus.io/scrape": "true",
                    },
                    labels=labels,
                    name=f"backup-metrics-{name}",
                ),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            command=["metrics-exporter", "-vv"],
                            env=env,
                            image=config.CLUSTER_BACKUP_IMAGE,
                            name="metrics-exporter",
                            ports=[
                                V1ContainerPort(
                                    container_port=prometheus_port,
                                    name="backup-metrics",
                                )
                            ],
                        )
                    ],
                    image_pull_secrets=image_pull_secrets,
                    restart_policy="Always",
                ),
            ),
        ),
    )


def create_backups(
    apps: AppsV1Api,
    batchv1_beta1: BatchV1beta1Api,
    owner_references: Optional[List[V1OwnerReference]],
    namespace: str,
    name: str,
    labels: LabelType,
    http_port: int,
    prometheus_port: int,
    backups: Dict[str, Any],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    has_ssl: bool,
) -> Union[Tuple[Awaitable[V1beta1CronJob], Awaitable[V1Deployment]], Tuple]:
    backup_aws = backups.get("aws")
    if backup_aws:
        return (
            call_kubeapi(
                batchv1_beta1.create_namespaced_cron_job,
                logger,
                continue_on_conflict=True,
                namespace=namespace,
                body=get_backup_cronjob(
                    owner_references,
                    name,
                    labels,
                    http_port,
                    backup_aws,
                    image_pull_secrets,
                    has_ssl,
                ),
            ),
            call_kubeapi(
                apps.create_namespaced_deployment,
                logger,
                continue_on_conflict=True,
                namespace=namespace,
                body=get_backup_metrics_exporter(
                    owner_references,
                    name,
                    labels,
                    http_port,
                    prometheus_port,
                    backup_aws,
                    image_pull_secrets,
                    has_ssl,
                ),
            ),
        )
    return ()
