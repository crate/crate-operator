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

import logging
from typing import Any, Dict, List, Optional

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
    V1ObjectFieldSelector,
    V1ObjectMeta,
    V1OwnerReference,
    V1PodSpec,
    V1PodTemplateSpec,
    V1SecretKeySelector,
)
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.config import config
from crate.operator.constants import (
    BACKUP_METRICS_DEPLOYMENT_NAME,
    LABEL_COMPONENT,
    LABEL_NAME,
    SYSTEM_USERNAME,
)
from crate.operator.utils import crate
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.kubeapi import call_kubeapi
from crate.operator.utils.typing import LabelType


def get_backup_env(
    name: str, http_port: int, backup_aws: Dict[str, Any], has_ssl: bool
) -> List[V1EnvVar]:
    schema = "https" if has_ssl else "http"
    return [
        # The base path is here for backwards-compatibility and
        # is not used by newer versions of the backup CronJob,
        # in favour of the namespace.
        V1EnvVar(name="BASE_PATH", value=backup_aws.get("basePath", "")),
        V1EnvVar(
            name="NAMESPACE",
            value_from=V1EnvVarSource(
                field_ref=V1ObjectFieldSelector(
                    api_version="v1",
                    field_path="metadata.namespace",
                )
            ),
        ),
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
                    key="password", name=f"user-system-{name}"
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


def get_webhook_env():
    if (
        config.WEBHOOK_URL is not None
        and config.WEBHOOK_USERNAME is not None
        and config.WEBHOOK_PASSWORD is not None
    ):
        return [
            V1EnvVar(name="WEBHOOK_URL", value=config.WEBHOOK_URL),
            V1EnvVar(name="AUTH_USER", value=config.WEBHOOK_USERNAME),
            V1EnvVar(name="AUTH_PASS", value=config.WEBHOOK_PASSWORD),
        ]
    return []


def get_backup_cronjob(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    http_port: int,
    backup_aws: Dict[str, Any],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    has_ssl: bool,
) -> V1beta1CronJob:
    env = (
        [
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
        ]
        + get_backup_env(name, http_port, backup_aws, has_ssl)
        + get_webhook_env()
    )

    if "endpointUrl" in backup_aws:
        env.append(
            V1EnvVar(
                name="S3_ENDPOINT_URL",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        key=backup_aws["endpointUrl"]["secretKeyRef"]["key"],
                        name=backup_aws["endpointUrl"]["secretKeyRef"]["name"],
                        optional=True,
                    ),
                ),
            )
        )

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
                            labels=labels, name=f"create-snapshot-{name}"
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
            name=BACKUP_METRICS_DEPLOYMENT_NAME.format(name=name),
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
                    name=BACKUP_METRICS_DEPLOYMENT_NAME.format(name=name),
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


async def create_backups(
    owner_references: Optional[List[V1OwnerReference]],
    namespace: str,
    name: str,
    labels: LabelType,
    http_port: int,
    prometheus_port: int,
    backups: Dict[str, Any],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    has_ssl: bool,
    logger: logging.Logger,
) -> None:
    backup_aws = backups.get("aws")
    async with ApiClient() as api_client:
        apps = AppsV1Api(api_client)
        batchv1_beta1 = BatchV1beta1Api(api_client)
        if backup_aws:
            await call_kubeapi(
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
            )
            await call_kubeapi(
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
            )


class CreateBackupsSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_create_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        owner_references: Optional[List[V1OwnerReference]],
        backup_metrics_labels: LabelType,
        http_port: int,
        prometheus_port: int,
        backups: Dict[str, Any],
        image_pull_secrets: Optional[List[V1LocalObjectReference]],
        has_ssl: bool,
        logger: logging.Logger,
        **kwargs: Any,
    ):

        await create_backups(
            owner_references,
            namespace,
            name,
            backup_metrics_labels,
            http_port,
            prometheus_port,
            backups,
            image_pull_secrets,
            has_ssl,
            logger,
        )
