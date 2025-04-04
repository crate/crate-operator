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
from typing import Any, Dict, List, Optional

from kubernetes_asyncio.client import (
    AppsV1Api,
    V1Container,
    V1ContainerPort,
    V1CronJob,
    V1CronJobSpec,
    V1Deployment,
    V1DeploymentSpec,
    V1EnvVar,
    V1EnvVarSource,
    V1JobSpec,
    V1JobTemplateSpec,
    V1LabelSelector,
    V1LocalObjectReference,
    V1ObjectFieldSelector,
    V1ObjectMeta,
    V1OwnerReference,
    V1PodSpec,
    V1PodTemplateSpec,
    V1SecretKeySelector,
)
from kubernetes_asyncio.client.api.batch_v1_api import BatchV1Api

from crate.operator.config import config
from crate.operator.constants import (
    BACKUP_METRICS_DEPLOYMENT_NAME,
    LABEL_COMPONENT,
    LABEL_NAME,
    SYSTEM_USERNAME,
)
from crate.operator.utils import crate
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.kubeapi import call_kubeapi
from crate.operator.utils.typing import LabelType


def get_aws_backup_env(
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


def get_azure_backup_env(
    name: str, http_port: int, backup_azure: Dict[str, Any], has_ssl: bool
) -> List[V1EnvVar]:
    schema = "https" if has_ssl else "http"
    return [
        V1EnvVar(
            name="NAMESPACE",
            value_from=V1EnvVarSource(
                field_ref=V1ObjectFieldSelector(
                    api_version="v1",
                    field_path="metadata.namespace",
                )
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
            name="AZURE_CONTAINER",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    key=backup_azure["container"]["secretKeyRef"]["key"],
                    name=backup_azure["container"]["secretKeyRef"]["name"],
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


def get_aws_backup_cronjob(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    http_port: int,
    backup_aws: Dict[str, Any],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    has_ssl: bool,
) -> V1CronJob:
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
        + get_aws_backup_env(name, http_port, backup_aws, has_ssl)
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

    return V1CronJob(
        metadata=V1ObjectMeta(
            name=f"create-snapshot-{name}",
            labels=labels,
            owner_references=owner_references,
        ),
        spec=V1CronJobSpec(
            concurrency_policy="Forbid",
            failed_jobs_history_limit=1,
            job_template=V1JobTemplateSpec(
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


def get_azure_backup_cronjob(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    http_port: int,
    backup_azure: Dict[str, Any],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    has_ssl: bool,
) -> V1CronJob:
    env = (
        [
            V1EnvVar(
                name="AZURE_ACCOUNT_NAME",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        key=backup_azure["accountName"]["secretKeyRef"]["key"],
                        name=backup_azure["accountName"]["secretKeyRef"]["name"],
                    ),
                ),
            ),
            V1EnvVar(
                name="AZURE_ACCOUNT_KEY",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        key=backup_azure["accountKey"]["secretKeyRef"]["key"],
                        name=backup_azure["accountKey"]["secretKeyRef"]["name"],
                    ),
                ),
            ),
            V1EnvVar(name="CLUSTER_ID", value=name),
            V1EnvVar(name="PYTHONWARNINGS", value="ignore:Unverified HTTPS request"),
            V1EnvVar(name="REPOSITORY_PREFIX", value="system_backup"),
        ]
        + get_azure_backup_env(name, http_port, backup_azure, has_ssl)
        + get_webhook_env()
    )

    cron_job = V1CronJob(
        metadata=V1ObjectMeta(
            name=f"create-snapshot-{name}",
            labels=labels,
            owner_references=owner_references,
        ),
        spec=V1CronJobSpec(
            concurrency_policy="Forbid",
            failed_jobs_history_limit=1,
            job_template=V1JobTemplateSpec(
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
            schedule=backup_azure["cron"],
            successful_jobs_history_limit=1,
        ),
    )
    return cron_job


def get_aws_backup_metrics_exporter(
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
    ] + get_aws_backup_env(name, http_port, backup_aws, has_ssl)
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


def get_azure_backup_metrics_exporter(
    owner_references: Optional[List[V1OwnerReference]],
    name: str,
    labels: LabelType,
    http_port: int,
    prometheus_port: int,
    backup_azure: Dict[str, Any],
    image_pull_secrets: Optional[List[V1LocalObjectReference]],
    has_ssl: bool,
) -> V1Deployment:
    env = [
        V1EnvVar(name="EXPORTER_PORT", value=str(prometheus_port)),
        V1EnvVar(name="PYTHONWARNINGS", value="ignore:Unverified HTTPS request"),
        V1EnvVar(name="REPOSITORY_PREFIX", value="system_backup"),
    ] + get_azure_backup_env(name, http_port, backup_azure, has_ssl)
    # BASE_PATH, bucket and region are required by the metrics exporter
    env.extend(
        [
            V1EnvVar(name="BASE_PATH", value=""),
            V1EnvVar(name="BUCKET", value="azure"),
            V1EnvVar(
                name="REGION",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        key=backup_azure["region"]["secretKeyRef"]["key"],
                        name=backup_azure["region"]["secretKeyRef"]["name"],
                    ),
                ),
            ),
        ]
    )
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
    backup_azure = backups.get("azure_blob")

    async with GlobalApiClient() as api_client:
        apps = AppsV1Api(api_client)
        batch = BatchV1Api(api_client)
        if backup_aws:
            await call_kubeapi(
                batch.create_namespaced_cron_job,
                logger,
                continue_on_conflict=True,
                namespace=namespace,
                body=get_aws_backup_cronjob(
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
                body=get_aws_backup_metrics_exporter(
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

        if backup_azure:
            await call_kubeapi(
                batch.create_namespaced_cron_job,
                logger,
                continue_on_conflict=True,
                namespace=namespace,
                body=get_azure_backup_cronjob(
                    owner_references,
                    name,
                    labels,
                    http_port,
                    backup_azure,
                    image_pull_secrets,
                    has_ssl,
                ),
            )
            await call_kubeapi(
                apps.create_namespaced_deployment,
                logger,
                continue_on_conflict=True,
                namespace=namespace,
                body=get_azure_backup_metrics_exporter(
                    owner_references,
                    name,
                    labels,
                    http_port,
                    prometheus_port,
                    backup_azure,
                    image_pull_secrets,
                    has_ssl,
                ),
            )


async def update_backup_schedule_in_cronjob(namespace, name, new_value):
    async with GlobalApiClient() as api_client:

        patch = {"spec": {"schedule": new_value}}

        batch = BatchV1Api(api_client)
        await batch.patch_namespaced_cron_job(
            f"create-snapshot-{name}", namespace, patch
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
