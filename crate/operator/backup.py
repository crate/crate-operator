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

import kopf
from kubernetes_asyncio.client import (
    AppsV1Api,
    BatchV1Api,
    BatchV1beta1Api,
    CoreV1Api,
    V1beta1CronJob,
    V1beta1CronJobList,
    V1beta1CronJobSpec,
    V1beta1JobTemplateSpec,
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1EnvVar,
    V1EnvVarSource,
    V1JobList,
    V1JobSpec,
    V1JobStatus,
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
from crate.operator.constants import LABEL_COMPONENT, LABEL_NAME, SYSTEM_USERNAME
from crate.operator.cratedb import are_snapshots_in_progress, connection_factory
from crate.operator.utils.kopf import StateBasedSubHandler, subhandler_partial
from crate.operator.utils.kubeapi import (
    call_kubeapi,
    get_host,
    get_system_user_password,
)
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


DISABLE_CRONJOB_HANDLER_ID = "disable_cronjob"
IGNORE_CRONJOB = "ignore_cronjob"
CRONJOB_SUSPENDED = "cronjob_suspended"
CRONJOB_NAME = "cronjob_name"


class EnsureNoBackupsSubHandler(StateBasedSubHandler):
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        kopf.register(
            fn=subhandler_partial(
                self._ensure_cronjob_suspended, namespace, name, logger
            ),
            id=DISABLE_CRONJOB_HANDLER_ID,
        )
        kopf.register(
            fn=subhandler_partial(
                self._ensure_no_snapshots_in_progress, namespace, name, logger
            ),
            id="ensure_no_snapshots_in_progress",
        )
        kopf.register(
            fn=subhandler_partial(
                self._ensure_no_backup_cronjobs_running, namespace, name, logger
            ),
            id="ensure_no_cronjobs_running",
        )

    @staticmethod
    async def _ensure_cronjob_suspended(
        namespace: str, name: str, logger: logging.Logger
    ) -> Optional[Dict]:
        async with ApiClient() as api_client:
            batch = BatchV1beta1Api(api_client)

            jobs: V1beta1CronJobList = await batch.list_namespaced_cron_job(namespace)

            for job in jobs.items:
                job_name = job.metadata.name
                labels = job.metadata.labels
                if (
                    labels.get("app.kubernetes.io/component") == "backup"
                    and labels.get("app.kubernetes.io/name") == name
                ):
                    current_suspend_status = job.spec.suspend
                    if current_suspend_status:
                        logger.warn(
                            f"Found job {job_name} that is already suspended, ignoring"
                        )
                        return {
                            CRONJOB_NAME: job_name,
                            CRONJOB_SUSPENDED: True,
                            IGNORE_CRONJOB: True,
                        }

                    logger.info(
                        f"Temporarily suspending CronJob {job_name} "
                        f"while cluster update in progress"
                    )
                    update = {"spec": {"suspend": True}}
                    await batch.patch_namespaced_cron_job(job_name, namespace, update)
                    return {CRONJOB_NAME: job_name, CRONJOB_SUSPENDED: True}

        return None

    async def _ensure_no_snapshots_in_progress(self, namespace, name, logger):
        async with ApiClient() as api_client:
            core = CoreV1Api(api_client)

            host = await get_host(core, namespace, name)
            password = await get_system_user_password(core, namespace, name)
            conn_factory = connection_factory(host, password)

            snapshots_in_progress, statement = await are_snapshots_in_progress(
                conn_factory, logger
            )
            if snapshots_in_progress:
                # Raising a TemporaryError will clear any registered subhandlers, so we
                # execute this one directly instead to make sure it runs.
                # The same guarantees about it being executed only once still stand.
                await kopf.execute(
                    fns={
                        "notify_backup_running": subhandler_partial(
                            self._notify_backup_running, logger
                        )
                    }
                )
                raise kopf.TemporaryError(
                    "A snapshot is currently in progress, "
                    f"waiting for it to finish: {statement}",
                    delay=30,
                )

    async def _ensure_no_backup_cronjobs_running(
        self, namespace: str, name: str, logger: logging.Logger
    ):
        async with ApiClient() as api_client:
            batch = BatchV1Api(api_client)

            jobs: V1JobList = await call_kubeapi(
                batch.list_namespaced_job, logger, namespace=namespace
            )
            for job in jobs.items:
                job_name = job.metadata.name
                labels = job.metadata.labels
                job_status: V1JobStatus = job.status
                if (
                    labels.get("app.kubernetes.io/component") == "backup"
                    and labels.get("app.kubernetes.io/name") == name
                    and job_status.active is not None
                ):
                    await kopf.execute(
                        fns={
                            "notify_backup_running": subhandler_partial(
                                self._notify_backup_running, logger
                            )
                        }
                    )
                    raise kopf.TemporaryError(
                        "A snapshot k8s job is currently running, "
                        f"waiting for it to finish: {job_name}",
                        delay=30,
                    )

    async def _notify_backup_running(self, logger):
        ...
        # TODO: Send notification. This throws an exception as the API validation fails.
        # self.schedule_notification(
        #     WebhookEvent.SCALE,
        #     WebhookTemporaryFailurePayload(reason="A backup is in progress"),
        #     WebhookStatus.TEMPORARY_FAILURE,
        # )
        # await self.send_notifications(logger)


class EnsureCronjobReenabled(StateBasedSubHandler):
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        status: kopf.Status,
        **kwargs: Any,
    ):
        disabler_job_status = None
        for key in status.keys():
            if key.endswith(DISABLE_CRONJOB_HANDLER_ID):
                disabler_job_status = status.get(key)
                break

        if disabler_job_status is None:
            logger.info("No cronjob was disabled, so can't re-enable anything.")
            return

        if disabler_job_status.get(IGNORE_CRONJOB, False):
            logger.warning("Will not attempt to re-enable any CronJobs")
            return

        async with ApiClient() as api_client:
            job_name = disabler_job_status[CRONJOB_NAME]

            batch = BatchV1beta1Api(api_client)

            jobs: V1beta1CronJobList = await batch.list_namespaced_cron_job(namespace)

            for job in jobs.items:
                if job.metadata.name == job_name:
                    update = {"spec": {"suspend": False}}
                    await batch.patch_namespaced_cron_job(job_name, namespace, update)
                    logger.info(f"Re-enabled cronjob {job_name}")
