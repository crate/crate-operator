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

import pytest
from kubernetes_asyncio.client import AppsV1Api, BatchV1beta1Api, V1beta1CronJob

from crate.operator.backup import create_backups
from crate.operator.constants import (
    BACKUP_METRICS_DEPLOYMENT_NAME,
    LABEL_COMPONENT,
    LABEL_NAME,
)

from .utils import assert_wait_for


@pytest.mark.k8s
@pytest.mark.asyncio
class TestBackup:
    async def does_cronjob_exist(
        self, batchv1_beta1: BatchV1beta1Api, namespace: str, name: str
    ) -> bool:
        cjs = await batchv1_beta1.list_namespaced_cron_job(namespace=namespace)
        return name in (cj.metadata.name for cj in cjs.items)

    async def get_cronjob(
        selfself, batchv1_beta1: BatchV1beta1Api, namespace: str, name: str
    ) -> V1beta1CronJob:
        return await batchv1_beta1.read_namespaced_cron_job(
            namespace=namespace, name=name
        )

    async def does_deployment_exist(
        self, apps: AppsV1Api, namespace: str, name: str
    ) -> bool:
        deployments = await apps.list_namespaced_deployment(namespace=namespace)
        return name in (d.metadata.name for d in deployments.items)

    async def test_create(self, faker, namespace, api_client):
        apps = AppsV1Api(api_client)
        batchv1_beta1 = BatchV1beta1Api(api_client)
        name = faker.domain_word()

        backups_spec = {
            "aws": {
                "accessKeyId": {
                    "secretKeyRef": {
                        "key": faker.domain_word(),
                        "name": faker.domain_word(),
                    },
                },
                "basePath": faker.uri_path() + "/",
                "cron": "1 2 3 4 5",
                "region": {
                    "secretKeyRef": {
                        "key": faker.domain_word(),
                        "name": faker.domain_word(),
                    },
                },
                "bucket": {
                    "secretKeyRef": {
                        "key": faker.domain_word(),
                        "name": faker.domain_word(),
                    },
                },
                "secretAccessKey": {
                    "secretKeyRef": {
                        "key": faker.domain_word(),
                        "name": faker.domain_word(),
                    },
                },
            },
        }

        await create_backups(
            None,
            namespace.metadata.name,
            name,
            {LABEL_COMPONENT: "backup", LABEL_NAME: name},
            12345,
            23456,
            backups_spec,
            None,
            True,
            logging.getLogger(__name__),
        )
        await assert_wait_for(
            True,
            self.does_cronjob_exist,
            batchv1_beta1,
            namespace.metadata.name,
            f"create-snapshot-{name}",
        )
        await assert_wait_for(
            True,
            self.does_deployment_exist,
            apps,
            namespace.metadata.name,
            BACKUP_METRICS_DEPLOYMENT_NAME.format(name=name),
        )

    async def test_create_with_custom_backup_location(
        self, faker, namespace, api_client
    ):
        batchv1_beta1 = BatchV1beta1Api(api_client)
        name = faker.domain_word()

        backups_spec = {
            "aws": {
                "accessKeyId": {
                    "secretKeyRef": {
                        "key": faker.domain_word(),
                        "name": faker.domain_word(),
                    },
                },
                "basePath": faker.uri_path() + "/",
                "cron": "1 2 3 4 5",
                "region": {
                    "secretKeyRef": {
                        "key": faker.domain_word(),
                        "name": faker.domain_word(),
                    },
                },
                "bucket": {
                    "secretKeyRef": {
                        "key": faker.domain_word(),
                        "name": faker.domain_word(),
                    },
                },
                "secretAccessKey": {
                    "secretKeyRef": {
                        "key": faker.domain_word(),
                        "name": faker.domain_word(),
                    },
                },
                "endpointUrl": {
                    "secretKeyRef": {
                        "key": faker.domain_word(),
                        "name": faker.domain_word(),
                        "optional": True,
                    },
                },
            },
        }

        await create_backups(
            None,
            namespace.metadata.name,
            name,
            {LABEL_COMPONENT: "backup", LABEL_NAME: name},
            12345,
            23456,
            backups_spec,
            None,
            True,
            logging.getLogger(__name__),
        )
        await assert_wait_for(
            True,
            self.does_cronjob_exist,
            batchv1_beta1,
            namespace.metadata.name,
            f"create-snapshot-{name}",
        )
        job = await self.get_cronjob(
            batchv1_beta1, namespace.metadata.name, f"create-snapshot-{name}"
        )
        env_vars = [
            env.name
            for env in job.spec.job_template.spec.template.spec.containers[0].env
        ]
        assert "S3_ENDPOINT_URL" in env_vars

    async def test_not_enabled(self, faker, namespace, api_client):
        name = faker.domain_word()
        apps = AppsV1Api(api_client)
        batchv1_beta1 = BatchV1beta1Api(api_client)

        await create_backups(
            None,
            namespace.metadata.name,
            name,
            {},
            12345,
            23456,
            {},
            None,
            True,
            logging.getLogger(__name__),
        )
        assert (
            await self.does_cronjob_exist(
                batchv1_beta1, namespace.metadata.name, f"create-snapshot-{name}"
            )
            is False
        )
        assert (
            await self.does_deployment_exist(
                apps,
                namespace.metadata.name,
                BACKUP_METRICS_DEPLOYMENT_NAME.format(name=name),
            )
            is False
        )
