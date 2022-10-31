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

    async def test_change_schedule(self, faker, namespace, api_client):
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
