import asyncio

import pytest
from kubernetes_asyncio.client import AppsV1Api, BatchV1beta1Api

from crate.operator.backup import create_backups
from crate.operator.constants import LABEL_COMPONENT, LABEL_NAME

from .utils import assert_wait_for


@pytest.mark.k8s
@pytest.mark.asyncio
class TestBackup:
    async def does_cronjob_exist(
        self, batchv1_beta1: BatchV1beta1Api, namespace: str, name: str
    ) -> bool:
        cjs = await batchv1_beta1.list_namespaced_cron_job(namespace=namespace)
        return name in (cj.metadata.name for cj in cjs.items)

    async def does_deployment_exist(
        self, apps: AppsV1Api, namespace: str, name: str
    ) -> bool:
        deployments = await apps.list_namespaced_deployment(namespace=namespace)
        return name in (d.metadata.name for d in deployments.items)

    async def test_create(self, faker, namespace):
        apps = AppsV1Api()
        batchv1_beta1 = BatchV1beta1Api()
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
        cronjob, deployment = await asyncio.gather(
            *create_backups(
                apps,
                batchv1_beta1,
                None,
                namespace.metadata.name,
                name,
                {LABEL_COMPONENT: "backup", LABEL_NAME: name},
                12345,
                23456,
                backups_spec,
                None,
                True,
            )
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
            f"backup-metrics-{name}",
        )

    async def test_not_enabled(self, faker, namespace):
        apps = AppsV1Api()
        batchv1_beta1 = BatchV1beta1Api()
        name = faker.domain_word()

        ret = await asyncio.gather(
            *create_backups(
                apps,
                batchv1_beta1,
                None,
                namespace.metadata.name,
                name,
                {},
                12345,
                23456,
                {},
                None,
                True,
            )
        )
        assert ret == []
