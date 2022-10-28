from time import sleep

import pytest

from crate.operator.constants import API_GROUP, RESOURCE_CRATEDB
from .utils import start_cluster, assert_wait_for, is_kopf_handler_finished, DEFAULT_TIMEOUT
from kubernetes_asyncio.client import (
    CoreV1Api,
    CustomObjectsApi,
    BatchV1beta1Api
)

@pytest.mark.k8s
@pytest.mark.asyncio
async def test_update_backups_schedule(
    faker,
    namespace,
    kopf_runner,
    api_client,
):

    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
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

    host, password = await start_cluster(name, namespace, core, coapi, 1, backups_spec=backups_spec)

    body_changes = [
        {
            "op": "replace",
            "path": "/spec/backups/aws/cron",
            "value": "39 * * * *",
        },
    ]

    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=body_changes,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.backup_schedule_update",
        err_msg="Compute change has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.after_cluster_update",
        err_msg="Compute change has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    # sleep(300)
