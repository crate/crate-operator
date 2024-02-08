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
from unittest import mock

import kopf
import pytest
from kubernetes_asyncio.client import (
    BatchV1Api,
    CoreV1Api,
    CustomObjectsApi,
    V1ObjectMeta,
    V1Secret,
)

from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    KOPF_STATE_STORE_PREFIX,
    RESOURCE_CRATEDB,
    SnapshotRestoreType,
)
from crate.operator.cratedb import connection_factory
from crate.operator.restore_backup import (
    RESTORE_CLUSTER_CONCURRENT_REBALANCE,
    RESTORE_MAX_BYTES_PER_SEC,
    RestoreType,
)
from crate.operator.utils.formatting import b64encode
from crate.operator.webhooks import (
    WebhookAction,
    WebhookEvent,
    WebhookOperation,
    WebhookStatus,
)
from tests.utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    cluster_setting_equals,
    create_test_sys_jobs_table,
    does_backup_metrics_pod_exist,
    is_cluster_healthy,
    is_cronjob_enabled,
    is_kopf_handler_finished,
    mocked_coro_func_called_with,
    start_backup_metrics,
    start_cluster,
    was_notification_sent,
)


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client._send")
@mock.patch(
    "crate.operator.restore_backup.RestoreBackupSubHandler._create_backup_repository"
)
@mock.patch(
    "crate.operator.restore_backup.RestoreBackupSubHandler._ensure_snapshot_exists"
)
@mock.patch(
    "crate.operator.restore_backup.RestoreBackupSubHandler._start_restore_snapshot"
)
async def test_restore_backup(
    mock_start_restore_snapshot,
    mock_ensure_snapshot_exists,
    mock_create_repository,
    mock_send_notification,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    batch = BatchV1Api(api_client)
    name = faker.domain_word()
    number_of_nodes = 1

    bucket = faker.domain_word()
    base_path = faker.uri_path()
    secret_access_key = faker.domain_word()
    access_key_id = faker.domain_word()
    snapshot = faker.domain_word()

    await core.create_namespaced_secret(
        namespace=namespace.metadata.name,
        body=V1Secret(
            data={
                "bucket": b64encode(bucket),
                "base-path": b64encode(base_path),
                "secret-access-key": b64encode(secret_access_key),
                "access-key-id": b64encode(access_key_id),
            },
            metadata=V1ObjectMeta(
                name=config.RESTORE_BACKUP_SECRET_NAME.format(name=name)
            ),
            type="Opaque",
        ),
    )

    host, password = await start_cluster(
        name,
        namespace,
        core,
        coapi,
        number_of_nodes,
    )

    conn_factory = connection_factory(host, password)
    await create_test_sys_jobs_table(conn_factory)

    await start_backup_metrics(name, namespace, faker)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        number_of_nodes,
        err_msg="Cluster wasn't healthy after 5 minutes.",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await patch_cluster_spec(coapi, namespace.metadata.name, name, snapshot, faker)

    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        mock.call(
            WebhookEvent.FEEDBACK,
            WebhookStatus.IN_PROGRESS,
            namespace.metadata.name,
            name,
            feedback_data={
                "message": "Preparing to restore data from snapshot.",
                "operation": WebhookOperation.UPDATE.value,
                "action": WebhookAction.RESTORE_SNAPSHOT.value,
            },
            unsafe=mock.ANY,
            logger=mock.ANY,
        ),
        err_msg="In progress notification has not been sent.",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        True,
        cluster_setting_equals,
        connection_factory(host, password),
        "indices.recovery.max_bytes_per_sec",
        RESTORE_MAX_BYTES_PER_SEC,
        err_msg="Cluster setting `max_bytes_per_sec` has not been updated.",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        True,
        cluster_setting_equals,
        connection_factory(host, password),
        "cluster.routing.allocation.cluster_concurrent_rebalance",
        RESTORE_CLUSTER_CONCURRENT_REBALANCE,
        err_msg="Cluster setting `cluster_concurrent_rebalance` has not been updated.",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        False,
        does_backup_metrics_pod_exist,
        core,
        name,
        namespace.metadata.name,
        err_msg="Backup metrics has not been scaled down.",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        True,
        mocked_coro_func_called_with,
        mock_create_repository,
        mock.call(
            mock.ANY,
            mock.ANY,
            {
                "basePath": base_path,
                "bucket": bucket,
                "accessKeyId": access_key_id,
                "secretAccessKey": secret_access_key,
            },
            mock.ANY,
        ),
        err_msg="Expected create repository call not found.",
        timeout=DEFAULT_TIMEOUT * 2,
    )
    await assert_wait_for(
        True,
        mocked_coro_func_called_with,
        mock_ensure_snapshot_exists,
        mock.call(
            mock.ANY,
            mock.ANY,
            snapshot,
            mock.ANY,
        ),
        err_msg="Did not call ensure snapshot exists.",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        True,
        mocked_coro_func_called_with,
        mock_start_restore_snapshot,
        mock.call(mock.ANY, mock.ANY, snapshot, "all", mock.ANY, [], [], []),
        err_msg="Did not call start restore snapshot.",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        True,
        does_backup_metrics_pod_exist,
        core,
        name,
        namespace.metadata.name,
        err_msg="Backup metrics has not been scaled up again.",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        False,
        does_credentials_secret_exist,
        core,
        name,
        namespace.metadata.name,
        err_msg="Secret has not been deleted.",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        mock.call(
            WebhookEvent.FEEDBACK,
            WebhookStatus.SUCCESS,
            namespace.metadata.name,
            name,
            feedback_data={
                "message": "The snapshot has been restored successfully.",
                "operation": WebhookOperation.UPDATE.value,
                "action": WebhookAction.RESTORE_SNAPSHOT.value,
            },
            unsafe=mock.ANY,
            logger=mock.ANY,
        ),
        err_msg="Success notification has not been sent.",
        timeout=DEFAULT_TIMEOUT * 3,
    )
    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_restore/spec.cluster.restoreSnapshot",
        err_msg="Restore handler has not finished",
        timeout=DEFAULT_TIMEOUT * 3,
    )

    await assert_wait_for(
        True,
        is_cronjob_enabled,
        batch,
        namespace.metadata.name,
        f"create-snapshot-{name}",
        err_msg="The backup cronjob is disabled",
        timeout=DEFAULT_TIMEOUT,
    )


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client._send")
@mock.patch(
    "crate.operator.restore_backup.RestoreBackupSubHandler._create_backup_repository",
    side_effect=kopf.PermanentError(
        "Backup repository is not accessible with the given credentials."
    ),
)
@mock.patch(
    "crate.operator.restore_backup.RestoreBackupSubHandler._ensure_snapshot_exists"
)
@mock.patch(
    "crate.operator.restore_backup.RestoreBackupSubHandler._start_restore_snapshot"
)
async def test_restore_backup_create_repo_fails(
    mock_start_restore_snapshot,
    mock_ensure_snapshot_exists,
    mock_create_repository,
    mock_send_notification,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    snapshot = faker.domain_word()
    number_of_nodes = 1

    await core.create_namespaced_secret(
        namespace=namespace.metadata.name,
        body=V1Secret(
            data={
                "bucket": b64encode(faker.domain_word()),
                "base-path": b64encode(faker.uri_path()),
                "secret-access-key": b64encode(faker.domain_word()),
                "access-key-id": b64encode(faker.domain_word()),
            },
            metadata=V1ObjectMeta(
                name=config.RESTORE_BACKUP_SECRET_NAME.format(name=name)
            ),
            type="Opaque",
        ),
    )

    host, password = await start_cluster(
        name,
        namespace,
        core,
        coapi,
        number_of_nodes,
    )

    conn_factory = connection_factory(host, password)
    await create_test_sys_jobs_table(conn_factory)

    await start_backup_metrics(name, namespace, faker)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        number_of_nodes,
        err_msg="Cluster wasn't healthy after 5 minutes.",
        timeout=DEFAULT_TIMEOUT * 5,
    )
    await patch_cluster_spec(coapi, namespace.metadata.name, name, snapshot, faker)

    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        mock.call(
            WebhookEvent.FEEDBACK,
            WebhookStatus.IN_PROGRESS,
            namespace.metadata.name,
            name,
            feedback_data={
                "message": "Preparing to restore data from snapshot.",
                "operation": WebhookOperation.UPDATE.value,
                "action": WebhookAction.RESTORE_SNAPSHOT.value,
            },
            unsafe=mock.ANY,
            logger=mock.ANY,
        ),
        err_msg="In progress notification has not been sent.",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        True,
        was_notification_sent,
        mock_send_notification,
        mock.call(
            WebhookEvent.FEEDBACK,
            WebhookStatus.FAILURE,
            namespace.metadata.name,
            name,
            feedback_data={
                "message": (
                    "Backup repository is not accessible with the given credentials."
                ),
                "operation": WebhookOperation.UPDATE.value,
                "action": WebhookAction.RESTORE_SNAPSHOT.value,
            },
            unsafe=mock.ANY,
            logger=mock.ANY,
        ),
        err_msg="Exception notification has not been sent.",
        timeout=DEFAULT_TIMEOUT * 3,
    )
    await assert_wait_for(
        True,
        does_backup_metrics_pod_exist,
        core,
        name,
        namespace.metadata.name,
        err_msg="Backup metrics has not been scaled up again.",
        timeout=DEFAULT_TIMEOUT,
    )
    await assert_wait_for(
        False,
        does_credentials_secret_exist,
        core,
        name,
        namespace.metadata.name,
        err_msg="Secret has not been deleted.",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_restore/spec.cluster.restoreSnapshot",
        err_msg="Restore handler has not finished",
        timeout=DEFAULT_TIMEOUT * 3,
    )


@pytest.mark.parametrize(
    "restore_type, expected_keyword, params",
    [
        (SnapshotRestoreType.ALL, "ALL", None),
        (SnapshotRestoreType.METADATA, "METADATA", None),
        (
            SnapshotRestoreType.SECTIONS,
            "TABLES,USERS,PRIVILEGES",
            ("tables", "users", "privileges"),
        ),
        (SnapshotRestoreType.TABLES, "TABLE table1,table2", ("table1", "table2")),
        (
            SnapshotRestoreType.PARTITIONS,
            (
                "TABLE table1 PARTITION (col1=val1,col2=val2),"
                "TABLE table2 PARTITION (col3=val3)"
            ),
            [
                {
                    "table_ident": "table1",
                    "columns": [
                        {"name": "col1", "value": "val1"},
                        {"name": "col2", "value": "val2"},
                    ],
                },
                {
                    "table_ident": "table2",
                    "columns": [{"name": "col3", "value": "val3"}],
                },
            ],
        ),
    ],
)
def test_get_restore_type_keyword(restore_type, expected_keyword, params):
    func_kwargs = {}
    if params:
        func_kwargs[restore_type.value] = params
    restore_keyword = RestoreType.create(
        restore_type.value, **func_kwargs
    ).get_restore_keyword()
    assert restore_keyword == expected_keyword


async def patch_cluster_spec(
    coapi: CustomObjectsApi, namespace: str, name: str, snapshot: str, faker
):
    restore_snapshot_spec = {
        "accessKeyId": {
            "secretKeyRef": {
                "key": "access-key-id",
                "name": config.RESTORE_BACKUP_SECRET_NAME.format(name=name),
            },
        },
        "basePath": {
            "secretKeyRef": {
                "key": "base-path",
                "name": config.RESTORE_BACKUP_SECRET_NAME.format(name=name),
            },
        },
        "bucket": {
            "secretKeyRef": {
                "key": "bucket",
                "name": config.RESTORE_BACKUP_SECRET_NAME.format(name=name),
            },
        },
        "secretAccessKey": {
            "secretKeyRef": {
                "key": "secret-access-key",
                "name": config.RESTORE_BACKUP_SECRET_NAME.format(name=name),
            },
        },
        "snapshot": snapshot,
        "type": "all",
    }
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace,
        name=name,
        body=[
            {
                "op": "add",
                "path": "/spec/cluster/restoreSnapshot",
                "value": restore_snapshot_spec,
            },
        ],
    )


async def does_credentials_secret_exist(
    core: CoreV1Api, namespace: str, name: str
) -> bool:
    secrets = await core.list_namespaced_secret(namespace)
    return config.RESTORE_BACKUP_SECRET_NAME.format(name=name) in (
        s.metadata.name for s in secrets.items
    )
