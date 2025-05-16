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
    DEFAULT_BACKUP_STORAGE_TYPE,
    KOPF_STATE_STORE_PREFIX,
    RESOURCE_CRATEDB,
    SnapshotRestoreType,
)
from crate.operator.cratedb import connection_factory
from crate.operator.restore_backup import (
    RESTORE_CLUSTER_CONCURRENT_REBALANCE,
    RESTORE_MAX_BYTES_PER_SEC,
    BackupRepositoryData,
    RestoreBackupSubHandler,
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
@pytest.mark.parametrize(
    "gc_enabled, storage_type", [(True, "s3"), (False, None), (False, "azure")]
)
async def test_restore_backup(
    mock_start_restore_snapshot,
    mock_ensure_snapshot_exists,
    mock_create_repository,
    mock_send_notification,
    gc_enabled,
    storage_type,
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

    if gc_enabled:
        grand_central_spec = {
            "backendEnabled": True,
            "backendImage": "cloud.registry.cr8.net/crate/grand-central:latest",
            "apiUrl": "https://my-cratedb-api.cloud/",
            "jwkUrl": "https://my-cratedb-api.cloud/api/v2/meta/jwk/",
        }
        additional_cluster_spec = {
            "externalDNS": "my-crate-cluster.aks1.eastus.azure.cratedb-dev.net.",
        }

    host, password = await start_cluster(
        name,
        namespace,
        core,
        coapi,
        number_of_nodes,
        additional_cluster_spec=(additional_cluster_spec if gc_enabled else None),
        grand_central_spec=(grand_central_spec if gc_enabled else None),
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

    await patch_cluster_spec(
        coapi, namespace.metadata.name, name, snapshot, faker, storage_type
    )

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
    backup_repository_data = BackupRepositoryData(
        basePath=base_path,
        bucket=bucket,
        accessKeyId=access_key_id,
        secretAccessKey=secret_access_key,
    )
    if storage_type:
        backup_repository_data.storage_type = storage_type
    await assert_wait_for(
        True,
        mocked_coro_func_called_with,
        mock_create_repository,
        mock.call(
            mock.ANY,
            mock.ANY,
            backup_repository_data,
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
            ["tables", "users", "privileges"],
        ),
        (SnapshotRestoreType.TABLES, 'TABLE "doc"."table1"', ["doc.table1"]),
        (
            SnapshotRestoreType.TABLES,
            'TABLE "doc"."table1","doc"."my-table","doc"."my-table-name_!@^"',
            ['"doc"."table1"', "doc.my-table", "doc.my-table-name_!@^"],
        ),
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
    cursor = mock.AsyncMock()

    def mock_quote_ident(value, connection):
        if value.startswith('"') and value.endswith('"'):
            return value
        return f'"{value}"'

    with mock.patch(
        "crate.operator.restore_backup.quote_ident", side_effect=mock_quote_ident
    ):
        func_kwargs = {}
        if params:
            func_kwargs[restore_type.value] = params
        restore_keyword = RestoreType.create(
            restore_type.value, **func_kwargs
        ).get_restore_keyword(cursor=cursor)
        assert restore_keyword == expected_keyword


@pytest.fixture
def mock_cratedb_connection():
    mock_cursor_cm = mock.MagicMock()
    mock_cursor = mock.AsyncMock()
    mock_cursor_cm.return_value.__aenter__.return_value = mock_cursor
    mock_cursor_cm.return_value.__aexit__.return_value = None
    mock_cursor.fetchone.return_value = None

    mock_conn_cm = mock.MagicMock()
    mock_conn = mock.AsyncMock()
    mock_conn_cm.return_value.__aenter__.return_value = mock_conn
    mock_conn_cm.return_value.__aexit__.return_value = None
    mock_conn.cursor = mock_cursor_cm

    return {
        "mock_conn_context_manager": mock_conn_cm,
        "mock_conn": mock_conn,
        "mock_cursor_context_manager": mock_cursor_cm,
        "mock_cursor": mock_cursor,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("storage_type", ["s3", "azure", None])
async def test_create_backup_repository(storage_type, faker, mock_cratedb_connection):
    mock_conn_cm = mock_cratedb_connection["mock_conn_context_manager"]
    mock_cursor = mock_cratedb_connection["mock_cursor"]

    bucket = faker.domain_word()
    base_path = faker.uri_path()
    secret_access_key = faker.domain_word()
    access_key_id = faker.domain_word()
    repository = faker.domain_word()
    mock_logger = mock.Mock(spec=logging.Logger)

    data = BackupRepositoryData(
        basePath=base_path,
        bucket=bucket,
        accessKeyId=access_key_id,
        secretAccessKey=secret_access_key,
    )
    if storage_type:
        data.storage_type = storage_type

    with mock.patch(
        "crate.operator.restore_backup.quote_ident", return_value=repository
    ):
        await RestoreBackupSubHandler._create_backup_repository(
            mock_conn_cm, repository, data, mock_logger
        )

    # Make sure that it uses the default value if the storage type isn't specified
    expected_type = storage_type or DEFAULT_BACKUP_STORAGE_TYPE
    mock_cursor.execute.assert_has_awaits(
        [
            mock.call("SELECT * FROM sys.repositories WHERE name=%s", (repository,)),
            mock.call(
                f"CREATE REPOSITORY {repository} type %s with (access_key = %s, "
                "secret_key = %s, bucket = %s, base_path = %s, readonly=true, "
                "max_restore_bytes_per_sec = '240mb')",
                (expected_type, access_key_id, secret_access_key, bucket, base_path),
            ),
        ]
    )


async def patch_cluster_spec(
    coapi: CustomObjectsApi,
    namespace: str,
    name: str,
    snapshot: str,
    faker,
    storage_type: str | None = None,
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
    if storage_type:
        restore_snapshot_spec["storageType"] = storage_type

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
