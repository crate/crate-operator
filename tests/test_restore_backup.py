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
from typing import Any
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
    BackupStorageProvider,
    SnapshotRestoreType,
)
from crate.operator.cratedb import connection_factory
from crate.operator.restore_backup import (
    RESTORE_CLUSTER_CONCURRENT_REBALANCE,
    RESTORE_MAX_BYTES_PER_SEC,
    RestoreBackupSubHandler,
    RestoreInternalTables,
    RestoreType,
)
from crate.operator.restore_backup_repository_data import (
    AwsBackupRepositoryData,
    AzureBackupRepositoryData,
    BackupRepositoryData,
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


@pytest.fixture
def backup_repository_data(faker):
    return {
        BackupStorageProvider.AWS: {
            "basePath": faker.uri_path(),
            "bucket": faker.domain_word(),
            "accessKeyId": faker.domain_word(),
            "secretAccessKey": faker.domain_word(),
        },
        BackupStorageProvider.AZURE_BLOB: {
            "accountKey": faker.domain_word(),
            "accountName": faker.domain_word(),
            "basePath": faker.uri_path(),
            "container": faker.domain_word(),
        },
    }


@pytest.fixture
def mock_quote_ident():

    def mock_quote_ident(value, connection):
        if value.startswith('"') and value.endswith('"'):
            return value
        return f'"{value}"'

    with mock.patch(
        "crate.operator.restore_backup.quote_ident", side_effect=mock_quote_ident
    ):
        yield


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client._send")
@mock.patch.object(RestoreBackupSubHandler, "_create_backup_repository")
@mock.patch.object(RestoreBackupSubHandler, "_ensure_snapshot_exists")
@mock.patch.object(RestoreBackupSubHandler, "_start_restore_snapshot")
@mock.patch.object(RestoreInternalTables, "remove_duplicated_tables")
@mock.patch.object(RestoreInternalTables, "cleanup_tables")
@pytest.mark.parametrize(
    "gc_enabled, backup_provider_str", [(True, "aws"), (False, None)]
)
async def test_restore_backup_aws(
    mock_cleanup_gc_tables,
    mock_remove_duplicated_tables,
    mock_start_restore_snapshot,
    mock_ensure_snapshot_exists,
    mock_create_repository,
    mock_send_notification,
    gc_enabled,
    backup_provider_str,
    faker,
    namespace,
    kopf_runner,
    api_client,
    backup_repository_data,
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    batch = BatchV1Api(api_client)
    name = faker.domain_word()
    number_of_nodes = 1

    snapshot = faker.domain_word()
    data = backup_repository_data[BackupStorageProvider.AWS]

    await core.create_namespaced_secret(
        namespace=namespace.metadata.name,
        body=V1Secret(
            data={
                "bucket": b64encode(data["bucket"]),
                "base-path": b64encode(data["basePath"]),
                "secret-access-key": b64encode(data["secretAccessKey"]),
                "access-key-id": b64encode(data["accessKeyId"]),
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
        coapi, namespace.metadata.name, name, snapshot, faker, backup_provider_str
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
    expected_repository_data = BackupRepositoryData(
        data=AwsBackupRepositoryData(**data)
    )
    if backup_provider_str:
        expected_repository_data.backup_provider = BackupStorageProvider(
            backup_provider_str
        )
    await assert_wait_for(
        True,
        mocked_coro_func_called_with,
        mock_create_repository,
        mock.call(
            mock.ANY,
            mock.ANY,
            expected_repository_data,
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
        mock_remove_duplicated_tables,
        mock.call(),
        err_msg="Did not call remove duplicate tables.",
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
        mocked_coro_func_called_with,
        mock_cleanup_gc_tables,
        mock.call(),
        err_msg="Did not call cleanup grand central tables.",
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
@mock.patch.object(RestoreBackupSubHandler, "_create_backup_repository")
@mock.patch.object(RestoreBackupSubHandler, "_ensure_snapshot_exists")
@mock.patch.object(RestoreBackupSubHandler, "_start_restore_snapshot")
@mock.patch.object(RestoreInternalTables, "remove_duplicated_tables")
@mock.patch.object(RestoreInternalTables, "cleanup_tables")
@pytest.mark.parametrize("gc_enabled", [True, False])
async def test_restore_backup_azure_blob(
    mock_cleanup_gc_tables,
    mock_remove_duplicated_tables,
    mock_start_restore_snapshot,
    mock_ensure_snapshot_exists,
    mock_create_repository,
    mock_send_notification,
    gc_enabled,
    faker,
    namespace,
    kopf_runner,
    api_client,
    backup_repository_data,
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    batch = BatchV1Api(api_client)
    name = faker.domain_word()
    number_of_nodes = 1

    snapshot = faker.domain_word()
    data = backup_repository_data[BackupStorageProvider.AZURE_BLOB]

    await core.create_namespaced_secret(
        namespace=namespace.metadata.name,
        body=V1Secret(
            data={
                "account-key": b64encode(data["accountKey"]),
                "account-name": b64encode(data["accountName"]),
                "base-path": b64encode(data["basePath"]),
                "container": b64encode(data["container"]),
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
        coapi,
        namespace.metadata.name,
        name,
        snapshot,
        faker,
        BackupStorageProvider.AZURE_BLOB.value,
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
    expected_repository_data = BackupRepositoryData(
        backup_provider=BackupStorageProvider.AZURE_BLOB,
        data=AzureBackupRepositoryData(**data),
    )
    await assert_wait_for(
        True,
        mocked_coro_func_called_with,
        mock_create_repository,
        mock.call(
            mock.ANY,
            mock.ANY,
            expected_repository_data,
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
        mock_remove_duplicated_tables,
        mock.call(),
        err_msg="Did not call remove grand central tables.",
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
        mocked_coro_func_called_with,
        mock_cleanup_gc_tables,
        mock.call(),
        err_msg="Did not call cleanup grand central tables.",
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
@mock.patch.object(
    RestoreBackupSubHandler,
    "_create_backup_repository",
    side_effect=kopf.PermanentError(
        "Backup repository is not accessible with the given credentials."
    ),
)
@mock.patch.object(RestoreBackupSubHandler, "_ensure_snapshot_exists")
@mock.patch.object(RestoreBackupSubHandler, "_start_restore_snapshot")
@mock.patch.object(RestoreInternalTables, "remove_duplicated_tables")
@mock.patch.object(RestoreInternalTables, "cleanup_tables")
async def test_restore_backup_create_repo_fails(
    mock_cleanup_gc_tables,
    mock_remove_duplicated_tables,
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
def test_get_restore_type_keyword(
    restore_type, expected_keyword, params, mock_quote_ident
):
    cursor = mock.AsyncMock()
    func_kwargs = {}
    if params:
        func_kwargs[restore_type.value] = params
    restore_keyword = RestoreType.create(
        restore_type.value, **func_kwargs
    ).get_restore_keyword(cursor=cursor)
    assert restore_keyword == expected_keyword


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backup_provider",
    [BackupStorageProvider.AWS, BackupStorageProvider.AZURE_BLOB, None],
)
async def test_create_backup_repository(
    backup_provider, faker, mock_cratedb_connection, backup_repository_data
):
    mock_cursor = mock_cratedb_connection["mock_cursor"]
    mock_cursor.fetchone.return_value = None

    repository = faker.domain_word()
    mock_logger = mock.Mock(spec=logging.Logger)
    conn_factory = connection_factory("host", "password")

    data_dict = backup_repository_data[backup_provider or BackupStorageProvider.AWS]
    data_cls = BackupRepositoryData.get_class_from_backup_provider(backup_provider)
    data = BackupRepositoryData(data=data_cls(**data_dict))
    # If the storage provider is not specified, it should default to AWS S3
    if backup_provider:
        data.backup_provider = backup_provider

    with mock.patch(
        "crate.operator.restore_backup.quote_ident", return_value=repository
    ):
        await RestoreBackupSubHandler._create_backup_repository(
            conn_factory, repository, data, mock_logger
        )

    if backup_provider == BackupStorageProvider.AZURE_BLOB:
        expected_stmt = (
            f"CREATE REPOSITORY {repository} TYPE azure "
            "WITH (max_restore_bytes_per_sec = %s, readonly = %s, "
            "key = %s, account = %s, base_path = %s, container = %s);"
        )
        expected_values_azure = (
            "240mb",
            "true",
            data_dict["accountKey"],
            data_dict["accountName"],
            data_dict["basePath"],
            data_dict["container"],
        )
    # Make sure that it uses AWS S3 as a default if the storage type isn't specified
    else:
        expected_stmt = (
            f"CREATE REPOSITORY {repository} TYPE s3 "
            "WITH (max_restore_bytes_per_sec = %s, readonly = %s, "
            "access_key = %s, base_path = %s, bucket = %s, secret_key = %s);"
        )
        expected_values_aws = (
            "240mb",
            "true",
            data_dict["accessKeyId"],
            data_dict["basePath"],
            data_dict["bucket"],
            data_dict["secretAccessKey"],
        )

    expected_values = (
        expected_values_azure
        if backup_provider == BackupStorageProvider.AZURE_BLOB
        else expected_values_aws
    )
    mock_cursor.execute.assert_has_awaits(
        [
            mock.call("SELECT * FROM sys.repositories WHERE name=%s", (repository,)),
            mock.call(expected_stmt, expected_values),
        ]
    )


def get_azure_blob_secrets(name: str) -> dict[str, Any]:
    return {
        "accountKey": {
            "secretKeyRef": {
                "key": "account-key",
                "name": config.RESTORE_BACKUP_SECRET_NAME.format(name=name),
            },
        },
        "accountName": {
            "secretKeyRef": {
                "key": "account-name",
                "name": config.RESTORE_BACKUP_SECRET_NAME.format(name=name),
            },
        },
        "basePath": {
            "secretKeyRef": {
                "key": "base-path",
                "name": config.RESTORE_BACKUP_SECRET_NAME.format(name=name),
            },
        },
        "container": {
            "secretKeyRef": {
                "key": "container",
                "name": config.RESTORE_BACKUP_SECRET_NAME.format(name=name),
            },
        },
    }


def get_aws_secrets(name: str) -> dict[str, Any]:
    return {
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
    }


async def patch_cluster_spec(
    coapi: CustomObjectsApi,
    namespace: str,
    name: str,
    snapshot: str,
    faker,
    backup_provider_str: str | None = None,
):
    restore_snapshot_spec = {
        "snapshot": snapshot,
        "type": "all",
    }
    if backup_provider_str == BackupStorageProvider.AZURE_BLOB.value:
        restore_snapshot_spec.update(get_azure_blob_secrets(name))
    else:
        restore_snapshot_spec.update(get_aws_secrets(name))

    if backup_provider_str:
        restore_snapshot_spec["backupProvider"] = backup_provider_str

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


@pytest.fixture
def replace_gc_tables_data(faker, mock_cratedb_connection):
    repository = faker.domain_word()
    snapshot = faker.domain_word()
    table_a = faker.domain_word()
    table_b = faker.domain_word()
    tables = [table_a, table_b]
    tables_with_schema = [f"gc.{t}" for t in tables]

    mock_cursor = mock_cratedb_connection["mock_cursor"]
    mock_logger = mock.Mock(spec=logging.Logger)
    conn_factory = connection_factory("host", "password")

    gc_tables_cls = RestoreInternalTables(
        conn_factory, repository, snapshot, mock_logger
    )

    return (
        repository,
        snapshot,
        tables,
        tables_with_schema,
        mock_cursor,
        gc_tables_cls,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "gc_enabled, type_all", [(True, False), (False, True), (True, True), (False, False)]
)
async def test_replace_gc_tables(
    gc_enabled, type_all, replace_gc_tables_data, mock_quote_ident
):
    repository, snapshot, tables, tables_with_schema, mock_cursor, gc_tables_cls = (
        replace_gc_tables_data
    )

    fetch_response = [(t,) for t in tables_with_schema]
    mock_cursor.fetchall.return_value = fetch_response if gc_enabled else []
    tables_param = None if type_all else tables_with_schema

    await gc_tables_cls.remove_duplicated_tables(tables_param)

    # Verify query for restore types ALL and TABLES
    if type_all:
        stmts = [
            mock.call(
                "WITH tables AS ("
                "  SELECT unnest(tables) AS t "
                "  FROM sys.snapshots "
                "  WHERE repository=%s AND name=%s"
                ") "
                "SELECT * FROM tables WHERE t LIKE 'gc.%%';",
                (repository, snapshot),
            ),
        ]
    else:
        stmts = [
            mock.call(
                "WITH tables AS ("
                "  SELECT unnest(tables) AS t "
                "  FROM sys.snapshots "
                "  WHERE repository=%s AND name=%s"
                ") "
                "SELECT * FROM tables WHERE t IN "
                f"('{tables_with_schema[0]}','{tables_with_schema[1]}');",
                (repository, snapshot),
            ),
        ]

    if gc_enabled:
        stmts.append(
            mock.call(f'ALTER TABLE "gc"."{tables[0]}" RENAME TO "{tables[0]}_temp";')
        )
        stmts.append(
            mock.call(f'ALTER TABLE "gc"."{tables[1]}" RENAME TO "{tables[1]}_temp";')
        )

    mock_cursor.execute.assert_has_awaits(stmts)
    assert gc_tables_cls.gc_tables_renamed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("gc_tables_renamed", [True, False])
async def test_restore_gc_tables(
    gc_tables_renamed, replace_gc_tables_data, mock_quote_ident
):
    _, _, tables, tables_with_schema, mock_cursor, gc_tables_cls = (
        replace_gc_tables_data
    )

    gc_tables_cls.gc_tables_renamed = gc_tables_renamed
    gc_tables_cls.gc_tables = tables_with_schema

    await gc_tables_cls.restore_tables()

    if not gc_tables_renamed:
        mock_cursor.execute.assert_not_awaited()
    else:
        mock_cursor.execute.assert_has_awaits(
            [
                mock.call(
                    f'ALTER TABLE "gc"."{tables[0]}_temp" RENAME TO "{tables[0]}";'
                ),
                mock.call(
                    f'ALTER TABLE "gc"."{tables[1]}_temp" RENAME TO "{tables[1]}";'
                ),
            ]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("gc_tables_renamed", [True, False])
async def test_cleanup_gc_tables(
    gc_tables_renamed, replace_gc_tables_data, mock_quote_ident
):
    _, _, tables, tables_with_schema, mock_cursor, gc_tables_cls = (
        replace_gc_tables_data
    )

    gc_tables_cls.gc_tables_renamed = gc_tables_renamed
    gc_tables_cls.gc_tables = tables_with_schema

    await gc_tables_cls.cleanup_tables()

    if not gc_tables_renamed:
        mock_cursor.execute.assert_not_awaited()
    else:
        mock_cursor.execute.assert_has_awaits(
            [
                mock.call(f'DROP TABLE "gc"."{tables[0]}_temp";'),
                mock.call(f'DROP TABLE "gc"."{tables[1]}_temp";'),
            ]
        )
