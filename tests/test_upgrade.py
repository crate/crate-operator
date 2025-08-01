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
import asyncio
import itertools
from unittest import mock

import kopf
import pytest
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi

from crate.operator.constants import (
    API_GROUP,
    INTERNAL_TABLES,
    RESOURCE_CRATEDB,
    CloudProvider,
)
from crate.operator.cratedb import connection_factory
from crate.operator.create import get_statefulset_crate_command
from crate.operator.upgrade import (
    check_reindexing_tables,
    recreate_internal_tables,
    upgrade_command_data_nodes,
    upgrade_command_global_jwt_config,
    upgrade_command_hostname_and_zone,
    upgrade_command_jwt_auth,
)
from crate.operator.webhooks import (
    WebhookAction,
    WebhookEvent,
    WebhookOperation,
    WebhookStatus,
)

from .utils import (
    CRATE_VERSION,
    DEFAULT_TIMEOUT,
    assert_wait_for,
    cluster_routing_allocation_enable_equals,
    create_test_sys_jobs_table,
    do_pod_ids_exist,
    do_pods_exist,
    is_cluster_healthy,
    is_kopf_handler_finished,
    start_cluster,
    was_notification_sent,
)


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client._send")
async def test_upgrade_cluster(
    mock_send_notification, faker, namespace, kopf_runner, api_client
):
    version_from = "5.2.3"
    version_to = CRATE_VERSION
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(name, namespace, core, coapi, 3, version_from)

    await assert_wait_for(
        True,
        do_pods_exist,
        core,
        namespace.metadata.name,
        {
            f"crate-data-hot-{name}-0",
            f"crate-data-hot-{name}-1",
            f"crate-data-hot-{name}-2",
        },
    )

    conn_factory = connection_factory(host, password)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        conn_factory,
        3,
        err_msg="Cluster wasn't healthy",
        timeout=DEFAULT_TIMEOUT,
    )

    await create_test_sys_jobs_table(conn_factory)

    pods = await core.list_namespaced_pod(namespace=namespace.metadata.name)
    original_pods = {p.metadata.uid for p in pods.items}
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "replace",
                "path": "/spec/cluster/version",
                "value": version_to,
            },
        ],
    )

    await assert_wait_for(
        True,
        cluster_routing_allocation_enable_equals,
        connection_factory(host, password),
        "new_primaries",
        err_msg="Cluster routing allocation setting has not been updated",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await assert_wait_for(
        False,
        do_pod_ids_exist,
        core,
        namespace.metadata.name,
        original_pods,
        timeout=DEFAULT_TIMEOUT * 15,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.upgrade",
        err_msg="Upgrade has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.restart",
        err_msg="Restart has not finished",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        3,
        err_msg="Cluster wasn't healthy",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        cluster_routing_allocation_enable_equals,
        connection_factory(host, password),
        "all",
        err_msg="Cluster routing allocation setting has not been updated",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    notification_success_call = mock.call(
        WebhookEvent.UPGRADE,
        WebhookStatus.SUCCESS,
        namespace.metadata.name,
        name,
        upgrade_data=mock.ANY,
        unsafe=mock.ANY,
        logger=mock.ANY,
    )
    assert await was_notification_sent(
        mock_send_notification=mock_send_notification, call=notification_success_call
    ), "A success notification was expected but was not sent"


@pytest.mark.parametrize(
    "total_nodes, old_quorum, data_nodes, new_quorum",
    [(1, 1, 1, 1), (3, 2, 2, 2), (8, 5, 5, 3), (31, 16, 27, 14)],
)
def test_upgrade_sts_command(total_nodes, old_quorum, data_nodes, new_quorum):
    cmd = get_statefulset_crate_command(
        namespace="some-namespace",
        name="cluster1",
        master_nodes=[f"node-{i}" for i in range(total_nodes - data_nodes)],
        total_nodes_count=total_nodes,
        data_nodes_count=data_nodes,
        crate_node_name_prefix="node-",
        cluster_name="my-cluster",
        node_name="node",
        node_spec={"resources": {"limits": {"cpu": 1}, "disk": {"count": 1}}},
        cluster_settings=None,
        has_ssl=False,
        is_master=True,
        is_data=True,
        crate_version="4.6.3",
        cloud_settings={},
    )
    assert f"-Cgateway.recover_after_nodes={old_quorum}" in cmd
    assert f"-Cgateway.expected_nodes={total_nodes}" in cmd

    new_cmd = upgrade_command_data_nodes(cmd, data_nodes)
    assert f"-Cgateway.recover_after_data_nodes={new_quorum}" in new_cmd
    assert f"-Cgateway.expected_data_nodes={data_nodes}" in new_cmd


def test_upgrade_sts_command_with_jwt():
    cmd = get_statefulset_crate_command(
        namespace="some-namespace",
        name="cluster1",
        master_nodes=["node-1"],
        total_nodes_count=3,
        data_nodes_count=2,
        crate_node_name_prefix="node-",
        cluster_name="my-cluster",
        node_name="node",
        node_spec={"resources": {"limits": {"cpu": 1}, "disk": {"count": 1}}},
        cluster_settings=None,
        has_ssl=False,
        is_master=True,
        is_data=True,
        crate_version="5.6.5",
        cloud_settings={},
    )
    assert "-Cauth.host_based.config.98.method=jwt" not in cmd
    assert "-Cauth.host_based.config.98.protocol=http" not in cmd
    assert "-Cauth.host_based.config.98.ssl=on" not in cmd

    new_cmd = upgrade_command_jwt_auth(cmd)
    assert "-Cauth.host_based.config.98.method=jwt" in new_cmd
    assert "-Cauth.host_based.config.98.protocol=http" in new_cmd
    assert "-Cauth.host_based.config.98.ssl=on" in new_cmd


@pytest.mark.parametrize(
    "provider", [CloudProvider.AWS, CloudProvider.AZURE, CloudProvider.GCP]
)
def test_upgrade_sts_command_hostname_zone(provider):
    cmd = [
        "-Cnode.name=data-hot-$(hostname | rev | cut -d- -f1 | rev)",
    ]
    if provider == CloudProvider.GCP:
        cmd.append(
            "-Cnode.attr.zone=$(curl -s 'http://123.123.123.123/computeMetadata/v1/instance/zone' "  # noqa
            "-H 'Metadata-Flavor: Google' | rev | cut -d '/' -f 1 | rev)",
        )
    with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", provider.value):
        assert "-Cnode.name=data-hot-$(hostname | rev | cut -d- -f1 | rev)" in cmd
        assert "-Cnode.name=data-hot-$(hostname | awk -F- '{print $NF}')" not in cmd
        if provider == CloudProvider.GCP:
            assert any(
                item.startswith("-Cnode.attr.zone=")
                and "rev | cut -d '/' -f 1 | rev" in item
                for item in cmd
            ), "initial GCP cmd does not contain rev | cut"

        new_cmd = upgrade_command_hostname_and_zone(cmd)
        assert (
            "-Cnode.name=data-hot-$(hostname | rev | cut -d- -f1 | rev)" not in new_cmd
        )
        assert "-Cnode.name=data-hot-$(hostname | awk -F- '{print $NF}')" in new_cmd

        if provider == CloudProvider.GCP:
            assert any(
                item.startswith("-Cnode.attr.zone=")
                and "awk -F'/' '{print $NF}'" in item
                for item in new_cmd
            ), "replacement in GCP cmd did not occur as expected"


def test_upgrade_sts_command_with_global_jwt_config():
    cluster_name = "cluster1"
    cloud_settings = {"jwkUrl": "https://my-cratedb-api.cloud/api/v2/meta/jwk/"}
    cmd = get_statefulset_crate_command(
        namespace="some-namespace",
        name=cluster_name,
        master_nodes=["node-1"],
        total_nodes_count=3,
        data_nodes_count=2,
        crate_node_name_prefix="node-",
        cluster_name="my-cluster",
        node_name="node",
        node_spec={"resources": {"limits": {"cpu": 1}, "disk": {"count": 1}}},
        cluster_settings=None,
        has_ssl=False,
        is_master=True,
        is_data=True,
        crate_version="5.6.5",
        cloud_settings=cloud_settings,
    )
    assert (
        "-Cauth.host_based.jwt.iss=https://my-cratedb-api.cloud/api/v2/meta/jwk/"
        not in cmd
    )
    assert "-Cauth.host_based.jwt.aud=cluster1" not in cmd

    new_cmd = upgrade_command_global_jwt_config(cmd, cluster_name, cloud_settings)
    assert (
        "-Cauth.host_based.jwt.iss=https://my-cratedb-api.cloud/api/v2/meta/jwk/"
        in new_cmd
    )
    assert "-Cauth.host_based.jwt.aud=cluster1" in new_cmd


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.upgrade.update_statefulset")
@mock.patch("crate.operator.webhooks.webhook_client._send")
async def test_upgrade_rollback_on_failure(
    mock_send_notification,
    mock_update_sts,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    version_from = "5.2.3"
    version_to = "5.7.10"
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    # simulate failure inside one of the update calls
    mock_update_sts.side_effect = kopf.PermanentError(
        "Simulated StatefulSet update failure"
    )

    host, password = await start_cluster(name, namespace, core, coapi, 1, version_from)

    await assert_wait_for(
        True,
        do_pods_exist,
        core,
        namespace.metadata.name,
        {
            f"crate-data-hot-{name}-0",
        },
    )

    conn_factory = connection_factory(host, password)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        conn_factory,
        1,
        err_msg="Cluster wasn't healthy",
        timeout=DEFAULT_TIMEOUT,
    )

    await create_test_sys_jobs_table(conn_factory)

    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "replace",
                "path": "/spec/cluster/version",
                "value": version_to,
            },
        ],
    )

    await assert_wait_for(
        True,
        cluster_routing_allocation_enable_equals,
        connection_factory(host, password),
        "new_primaries",
        err_msg="Cluster routing allocation setting has not been updated",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    # wait for upgrade handler to fail and rollback to run
    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        "operator.cloud.crate.io/cluster_update.final_rollback",
        err_msg="Rollback handler was not triggered",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    # check that the version was set back
    crd = await coapi.get_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
    )
    assert crd["spec"]["cluster"]["version"] == version_from

    # check that the failure webhook was sent
    failure_call = mock.call(
        WebhookEvent.FEEDBACK,
        WebhookStatus.FAILURE,
        namespace.metadata.name,
        name,
        feedback_data={
            "message": "Simulated StatefulSet update failure",
            "operation": WebhookOperation.UPDATE.value,
            "action": WebhookAction.UPGRADE.value,
        },
        unsafe=mock.ANY,
        logger=mock.ANY,
    )
    assert await was_notification_sent(
        mock_send_notification=mock_send_notification,
        call=failure_call,
    ), "Expected failure notification was not sent"


@pytest.mark.k8s
@pytest.mark.asyncio
@mock.patch("crate.operator.webhooks.webhook_client._send")
async def test_upgrade_blocked_if_rollback_annotation_set(
    mock_send_notification,
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    version_from = "5.2.3"
    version_to = "5.7.10"
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()

    host, password = await start_cluster(name, namespace, core, coapi, 1, version_from)

    await assert_wait_for(
        True,
        do_pods_exist,
        core,
        namespace.metadata.name,
        {
            f"crate-data-hot-{name}-0",
        },
    )

    conn_factory = connection_factory(host, password)

    await assert_wait_for(
        True,
        is_cluster_healthy,
        conn_factory,
        1,
        err_msg="Cluster wasn't healthy",
        timeout=DEFAULT_TIMEOUT,
    )

    # manually simulate that a rollback already happened
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "add",
                "path": "/metadata/annotations/operator.cloud.crate.io~1rollback-upgrade",  # noqa
                "value": "true",
            }
        ],
    )

    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "replace",
                "path": "/spec/cluster/version",
                "value": version_to,
            },
        ],
    )

    # wait a bit to give kopf time to potentially start the upgrade (if it wrongly did)
    await asyncio.sleep(5)

    # check that the version is still unchanged
    crd = await coapi.get_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
    )
    # version should reflect the patch, but no upgrade logic must have executed
    assert crd["spec"]["cluster"]["version"] == version_to
    # and the annotation should have been set back to false
    assert (
        crd["metadata"]["annotations"].get("operator.cloud.crate.io/rollback-upgrade")
        == "false"
    )

    # confirm no success or failure webhook for the upgrade was sent
    upgrade_calls = [
        call
        for call in mock_send_notification.call_args_list
        if (
            (
                data := call.kwargs.get("feedback_data")
                or call.kwargs.get("info_data")
                or call.kwargs.get("upgrade_data")
            )
            and data.get("operation") == WebhookOperation.UPDATE
            and data.get("action") == WebhookAction.UPGRADE
        )
    ]

    assert not upgrade_calls, f"Unexpected upgrade webhooks triggered: {upgrade_calls}"


@pytest.mark.asyncio
@mock.patch("crate.operator.upgrade.get_host", new_callable=mock.AsyncMock)
@mock.patch(
    "crate.operator.upgrade.get_system_user_password", new_callable=mock.AsyncMock
)
async def test_check_reindexing_tables_detects_tables(
    mock_get_pwd,
    mock_get_host,
    mock_cratedb_connection,
):
    mock_get_host.return_value = "localhost"
    mock_get_pwd.return_value = "pwd"

    mock_cursor = mock_cratedb_connection["mock_cursor"]
    mock_cursor.fetchall.return_value = [("table1",), ("table2",)]

    body = mock.MagicMock()
    old = {"spec": {"cluster": {"version": "5.10.10"}}}
    body.spec = {"cluster": {"version": "6.0.0"}}

    with pytest.raises(kopf.PermanentError, match="Tables need re-indexing"):
        await check_reindexing_tables(
            core=mock.MagicMock(),
            namespace="ns",
            name="my-cluster",
            body=body,
            old=old,
            logger=mock.MagicMock(),
        )

    mock_cursor.execute.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old_version,new_version,expected_lucene",
    [
        ("4.8.4", "5.0.0", "7.%"),  # upgrade 4 -> 5
        ("5.10.10", "6.0.0", "8.%"),  # upgrade 5 -> 6
        ("6.7.8", "7.0.0", "9.%"),  # future upgrade 6 -> 7
    ],
)
@mock.patch("crate.operator.upgrade.get_host", new_callable=mock.AsyncMock)
@mock.patch(
    "crate.operator.upgrade.get_system_user_password", new_callable=mock.AsyncMock
)
async def test_check_reindexing_tables_dynamic_lucene_version(
    mock_get_pwd,
    mock_get_host,
    old_version,
    new_version,
    expected_lucene,
    mock_cratedb_connection,
):
    mock_get_host.return_value = "localhost"
    mock_get_pwd.return_value = "pwd"

    mock_cursor = mock_cratedb_connection["mock_cursor"]
    mock_cursor.fetchall.return_value = [("table1",)]

    body = mock.MagicMock()
    old = {"spec": {"cluster": {"version": old_version}}}
    body.spec = {"cluster": {"version": new_version}}

    with pytest.raises(kopf.PermanentError, match="Tables need re-indexing"):
        await check_reindexing_tables(
            core=mock.MagicMock(),
            namespace="ns",
            name="my-cluster",
            body=body,
            old=old,
            logger=mock.MagicMock(),
        )

    # Verify that correct Lucene version was used in the query
    executed_query = mock_cursor.execute.call_args[0][0]
    assert expected_lucene in executed_query


@pytest.mark.asyncio
@mock.patch("crate.operator.upgrade.get_host", new_callable=mock.AsyncMock)
@mock.patch(
    "crate.operator.upgrade.get_system_user_password", new_callable=mock.AsyncMock
)
async def test_recreate_internal_tables_creates_tmp_table(
    mock_get_password,
    mock_get_host,
    mock_cratedb_connection,
):
    mock_get_host.return_value = "localhost"
    mock_get_password.return_value = "pwd"

    mock_cursor = mock_cratedb_connection["mock_cursor"]

    ddl = """
        CREATE TABLE IF NOT EXISTS "gc"."alembic_version" (
           "version_num" TEXT NOT NULL
        )
        CLUSTERED INTO 1 SHARDS
        WITH (
           column_policy = 'strict',
           number_of_replicas = '0-1'
        )
    """
    mock_cursor.fetchone.side_effect = itertools.cycle(
        [
            (1,),  # table exists
            (ddl,),  # SHOW CREATE TABLE
        ]
    )

    core = mock.MagicMock()
    logger = mock.MagicMock()
    body = mock.MagicMock()
    old = {"spec": {"cluster": {"version": "5.10.10"}}}
    body.spec = {"cluster": {"version": "6.0.0"}}

    await recreate_internal_tables(core, "ns", "my-cluster", body, old, logger)

    executed_sql = " ".join(call.args[0] for call in mock_cursor.execute.call_args_list)

    for full_table in INTERNAL_TABLES:
        schema, table = full_table.split(".")
        tmp_table = f"tmp_{table}"

        # Ensure all operations were called for this table
        assert f'CREATE TABLE IF NOT EXISTS "{schema}"."{tmp_table}"' in executed_sql
        assert f'INSERT INTO "{schema}"."{tmp_table}"' in executed_sql
        assert f'ALTER CLUSTER SWAP TABLE "{schema}"."{tmp_table}"' in executed_sql
        assert f'DROP TABLE IF EXISTS "{schema}"."{tmp_table}"' in executed_sql

    # Ensure no CREATE on the original table
    assert 'CREATE TABLE IF NOT EXISTS "gc"."alembic_version"' not in executed_sql


@pytest.mark.asyncio
@mock.patch("crate.operator.upgrade.get_host", new_callable=mock.AsyncMock)
@mock.patch(
    "crate.operator.upgrade.get_system_user_password", new_callable=mock.AsyncMock
)
async def test_recreate_internal_tables_skips_missing_tables(
    mock_get_password,
    mock_get_host,
    mock_cratedb_connection,
):
    mock_get_host.return_value = "localhost"
    mock_get_password.return_value = "pwd"

    mock_cursor = mock_cratedb_connection["mock_cursor"]

    # Returns (0,) which means that the table doesn't exist, repeated for all tables
    mock_cursor.fetchone.side_effect = itertools.repeat((0,))

    core = mock.MagicMock()
    logger = mock.MagicMock()
    body = mock.MagicMock()
    old = {"spec": {"cluster": {"version": "5.10.10"}}}
    body.spec = {"cluster": {"version": "6.0.0"}}

    await recreate_internal_tables(core, "ns", "my-cluster", body, old, logger)

    executed_sql = " ".join(call.args[0] for call in mock_cursor.execute.call_args_list)

    for full_table in INTERNAL_TABLES:
        schema, table = full_table.split(".")
        tmp_table = f"tmp_{table}"

        # Ensure none of these operations were called
        assert (
            f'CREATE TABLE IF NOT EXISTS "{schema}"."{tmp_table}"' not in executed_sql
        )
        assert f'INSERT INTO "{schema}"."{tmp_table}"' not in executed_sql
        assert f'ALTER CLUSTER SWAP TABLE "{schema}"."{tmp_table}"' not in executed_sql
        assert f'DROP TABLE IF EXISTS "{schema}"."{tmp_table}"' not in executed_sql
