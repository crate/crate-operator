from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import kopf
import pytest

from crate.operator.operations import _node_groups_to_restart, restart_cluster
from crate.operator.webhooks import WebhookAction


@pytest.mark.asyncio
@patch("crate.operator.operations.reset_cluster_setting", new_callable=AsyncMock)
@patch("crate.operator.operations.set_cluster_setting", new_callable=AsyncMock)
@patch("crate.operator.operations._get_connection_factory", new_callable=AsyncMock)
@patch("crate.operator.operations.GlobalApiClient")
@patch("crate.operator.operations.get_pods_in_statefulset", new_callable=AsyncMock)
@patch("crate.operator.operations.get_pods_in_cluster", new_callable=AsyncMock)
@patch(
    "crate.operator.operations.send_operation_progress_notification",
    new_callable=AsyncMock,
)
@patch("crate.operator.operations.get_system_user_password", new_callable=AsyncMock)
@patch("crate.operator.operations.get_host", new_callable=AsyncMock)
@patch("crate.operator.operations.is_cluster_healthy", new_callable=AsyncMock)
async def test_restart_cluster_calls_set_cluster_setting(
    mock_is_cluster_healthy,
    _mock_get_host,
    _mock_get_system_user_password,
    _mock_send_notification,
    mock_get_pods_in_cluster,
    mock_get_pods_in_statefulset,
    _mock_global_api_client,
    mock_get_connection_factory,
    mock_set_cluster_setting,
    mock_reset_cluster_setting,
):
    # mock 2 pods in statefulset
    mock_get_pods_in_statefulset.return_value = [
        {"uid": "uid1", "name": "crate-data-hot-123-0"},
        {"uid": "uid2", "name": "crate-data-hot-123-1"},
    ]
    mock_get_pods_in_cluster.side_effect = [
        # first call: both pods exist > triggers pod deletion
        (["uid1", "uid2"], ["crate-data-hot-123-0", "crate-data-hot-123-1"]),
        # second call: uid1 deleted but pod name still present > triggers check
        # for cluster health
        (["uid2"], ["crate-data-hot-123-0", "crate-data-hot-123-1"]),
    ]

    # first unhealthy, second healthy
    mock_is_cluster_healthy.side_effect = [False, True]

    core = MagicMock()
    core.delete_namespaced_pod = AsyncMock()
    logger = MagicMock()
    patch_obj = kopf.Patch()
    status: dict[str, Any] = {}

    old = {
        "spec": {
            "nodes": {
                "data": [
                    {
                        "name": "hot",
                        "replicas": 2,
                        "resources": {
                            "requests": {"cpu": 0.5, "memory": "1Gi"},
                            "limits": {"cpu": 0.5, "memory": "1Gi"},
                            "heapRatio": 0.25,
                            "disk": {
                                "storageClass": "default",
                                "size": "16GiB",
                                "count": 1,
                            },
                        },
                    },
                ],
            }
        }
    }

    # simulate a first restart (new_primaries)
    with pytest.raises(kopf.TemporaryError, match="Waiting for pod"):
        await restart_cluster(
            core=core,
            namespace="abc",
            name="test-cluster",
            old=old,
            body=old,
            logger=logger,
            patch=patch_obj,
            status=status,
            action=WebhookAction.UPGRADE,
        )

    # simulate pod gone, cluster unhealthy > triggers allocation=all
    status["pendingPods"] = [{"uid": "uid1", "name": "crate-data-hot-123-0"}]
    with pytest.raises(kopf.TemporaryError, match="Cluster is not healthy yet"):
        await restart_cluster(
            core=core,
            namespace="abc",
            name="test-cluster",
            old=old,
            body=old,
            logger=logger,
            patch=patch_obj,
            status=status,
            action=WebhookAction.UPGRADE,
        )

    # assert both cluster settings were applied
    mock_set_cluster_setting.assert_any_await(
        mock_get_connection_factory.return_value,
        logger,
        setting="cluster.routing.allocation.enable",
        value="new_primaries",
        mode="PERSISTENT",
    )
    mock_reset_cluster_setting.assert_awaited_once_with(
        mock_get_connection_factory.return_value,
        logger,
        setting="cluster.routing.allocation.enable",
    )


def _res(cpu):
    return {
        "resources": {
            "limits": {"cpu": cpu, "memory": "1Gi"},
            "requests": {"cpu": cpu, "memory": "1Gi"},
        }
    }


def _spec(master_cpu=2, hot_cpu=2, cold_cpu=None):
    data = [{"name": "hot", "replicas": 1, **_res(hot_cpu)}]
    if cold_cpu is not None:
        data.append({"name": "cold", "replicas": 1, **_res(cold_cpu)})
    nodes = {"master": {"replicas": 3, **_res(master_cpu)}, "data": data}
    return {"spec": {"nodes": nodes}}


def test_change_compute_restarts_only_the_changed_master_group():
    # Only the master CPU changed -> only the master group restarts (the data
    # node must not be rolled).
    groups = _node_groups_to_restart(
        _spec(master_cpu=2, hot_cpu=2),
        _spec(master_cpu=3, hot_cpu=2),
        WebhookAction.CHANGE_COMPUTE,
    )
    assert [g.name for g in groups] == ["master"]


def test_change_compute_restarts_only_the_changed_data_group():
    groups = _node_groups_to_restart(
        _spec(master_cpu=2, hot_cpu=2),
        _spec(master_cpu=2, hot_cpu=3),
        WebhookAction.CHANGE_COMPUTE,
    )
    assert [g.name for g in groups] == ["hot"]


def test_change_compute_restarts_all_changed_groups_masters_first():
    # master + hot + cold all change at once -> all restart, masters first,
    # serialised by the caller's one-at-a-time loop.
    groups = _node_groups_to_restart(
        _spec(master_cpu=2, hot_cpu=2, cold_cpu=2),
        _spec(master_cpu=3, hot_cpu=3, cold_cpu=3),
        WebhookAction.CHANGE_COMPUTE,
    )
    assert [g.name for g in groups] == ["master", "hot", "cold"]


def test_change_compute_with_no_compute_change_restarts_nothing():
    groups = _node_groups_to_restart(_spec(), _spec(), WebhookAction.CHANGE_COMPUTE)
    assert groups == []


def test_upgrade_restarts_all_groups_regardless_of_change():
    # An upgrade must roll every node (new image), even with no resource change.
    groups = _node_groups_to_restart(_spec(), _spec(), WebhookAction.UPGRADE)
    assert [g.name for g in groups] == ["master", "hot"]
