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

import contextlib
from unittest import mock

import kopf
import pytest
from kubernetes_asyncio.client import ApiException

from crate.operator.constants import BACKUP_METRICS_DEPLOYMENT_NAME
from crate.operator.handlers.handle_update_cratedb import _data_nodes_cross_zero
from crate.operator.operations import (
    check_all_master_nodes_gone,
    check_all_master_nodes_present,
    iter_node_groups,
    scale_backup_metrics_deployment_if_present,
    scale_master_statefulset,
    suspend_or_start_cluster,
    validate_node_spec,
)


def _data_diff(old_replicas, new_replicas):
    """A ``spec.nodes.data`` diff for a single group changing replica count."""
    return kopf.Diff(
        [
            kopf.DiffItem(
                kopf.DiffOperation.CHANGE,
                ("spec", "nodes", "data"),
                [{"name": "hot", "replicas": old_replicas}],
                [{"name": "hot", "replicas": new_replicas}],
            )
        ]
    )


class TestIterNodeGroups:
    def test_masters_yielded_first(self):
        nodes = {
            "master": {"replicas": 3},
            "data": [{"name": "hot", "replicas": 7}],
        }

        groups = iter_node_groups(nodes)

        assert [g.name for g in groups] == ["master", "hot"]
        assert groups[0].is_master is True
        assert groups[1].is_master is False

    def test_legacy_cluster_without_masters(self):
        # No ``master`` key: only the data groups are enumerated, and nothing
        # is treated as a dedicated master.
        nodes = {"data": [{"name": "hot", "replicas": 3}]}

        groups = iter_node_groups(nodes)

        assert [g.name for g in groups] == ["hot"]
        assert all(g.is_master is False for g in groups)

    def test_statefulset_names_and_node_prefixes(self):
        nodes = {
            "master": {"replicas": 3},
            "data": [{"name": "hot", "replicas": 7}],
        }

        master, hot = iter_node_groups(nodes)

        assert master.node_name_prefix == "master"
        assert master.statefulset_name("my-cluster") == "crate-master-my-cluster"
        assert hot.node_name_prefix == "data-hot"
        assert hot.statefulset_name("my-cluster") == "crate-data-hot-my-cluster"

    def test_spec_is_passed_through(self):
        # The masters' spec is a single object (no ``name`` of its own); the
        # group's logical name is synthesized, the spec passed through as-is.
        master_spec = {"replicas": 3, "resources": {"limits": {"cpu": 16}}}
        nodes = {"master": master_spec, "data": [{"name": "hot", "replicas": 7}]}

        groups = iter_node_groups(nodes)

        assert groups[0].spec is master_spec
        assert "name" not in groups[0].spec


class TestDataNodesCrossZero:
    def test_suspend_is_detected(self):
        assert _data_nodes_cross_zero(_data_diff(3, 0)) is True

    def test_resume_is_detected(self):
        assert _data_nodes_cross_zero(_data_diff(0, 3)) is True

    def test_plain_scale_is_not_suspend_or_resume(self):
        assert _data_nodes_cross_zero(_data_diff(3, 2)) is False

    def test_master_only_diff_is_not_detected(self):
        # A standalone master.replicas change with no data transition: this is
        # exactly the case the dispatch guardrail must reject.
        diff = kopf.Diff(
            [
                kopf.DiffItem(
                    kopf.DiffOperation.CHANGE,
                    ("spec", "nodes", "master", "replicas"),
                    3,
                    0,
                )
            ]
        )
        assert _data_nodes_cross_zero(diff) is False


def _statefulset(spec_replicas=None, ready_replicas=None):
    sts = mock.Mock()
    sts.spec.replicas = spec_replicas
    sts.status.ready_replicas = ready_replicas
    return sts


class TestCheckAllMasterNodesPresent:
    @pytest.mark.asyncio
    async def test_waits_until_all_masters_ready(self):
        apps = mock.Mock()
        apps.read_namespaced_stateful_set = mock.AsyncMock(
            return_value=_statefulset(ready_replicas=1)
        )

        with pytest.raises(kopf.TemporaryError):
            await check_all_master_nodes_present(apps, "ns", "c", 3, mock.Mock())

    @pytest.mark.asyncio
    async def test_passes_when_all_masters_ready(self):
        apps = mock.Mock()
        apps.read_namespaced_stateful_set = mock.AsyncMock(
            return_value=_statefulset(ready_replicas=3)
        )

        await check_all_master_nodes_present(apps, "ns", "c", 3, mock.Mock())

    @pytest.mark.asyncio
    async def test_treats_missing_ready_count_as_zero(self):
        apps = mock.Mock()
        apps.read_namespaced_stateful_set = mock.AsyncMock(
            return_value=_statefulset(ready_replicas=None)
        )

        with pytest.raises(kopf.TemporaryError):
            await check_all_master_nodes_present(apps, "ns", "c", 1, mock.Mock())


class TestCheckAllMasterNodesGone:
    @pytest.mark.asyncio
    @mock.patch(
        "crate.operator.operations.get_pods_in_statefulset",
        new_callable=mock.AsyncMock,
    )
    async def test_raises_while_master_pods_present(self, mock_get_pods):
        mock_get_pods.return_value = [{"uid": "x", "name": "crate-master-c-0"}]

        with pytest.raises(kopf.TemporaryError):
            await check_all_master_nodes_gone(mock.Mock(), "ns", "c")

    @pytest.mark.asyncio
    @mock.patch(
        "crate.operator.operations.get_pods_in_statefulset",
        new_callable=mock.AsyncMock,
    )
    async def test_passes_when_no_master_pods(self, mock_get_pods):
        mock_get_pods.return_value = []

        await check_all_master_nodes_gone(mock.Mock(), "ns", "c")


class TestScaleMasterStatefulset:
    @pytest.mark.asyncio
    @mock.patch(
        "crate.operator.operations.update_statefulset_replicas",
        new_callable=mock.AsyncMock,
    )
    async def test_scales_when_replica_count_differs(self, mock_update):
        apps = mock.Mock()
        apps.read_namespaced_stateful_set = mock.AsyncMock(
            return_value=_statefulset(spec_replicas=0)
        )

        await scale_master_statefulset(apps, "ns", "c", 3, mock.Mock())

        mock_update.assert_awaited_once()

    @pytest.mark.asyncio
    @mock.patch(
        "crate.operator.operations.update_statefulset_replicas",
        new_callable=mock.AsyncMock,
    )
    async def test_noop_when_already_at_target(self, mock_update):
        apps = mock.Mock()
        apps.read_namespaced_stateful_set = mock.AsyncMock(
            return_value=_statefulset(spec_replicas=3)
        )

        await scale_master_statefulset(apps, "ns", "c", 3, mock.Mock())

        mock_update.assert_not_awaited()


class TestValidateNodeSpec:
    def test_accepts_data_only_cluster(self):
        validate_node_spec({"data": [{"name": "hot", "replicas": 3}]}, mock.Mock())

    def test_accepts_dedicated_masters_odd_ge_three(self):
        for replicas in (3, 5, 7):
            validate_node_spec(
                {
                    "master": {"replicas": replicas},
                    "data": [{"name": "hot", "replicas": 1}],
                },
                mock.Mock(),
            )

    def test_rejects_missing_data_group(self):
        with pytest.raises(kopf.PermanentError, match="at least one data node group"):
            validate_node_spec({"master": {"replicas": 3}}, mock.Mock())

    def test_rejects_empty_data_group(self):
        with pytest.raises(kopf.PermanentError, match="at least one data node group"):
            validate_node_spec({"data": []}, mock.Mock())

    def test_rejects_even_master_replicas(self):
        with pytest.raises(kopf.PermanentError, match="odd number >= 3"):
            validate_node_spec(
                {
                    "master": {"replicas": 4},
                    "data": [{"name": "hot", "replicas": 1}],
                },
                mock.Mock(),
            )

    def test_rejects_fewer_than_three_masters(self):
        with pytest.raises(kopf.PermanentError, match="odd number >= 3"):
            validate_node_spec(
                {
                    "master": {"replicas": 1},
                    "data": [{"name": "hot", "replicas": 1}],
                },
                mock.Mock(),
            )


def _suspend_resume_patches(order, master_replicas=3, data_groups=None):
    """
    Patch every collaborator ``suspend_or_start_cluster`` calls so the function
    runs end to end against mocks, recording the ordering-relevant calls into
    ``order``. Returns an ``ExitStack`` of the active patches.
    """
    if data_groups is None:
        data_groups = [{"name": "hot", "replicas": 0}]
    cratedb = {
        "spec": {
            "nodes": {"master": {"replicas": master_replicas}, "data": data_groups}
        },
        "metadata": {},
    }

    async def _record(label, *args, **kwargs):
        order.append(label)

    async def _scale_master(apps, namespace, name, target, logger):
        order.append(f"master_scale:{target}")

    async def _scale_data(apps, namespace, sts_name, sts, new_replicas):
        order.append(f"data_scale:{new_replicas}")

    p = mock.patch
    M = "crate.operator.operations."
    stack = contextlib.ExitStack()
    stack.enter_context(
        p(M + "get_cratedb_resource", new=mock.AsyncMock(return_value=cratedb))
    )
    stack.enter_context(
        p(M + "scale_master_statefulset", new=mock.AsyncMock(side_effect=_scale_master))
    )
    stack.enter_context(
        p(
            M + "check_all_master_nodes_present",
            new=mock.AsyncMock(
                side_effect=lambda *a, **k: order.append("masters_present")
            ),
        )
    )
    stack.enter_context(
        p(
            M + "check_all_master_nodes_gone",
            new=mock.AsyncMock(
                side_effect=lambda *a, **k: order.append("masters_gone")
            ),
        )
    )
    stack.enter_context(
        p(
            M + "check_all_data_nodes_gone",
            new=mock.AsyncMock(side_effect=lambda *a, **k: order.append("data_gone")),
        )
    )
    stack.enter_context(
        p(
            M + "check_cluster_healthy",
            new=mock.AsyncMock(
                side_effect=lambda *a, **k: order.append("health_check")
            ),
        )
    )
    stack.enter_context(
        p(
            M + "update_statefulset_replicas",
            new=mock.AsyncMock(side_effect=_scale_data),
        )
    )
    stack.enter_context(
        p(M + "is_service_present", new=mock.AsyncMock(return_value=True))
    )
    stack.enter_context(
        p(M + "is_lb_service_ready", new=mock.AsyncMock(return_value=True))
    )
    for fn in (
        "recreate_services",
        "send_operation_progress_notification",
        "suspend_or_start_grand_central",
        "delete_lb_service",
        "update_deployment_replicas",
        "_get_connection_factory",
        "check_all_data_nodes_present",
    ):
        stack.enter_context(p(M + fn, new=mock.AsyncMock()))
    return stack


def _data_scaling_diff(old_replicas, new_replicas):
    return kopf.Diff(
        [
            kopf.DiffItem(
                kopf.DiffOperation.CHANGE,
                ("0", "replicas"),
                old_replicas,
                new_replicas,
            )
        ]
    )


class TestSuspendResumeOrdering:
    @pytest.mark.asyncio
    async def test_resume_brings_masters_up_before_data(self):
        # On resume the masters must form a quorum *before* the data nodes are
        # scaled back up, otherwise the cluster can't recover.
        order: list = []
        old = {"spec": {"nodes": {"data": [{"name": "hot", "replicas": 0}]}}}
        apps = mock.Mock()
        apps.read_namespaced_stateful_set = mock.AsyncMock(
            return_value=_statefulset(spec_replicas=0)
        )

        with _suspend_resume_patches(order):
            await suspend_or_start_cluster(
                apps,
                mock.Mock(),
                "ns",
                "c",
                old,
                _data_scaling_diff(0, 3),
                False,
                mock.Mock(),
            )

        assert "master_scale:3" in order
        assert "data_scale:3" in order
        assert order.index("master_scale:3") < order.index("data_scale:3")
        assert order.index("masters_present") < order.index("data_scale:3")

    @pytest.mark.asyncio
    async def test_suspend_scales_masters_down_after_data_is_gone(self):
        # On suspend the masters go to zero only *after* every data node is gone
        # -- never out from under running data nodes (the deadlock-fix order).
        order: list = []
        old = {"spec": {"nodes": {"data": [{"name": "hot", "replicas": 3}]}}}
        apps = mock.Mock()
        apps.read_namespaced_stateful_set = mock.AsyncMock(
            return_value=_statefulset(spec_replicas=3)
        )

        with _suspend_resume_patches(
            order, data_groups=[{"name": "hot", "replicas": 0}]
        ):
            await suspend_or_start_cluster(
                apps,
                mock.Mock(),
                "ns",
                "c",
                old,
                _data_scaling_diff(3, 0),
                False,
                mock.Mock(),
            )

        assert "data_gone" in order
        assert "master_scale:0" in order
        assert order.index("data_scale:0") < order.index("master_scale:0")
        assert order.index("data_gone") < order.index("master_scale:0")
        assert order.index("master_scale:0") < order.index("masters_gone")


class TestScaleBackupMetricsIfPresent:
    @pytest.mark.asyncio
    async def test_scales_when_deployment_exists(self):
        apps = mock.Mock()
        with mock.patch(
            "crate.operator.operations.update_deployment_replicas",
            new=mock.AsyncMock(),
        ) as mock_update:
            await scale_backup_metrics_deployment_if_present(
                apps, "ns", "c", 1, mock.Mock()
            )
        mock_update.assert_awaited_once_with(
            apps, "ns", BACKUP_METRICS_DEPLOYMENT_NAME.format(name="c"), 1
        )

    @pytest.mark.asyncio
    async def test_missing_deployment_is_a_noop(self):
        apps = mock.Mock()
        with mock.patch(
            "crate.operator.operations.update_deployment_replicas",
            new=mock.AsyncMock(side_effect=ApiException(status=404)),
        ):
            # should not raise.
            await scale_backup_metrics_deployment_if_present(
                apps, "ns", "c", 0, mock.Mock()
            )

    @pytest.mark.asyncio
    async def test_other_api_errors_propagate(self):
        apps = mock.Mock()
        with mock.patch(
            "crate.operator.operations.update_deployment_replicas",
            new=mock.AsyncMock(side_effect=ApiException(status=409)),
        ):
            with pytest.raises(ApiException):
                await scale_backup_metrics_deployment_if_present(
                    apps, "ns", "c", 1, mock.Mock()
                )


class TestSuspendResumeWithoutBackups:
    @pytest.mark.asyncio
    async def test_resume_does_not_wedge_when_no_backup_metrics_deployment(self):
        order: list = []
        old = {"spec": {"nodes": {"data": [{"name": "hot", "replicas": 0}]}}}
        apps = mock.Mock()
        apps.read_namespaced_stateful_set = mock.AsyncMock(
            return_value=_statefulset(spec_replicas=0)
        )

        with _suspend_resume_patches(order):
            with mock.patch(
                "crate.operator.operations.update_deployment_replicas",
                new=mock.AsyncMock(side_effect=ApiException(status=404)),
            ):
                # should complete without raising despite the missing deployment.
                await suspend_or_start_cluster(
                    apps,
                    mock.Mock(),
                    "ns",
                    "c",
                    old,
                    _data_scaling_diff(0, 3),
                    True,
                    mock.Mock(),
                )

        assert "data_scale:3" in order
