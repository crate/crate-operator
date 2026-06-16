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

from crate.operator.handlers.handle_update_cratedb import _data_nodes_cross_zero
from crate.operator.operations import (
    check_all_master_nodes_gone,
    check_all_master_nodes_present,
    iter_node_groups,
    scale_master_statefulset,
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
