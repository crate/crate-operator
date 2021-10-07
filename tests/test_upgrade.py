# CrateDB Kubernetes Operator
# Copyright (C) 2021 Crate.IO GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import pytest

from crate.operator.create import get_statefulset_crate_command
from crate.operator.upgrade import upgrade_command


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
        node_spec={"resources": {"cpus": 1, "disk": {"count": 1}}},
        cluster_settings=None,
        has_ssl=False,
        is_master=True,
        is_data=True,
        crate_version="4.6.3",
    )
    assert f"-Cgateway.recover_after_nodes={old_quorum}" in cmd
    assert f"-Cgateway.expected_nodes={total_nodes}" in cmd

    new_cmd = upgrade_command(cmd, data_nodes)
    assert f"-Cgateway.recover_after_data_nodes={new_quorum}" in new_cmd
    assert f"-Cgateway.expected_data_nodes={data_nodes}" in new_cmd
