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

from crate.operator.operations import iter_node_groups


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
