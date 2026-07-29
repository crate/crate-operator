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
from typing import Any, Dict
from unittest import mock

import pytest

from crate.operator.constants import CloudProvider
from crate.operator.create import (
    get_statefulset_crate_command,
    get_statefulset_crate_env,
    get_topology_spread,
)

OTHER_PROVIDERS = [
    None,
    CloudProvider.AWS,
    CloudProvider.AZURE,
    CloudProvider.GCP,
    CloudProvider.OPENSHIFT,
]

METADATA_URL = "http://169.254.169.254/latest/meta-data/placement/availability-zone"

#: The whole zone snippet. AWS reads the same path, so tests that assert a
#: provider does *not* use it have to compare the snippet, not just the URL.
ZONE_SETTING = f"-Cnode.attr.zone=$(curl -s '{METADATA_URL}')"


def crate_command(provider):
    with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", provider):
        return get_statefulset_crate_command(
            namespace="some-namespace",
            name="cluster1",
            master_nodes=["node-0", "node-1", "node-2"],
            total_nodes_count=3,
            data_nodes_count=3,
            crate_node_name_prefix="node-",
            cluster_name="my-cluster",
            node_name="node",
            node_spec={
                "resources": {
                    "requests": {"cpu": 1},
                    "limits": {"cpu": 1},
                    "disk": {"count": 1},
                }
            },
            cluster_settings=None,
            has_ssl=False,
            is_master=True,
            is_data=True,
            crate_version="4.6.3",
            cloud_settings={},
        )


class TestStackitNetworkSettings:
    """
    STACKIT pods get carrier-grade NAT addresses, which CrateDB's default
    ``_site_`` resolution rejects, so we bind explicitly (crate/cloud#2926).
    """

    def test_binds_all_interfaces_and_publishes_pod_ip(self):
        cmd = crate_command(CloudProvider.STACKIT)

        assert "-Cnetwork.host=0.0.0.0" in cmd
        assert "-Cnetwork.publish_host=$(POD_IP)" in cmd

    @pytest.mark.parametrize("provider", OTHER_PROVIDERS)
    def test_other_providers_keep_the_cratedb_default(self, provider):
        cmd = crate_command(provider)

        assert not any(arg.startswith("-Cnetwork.") for arg in cmd)

    @pytest.mark.parametrize("settings_key", ["cluster_settings", "node_settings"])
    def test_settings_can_override_the_publish_host(self, settings_key):
        """
        Cluster and node settings are applied after the provider defaults, so an
        operator can still pin the publish host by hand.
        """
        overrides = {"network.publish_host": "_eth0_"}
        node_spec: Dict[str, Any] = {
            "resources": {
                "requests": {"cpu": 1},
                "limits": {"cpu": 1},
                "disk": {"count": 1},
            }
        }
        if settings_key == "node_settings":
            node_spec["settings"] = overrides
            cluster_settings = None
        else:
            cluster_settings = overrides

        with mock.patch(
            "crate.operator.create.config.CLOUD_PROVIDER", CloudProvider.STACKIT
        ):
            cmd = get_statefulset_crate_command(
                namespace="some-namespace",
                name="cluster1",
                master_nodes=["node-0", "node-1", "node-2"],
                total_nodes_count=3,
                data_nodes_count=3,
                crate_node_name_prefix="node-",
                cluster_name="my-cluster",
                node_name="node",
                node_spec=node_spec,
                cluster_settings=cluster_settings,
                has_ssl=False,
                is_master=True,
                is_data=True,
                crate_version="4.6.3",
                cloud_settings={},
            )

        assert "-Cnetwork.publish_host=_eth0_" in cmd
        assert "-Cnetwork.publish_host=$(POD_IP)" not in cmd

    def test_local_trust_rule_is_unchanged(self):
        """
        Binding on 0.0.0.0 must not widen the ``crate`` superuser's trust rule.
        """
        cmd = crate_command(CloudProvider.STACKIT)

        assert "-Cauth.host_based.config.0.address=_local_" in cmd
        assert "-Cauth.host_based.config.0.method=trust" in cmd
        assert "-Cauth.host_based.config.99.method=password" in cmd


class TestStackitZoneAttribute:
    """
    ``node.attr.zone`` comes from the EC2-compatible metadata API that OpenStack
    serves alongside its own. It answers with the bare zone name (``eu01-3``), so
    there is nothing to parse - unlike AWS, no IMDSv2 token is needed either.
    """

    def test_reads_the_zone_from_the_metadata_service(self):
        cmd = crate_command(CloudProvider.STACKIT)

        assert ZONE_SETTING in cmd

    @pytest.mark.parametrize("provider", OTHER_PROVIDERS)
    def test_other_providers_are_untouched(self, provider):
        cmd = crate_command(provider)

        assert ZONE_SETTING not in cmd


class TestStackitTopologySpread:
    def test_pods_are_spread_over_three_zones(self, faker):
        """
        ``min_domains=3`` with ``DoNotSchedule`` means the region has to offer three
        zones, otherwise pods stay Pending instead of merely being unspread.
        STACKIT ``eu01`` does (crate/cloud#3036).
        """
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", False):
            with mock.patch(
                "crate.operator.create.config.CLOUD_PROVIDER", CloudProvider.STACKIT
            ):
                topology_spread = get_topology_spread(name, logging.getLogger(__name__))

        assert topology_spread
        constraint = topology_spread[0]
        assert constraint.topology_key == "topology.kubernetes.io/zone"
        assert constraint.min_domains == 3
        assert constraint.max_skew == 1
        assert constraint.when_unsatisfiable == "DoNotSchedule"


class TestStackitCrateEnv:
    """
    ``-Cnetwork.publish_host`` resolves ``$(POD_IP)`` from the downward API.
    """

    def test_pod_ip_is_taken_from_the_downward_api(self):
        node_spec = {"resources": {"memory": "123Mi", "heapRatio": 0.456}}

        with mock.patch(
            "crate.operator.create.config.CLOUD_PROVIDER", CloudProvider.STACKIT
        ):
            env = get_statefulset_crate_env(node_spec, 1234, 5678, None)

        pod_ip = [e for e in env if e.name == "POD_IP"]
        assert len(pod_ip) == 1
        assert pod_ip[0].value is None
        assert pod_ip[0].value_from.field_ref.field_path == "status.podIP"
        assert pod_ip[0].value_from.field_ref.api_version == "v1"

    @pytest.mark.parametrize("provider", OTHER_PROVIDERS)
    def test_other_providers_have_no_pod_ip(self, provider):
        node_spec = {"resources": {"memory": "123Mi", "heapRatio": 0.456}}

        with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", provider):
            env = get_statefulset_crate_env(node_spec, 1234, 5678, None)

        assert [e.name for e in env] == ["CRATE_HEAP_SIZE", "CRATE_JAVA_OPTS"]

    def test_pod_ip_is_appended_after_the_ssl_secrets(self, faker):
        """
        Guards the positional unpacking other tests rely on.
        """
        node_spec = {"resources": {"memory": "123Mi", "heapRatio": 0.456}}
        secret_ref = {
            "secretKeyRef": {"key": faker.domain_word(), "name": faker.domain_word()}
        }
        ssl = {"keystoreKeyPassword": secret_ref, "keystorePassword": secret_ref}

        with mock.patch(
            "crate.operator.create.config.CLOUD_PROVIDER", CloudProvider.STACKIT
        ):
            env = get_statefulset_crate_env(node_spec, 1234, 5678, ssl)

        assert [e.name for e in env] == [
            "CRATE_HEAP_SIZE",
            "CRATE_JAVA_OPTS",
            "KEYSTORE_KEY_PASSWORD",
            "KEYSTORE_PASSWORD",
            "POD_IP",
        ]
