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

import subprocess
from unittest import mock

import pytest

from crate.operator.constants import CloudProvider
from crate.operator.create import (
    get_statefulset_crate_command,
    get_statefulset_crate_env,
)

OTHER_PROVIDERS = [
    None,
    CloudProvider.AWS,
    CloudProvider.AZURE,
    CloudProvider.GCP,
    CloudProvider.OPENSHIFT,
]

METADATA_URL = "http://169.254.169.254/openstack/latest/meta_data.json"

#: Shaped like a real SKE reply - one line, ``availability_zone`` in the middle,
#: base64 blobs and a nested ``meta`` object around it. Values are dummies.
OPENSTACK_METADATA = (
    '{"uuid": "00000000-0000-0000-0000-000000000000", "meta": {"cratedb": "shared", '
    '"node.kubernetes.io-role": "node"}, "hostname": "shoot--x--y-pool-z1-abcde", '
    '"launch_index": 0, "availability_zone": "eu01-1", '
    '"random_seed": "B9mC7ObLSxIS9h8jv5d17p3Ut+5WgAx7CfpcySsEIK2nObp906TM6w2u", '
    '"project_id": "00000000000000000000000000000000", "devices": []}'
)


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
        node_spec = {
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
    ``node.attr.zone`` is read from the OpenStack metadata service at pod launch.
    The exact shell snippet is asserted in
    ``tests/test_create.py::TestStatefulSetCrateCommand::test_zone_attr``; here we
    run it to prove it really extracts the zone.
    """

    def zone_pipeline(self):
        cmd = crate_command(CloudProvider.STACKIT)
        zone = [arg for arg in cmd if arg.startswith("-Cnode.attr.zone=")]
        assert len(zone) == 1
        # -Cnode.attr.zone=$(<pipeline>) -> <pipeline>
        return zone[0][len("-Cnode.attr.zone=$(") : -1]

    def run_pipeline(self, metadata):
        """
        Run the generated pipeline with ``curl`` replaced by a local echo, so the
        parsing is exercised without touching the network.
        """
        pipeline = self.zone_pipeline()
        assert pipeline.startswith(f"curl -s '{METADATA_URL}'")
        stdin = pipeline.replace(f"curl -s '{METADATA_URL}'", "cat", 1)
        return subprocess.run(
            ["sh", "-c", stdin],
            input=metadata,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def test_extracts_the_availability_zone(self):
        result = self.run_pipeline(OPENSTACK_METADATA)

        assert result.returncode == 0
        assert result.stdout.strip() == "eu01-1"

    def test_yields_nothing_when_the_metadata_service_is_unreachable(self):
        """
        A failed request must not emit a partial or bogus zone. It resolves to an
        empty attribute, same as the other providers do today.
        """
        result = self.run_pipeline("")

        assert result.stdout.strip() == ""

    @pytest.mark.parametrize("provider", OTHER_PROVIDERS)
    def test_other_providers_are_untouched(self, provider):
        cmd = crate_command(provider)

        assert not any(METADATA_URL in arg for arg in cmd)


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
