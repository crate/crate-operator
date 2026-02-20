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

import pytest

from crate.operator.constants import CloudProvider
from crate.operator.create import (
    get_owner_references,
    get_statefulset_containers,
    get_statefulset_init_containers,
)


class TestOpenShiftOwnerReferences:
    """Test that owner references are created correctly for OpenShift."""

    def test_openshift_disables_block_owner_deletion(self, faker):
        name = faker.domain_word()
        meta = {"uid": faker.uuid4()}

        with mock.patch(
            "crate.operator.create.config.CLOUD_PROVIDER", CloudProvider.OPENSHIFT
        ):
            owner_refs = get_owner_references(name, meta)

        assert len(owner_refs) == 1
        assert owner_refs[0].block_owner_deletion is False
        assert owner_refs[0].controller is True
        assert owner_refs[0].kind == "CrateDB"
        assert owner_refs[0].name == name

    @pytest.mark.parametrize(
        "provider", [None, CloudProvider.AWS, CloudProvider.AZURE, CloudProvider.GCP]
    )
    def test_non_openshift_enables_block_owner_deletion(self, provider, faker):
        name = faker.domain_word()
        meta = {"uid": faker.uuid4()}

        with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", provider):
            owner_refs = get_owner_references(name, meta)

        assert len(owner_refs) == 1
        assert owner_refs[0].block_owner_deletion is True


class TestOpenShiftInitContainers:
    """Test that init containers are configured correctly for OpenShift."""

    def test_openshift_skips_sysctl_init_container(self):
        with mock.patch(
            "crate.operator.create.config.CLOUD_PROVIDER", CloudProvider.OPENSHIFT
        ):
            containers = get_statefulset_init_containers("crate:5.10.16")

        container_names = [c.name for c in containers]
        assert "init-sysctl" not in container_names
        assert "fetch-jmx-exporter" in container_names
        assert "mkdir-heapdump" in container_names

    @pytest.mark.parametrize(
        "provider", [None, CloudProvider.AWS, CloudProvider.AZURE, CloudProvider.GCP]
    )
    def test_non_openshift_includes_sysctl_init_container(self, provider):
        with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", provider):
            containers = get_statefulset_init_containers("crate:5.10.16")

        container_names = [c.name for c in containers]
        assert "init-sysctl" in container_names
        assert "fetch-jmx-exporter" in container_names
        assert "mkdir-heapdump" in container_names

        # Verify sysctl container is privileged
        sysctl_container = next(c for c in containers if c.name == "init-sysctl")
        assert sysctl_container.security_context.privileged is True


class TestOpenShiftSidecarContainer:
    """Test that the crate-control sidecar is added on OpenShift."""

    def test_openshift_adds_crate_control_sidecar(self, faker):
        name = faker.domain_word()
        node_spec = {
            "resources": {
                "requests": {"cpu": 1, "memory": "1Gi"},
                "limits": {"cpu": 1, "memory": "1Gi"},
            }
        }

        with mock.patch(
            "crate.operator.create.config.CLOUD_PROVIDER", CloudProvider.OPENSHIFT
        ):
            with mock.patch(
                "crate.operator.create.config.CRATE_CONTROL_IMAGE",
                "crate/crate-control:latest",
            ):
                containers = get_statefulset_containers(
                    node_spec,
                    1,
                    2,
                    3,
                    4,
                    5,
                    "crate:5.10.16",
                    ["/docker-entrypoint.sh", "crate"],
                    [],
                    [],
                    name,
                    False,
                )

        container_names = [c.name for c in containers]
        assert "crate-control" in container_names

        # Verify crate-control container configuration
        control_container = next(c for c in containers if c.name == "crate-control")
        assert control_container.image == "crate/crate-control:latest"
        assert len(control_container.ports) == 1
        assert control_container.ports[0].container_port == 5050
        assert control_container.ports[0].name == "control"

        # Verify environment variables
        env_names = [e.name for e in control_container.env]
        assert "CRATE_HTTP_URL" in env_names
        assert "BOOTSTRAP_TOKEN" in env_names

        # Verify readiness probe
        assert control_container.readiness_probe is not None
        assert control_container.readiness_probe.http_get.path == "/health"
        assert control_container.readiness_probe.http_get.port == 5050

    def test_openshift_sidecar_uses_ssl_aware_url(self, faker):
        name = faker.domain_word()
        node_spec = {
            "resources": {
                "requests": {"cpu": 1, "memory": "1Gi"},
                "limits": {"cpu": 1, "memory": "1Gi"},
            }
        }

        with mock.patch(
            "crate.operator.create.config.CLOUD_PROVIDER", CloudProvider.OPENSHIFT
        ):
            with mock.patch(
                "crate.operator.create.config.CRATE_CONTROL_IMAGE",
                "crate/crate-control:latest",
            ):
                containers = get_statefulset_containers(
                    node_spec,
                    1,
                    2,
                    3,
                    4,
                    5,
                    "crate:5.10.16",
                    ["/docker-entrypoint.sh", "crate"],
                    [],
                    [],
                    name,
                    True,
                )

        control_container = next(c for c in containers if c.name == "crate-control")
        crate_http_url = next(
            e for e in control_container.env if e.name == "CRATE_HTTP_URL"
        )
        assert crate_http_url.value == "https://localhost:4200/_sql"

    @pytest.mark.parametrize(
        "provider", [None, CloudProvider.AWS, CloudProvider.AZURE, CloudProvider.GCP]
    )
    def test_non_openshift_no_crate_control_sidecar(self, provider, faker):
        name = faker.domain_word()
        node_spec = {
            "resources": {
                "requests": {"cpu": 1, "memory": "1Gi"},
                "limits": {"cpu": 1, "memory": "1Gi"},
            }
        }

        with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", provider):
            containers = get_statefulset_containers(
                node_spec,
                1,
                2,
                3,
                4,
                5,
                "crate:5.10.16",
                ["/docker-entrypoint.sh", "crate"],
                [],
                [],
                name,
                True,
            )

        container_names = [c.name for c in containers]
        assert "crate-control" not in container_names
        assert len(containers) == 2  # Only sql-exporter and crate


class TestOpenShiftLifecycleHooks:
    """Test that lifecycle hooks are conditionally added."""

    def test_openshift_skips_lifecycle_hooks(self, faker):
        name = faker.domain_word()
        node_spec = {
            "resources": {
                "requests": {"cpu": 1, "memory": "1Gi"},
                "limits": {"cpu": 1, "memory": "1Gi"},
            }
        }

        with mock.patch(
            "crate.operator.create.config.CLOUD_PROVIDER", CloudProvider.OPENSHIFT
        ):
            containers = get_statefulset_containers(
                node_spec,
                1,
                2,
                3,
                4,
                5,
                "crate:5.10.16",
                ["/docker-entrypoint.sh", "crate"],
                [],
                [],
                name,
                True,
            )

        crate_container = next(c for c in containers if c.name == "crate")
        assert (
            not hasattr(crate_container, "lifecycle")
            or crate_container.lifecycle is None
        )

    @pytest.mark.parametrize(
        "provider", [None, CloudProvider.AWS, CloudProvider.AZURE, CloudProvider.GCP]
    )
    def test_non_openshift_includes_lifecycle_hooks(self, provider, faker):
        name = faker.domain_word()
        node_spec = {
            "resources": {
                "requests": {"cpu": 1, "memory": "1Gi"},
                "limits": {"cpu": 1, "memory": "1Gi"},
            }
        }

        with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", provider):
            containers = get_statefulset_containers(
                node_spec,
                1,
                2,
                3,
                4,
                5,
                "crate:5.10.16",
                ["/docker-entrypoint.sh", "crate"],
                [],
                [],
                name,
                True,
            )

        crate_container = next(c for c in containers if c.name == "crate")
        assert crate_container.lifecycle is not None
        assert crate_container.lifecycle.post_start is not None
        assert crate_container.lifecycle.pre_stop is not None
