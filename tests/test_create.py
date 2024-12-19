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
import string
from os import walk
from typing import Set
from unittest import mock

import aiohttp
import pytest
from kubernetes_asyncio.client import (
    AppsV1Api,
    CoreV1Api,
    CustomObjectsApi,
    NetworkingV1Api,
    RbacAuthorizationV1Api,
)

from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    DATA_PVC_NAME_PREFIX,
    DCUTIL_BASE_URL,
    DCUTIL_BINARY,
    GRAND_CENTRAL_PROMETHEUS_PORT,
    GRAND_CENTRAL_RESOURCE_PREFIX,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_PART_OF,
    RESOURCE_CRATEDB,
    TERMINATION_GRACE_PERIOD_SECONDS,
    CloudProvider,
)
from crate.operator.create import (
    create_services,
    create_sql_exporter_config,
    create_statefulset,
    create_system_user,
    get_cluster_resource_limits,
    get_cluster_resource_requests,
    get_data_service,
    get_sql_exporter_config,
    get_statefulset_affinity,
    get_statefulset_containers,
    get_statefulset_crate_command,
    get_statefulset_crate_env,
    get_statefulset_crate_volume_mounts,
    get_statefulset_init_containers,
    get_statefulset_pvc,
    get_statefulset_volumes,
    get_tolerations,
    get_topology_spread,
    is_shared_resources_cluster,
)
from crate.operator.utils.formatting import b64decode, format_bitmath

from .utils import (
    CRATE_VERSION,
    CRATE_VERSION_WITH_JWT,
    assert_wait_for,
    do_pods_exist,
    start_cluster,
)


@pytest.fixture
def random_string(faker):
    def f():
        return "".join(faker.random_choices(string.ascii_letters + string.digits + "-"))

    return f


@pytest.mark.k8s
@pytest.mark.asyncio
class TestConfigMaps:
    async def does_configmap_exist(
        self, core: CoreV1Api, namespace: str, name: str
    ) -> bool:
        configmaps = await core.list_namespaced_config_map(namespace)
        return name in (c.metadata.name for c in configmaps.items)

    async def test_create(self, faker, namespace, api_client):
        core = CoreV1Api(api_client)
        name = faker.domain_word()
        await create_sql_exporter_config(
            None, namespace.metadata.name, name, {}, logging.getLogger(__name__)
        )
        await assert_wait_for(
            True,
            self.does_configmap_exist,
            core,
            namespace.metadata.name,
            f"crate-sql-exporter-{name}",
        )


class TestStatefulSetAffinity:
    def test_testing_true(self, faker):
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", True):
            affinity = get_statefulset_affinity(name, logging.getLogger(__name__), {})

        assert affinity is None

    def test_testing_false(self, faker):
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", False):
            affinity = get_statefulset_affinity(name, logging.getLogger(__name__), {})

        apa = affinity.pod_anti_affinity
        terms = apa.required_during_scheduling_ignored_during_execution[0]
        expressions = terms.label_selector.match_expressions
        assert [e.to_dict() for e in expressions] == [
            {
                "key": "app.kubernetes.io/component",
                "operator": "In",
                "values": ["cratedb"],
            },
            {"key": "app.kubernetes.io/name", "operator": "In", "values": [name]},
        ]
        assert terms.topology_key == "kubernetes.io/hostname"

    @pytest.mark.parametrize(
        "node_spec",
        [
            {
                "name": "hot",
                "replicas": 1,
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                    "requests": {"cpu": 0.5, "memory": "8589934592"},
                },
            },
            {
                "name": "hot",
                "replicas": 1,
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                },
            },
            {
                "name": "hot",
                "replicas": 1,
                "resources": {"cpus": 0.5, "memory": "8589934592"},
            },
        ],
    )
    def test_dedicated_resources_affinity(self, node_spec, faker):
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", False):
            affinity = get_statefulset_affinity(
                name, logging.getLogger(__name__), node_spec
            )

        apa = affinity.pod_anti_affinity
        terms = apa.required_during_scheduling_ignored_during_execution[0]
        expressions = terms.label_selector.match_expressions
        assert [e.to_dict() for e in expressions] == [
            {
                "key": "app.kubernetes.io/component",
                "operator": "In",
                "values": ["cratedb"],
            },
            {"key": "app.kubernetes.io/name", "operator": "In", "values": [name]},
        ]
        assert terms.topology_key == "kubernetes.io/hostname"

    @pytest.mark.parametrize(
        "node_spec",
        [
            {
                "name": "hot",
                "replicas": 1,
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                    "requests": {"cpu": 0.25, "memory": "8589934592"},
                },
                "nodepool": "shared",
            },
        ],
    )
    def test_shared_resources_affinity(self, node_spec, faker):
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", False):
            affinity = get_statefulset_affinity(
                name, logging.getLogger(__name__), node_spec
            )

        na = affinity.node_affinity
        selector = na.required_during_scheduling_ignored_during_execution
        terms = selector.node_selector_terms[0]
        expressions = terms.match_expressions
        assert [e.to_dict() for e in expressions] == [
            {
                "key": "cratedb",
                "operator": "In",
                "values": ["shared"],
            }
        ]

    @pytest.mark.parametrize("provider", [CloudProvider.AWS, CloudProvider.AZURE])
    def test_cloud_provider(self, provider, faker):
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", False):
            with mock.patch(
                "crate.operator.create.config.CLOUD_PROVIDER", provider.value
            ):
                topospread = get_topology_spread(name, logging.getLogger(__name__))

        terms = topospread[0]
        expressions = terms.label_selector.match_expressions
        assert [e.to_dict() for e in expressions] == [
            {
                "key": "app.kubernetes.io/component",
                "operator": "In",
                "values": ["cratedb"],
            },
            {"key": "app.kubernetes.io/name", "operator": "In", "values": [name]},
        ]
        assert terms.topology_key == "topology.kubernetes.io/zone"


class TestTolerations:
    def test_testing_true(self, faker):
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", True):
            tolerations = get_tolerations(name, logging.getLogger(__name__), {})

        assert tolerations is None

    @pytest.mark.parametrize(
        "node_spec",
        [
            {
                "name": "hot",
                "replicas": 1,
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                    "requests": {"cpu": 0.5, "memory": "8589934592"},
                },
            },
            {
                "name": "hot",
                "replicas": 1,
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                },
            },
            {
                "name": "hot",
                "replicas": 1,
                "resources": {"cpus": 0.5, "memory": "8589934592"},
            },
        ],
    )
    def test_dedicated_resources_tolerations(self, node_spec, faker):
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", False):
            tolerations = get_tolerations(name, logging.getLogger(__name__), node_spec)

        assert len(tolerations) == 1
        assert tolerations[0].to_dict() == {
            "effect": "NoSchedule",
            "key": "cratedb",
            "operator": "Equal",
            "toleration_seconds": None,
            "value": "any",
        }

    @pytest.mark.parametrize(
        "node_spec",
        [
            {
                "name": "hot",
                "replicas": 1,
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                    "requests": {"cpu": 0.25, "memory": "8589934592"},
                },
                "nodepool": "shared",
            },
        ],
    )
    def test_shared_resources_tolerations(self, node_spec, faker):
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", False):
            tolerations = get_tolerations(name, logging.getLogger(__name__), node_spec)

        toleration = tolerations[0]
        expected = {
            "key": "cratedb",
            "operator": "Equal",
            "value": "shared",
            "effect": "NoSchedule",
        }
        assert expected.items() <= toleration.to_dict().items()


class TestStatefulSetContainers:
    def test(self, faker, random_string):
        cpus = faker.pyfloat(min_value=0)
        memory = faker.numerify("%!!") + ".0" + faker.lexify("?i", "KMG")
        node_spec = {
            "resources": {
                "requests": {"cpu": cpus, "memory": memory},
                "limits": {"cpu": cpus, "memory": memory},
            }
        }
        c_sql_exporter, c_crate = get_statefulset_containers(
            node_spec,
            1,
            2,
            3,
            4,
            5,
            "foo/bar:1.2.3",
            ["/path/to/some/exec.sh", "--with", "args"],
            [],
            [],
        )
        assert c_sql_exporter.name == "sql-exporter"
        assert len(c_sql_exporter.volume_mounts) == 1

        assert c_crate.command == ["/path/to/some/exec.sh", "--with", "args"]
        assert c_crate.image == "foo/bar:1.2.3"
        assert c_crate.name == "crate"
        assert c_crate.resources.to_dict() == {
            "claims": None,
            "limits": {"cpu": str(cpus), "memory": memory},
            "requests": {"cpu": str(cpus), "memory": memory},
        }


class TestStatefulSetCrateCommand:
    def test_entrypoint_first_item(self, random_string):
        cmd = get_statefulset_crate_command(
            namespace=random_string(),
            name="cluster1",
            master_nodes=["data-node-0", "data-node-1", "data-node-2"],
            total_nodes_count=3,
            data_nodes_count=3,
            crate_node_name_prefix="data-node-",
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
        )
        assert ["/docker-entrypoint.sh", "crate"] == cmd[0:2]

    def test_cluster_name(self, random_string):
        cluster_name = random_string()
        cmd = get_statefulset_crate_command(
            namespace="some-namespace",
            name="cluster1",
            master_nodes=["data-node-0", "data-node-1", "data-node-2"],
            total_nodes_count=3,
            data_nodes_count=3,
            crate_node_name_prefix="data-node-",
            cluster_name=cluster_name,
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
        )
        assert f"-Ccluster.name={cluster_name}" in cmd

    def test_node_name(self, random_string):
        node_name = random_string()
        crate_node_name_prefix = random_string()
        cmd = get_statefulset_crate_command(
            namespace=random_string(),
            name=random_string(),
            master_nodes=["data-node-0", "data-node-1", "data-node-2"],
            total_nodes_count=3,
            data_nodes_count=3,
            crate_node_name_prefix=crate_node_name_prefix,
            cluster_name="my-cluster",
            node_name=node_name,
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
        )
        assert (
            f"-Cnode.name={crate_node_name_prefix}$(hostname | rev | cut -d- -f1 | rev)"
            in cmd
        )
        assert f"-Cnode.attr.node_name={node_name}" in cmd

    @pytest.mark.parametrize(
        "total, data_nodes, quorum",
        [(2, 1, 1), (3, 2, 2), (4, 3, 2), (5, 4, 3), (123, 122, 62)],
    )
    def test_node_counts(self, total, data_nodes, quorum):
        master_nodes = ["node-0"]
        cmd = get_statefulset_crate_command(
            namespace="some-namespace",
            name="cluster1",
            master_nodes=master_nodes,
            total_nodes_count=total,
            data_nodes_count=data_nodes,
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
            crate_version="4.7.0",
        )
        assert f"-Cgateway.recover_after_data_nodes={quorum}" in cmd
        assert f"-Cgateway.expected_data_nodes={data_nodes}" in cmd

    @pytest.mark.parametrize(
        "total, quorum", [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (123, 62)]
    )
    def test_node_counts_deprecated_settings(self, total, quorum):
        cmd = get_statefulset_crate_command(
            namespace="some-namespace",
            name="cluster1",
            master_nodes=["node-0", "node-1", "node-2"],
            total_nodes_count=total,
            data_nodes_count=total,
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
        )
        assert f"-Cgateway.recover_after_nodes={quorum}" in cmd
        assert f"-Cgateway.expected_nodes={total}" in cmd

    @pytest.mark.parametrize("count", [1, 2, 5])
    def test_disks_counts(self, count):
        cmd = get_statefulset_crate_command(
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
                    "disk": {"count": count},
                }
            },
            cluster_settings=None,
            has_ssl=False,
            is_master=True,
            is_data=True,
            crate_version="4.6.3",
        )
        arg = "-Cpath.data=" + ",".join(f"/data/data{i}" for i in range(count))
        assert arg in cmd

    @pytest.mark.parametrize("cpus, ceiled", [(0.1, 1), (2.5, 3), (4, 4)])
    def test_cpus(self, cpus, ceiled):
        cmd = get_statefulset_crate_command(
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
                    "requests": {"cpu": cpus},
                    "limits": {"cpu": cpus},
                    "disk": {"count": 1},
                }
            },
            cluster_settings=None,
            has_ssl=False,
            is_master=True,
            is_data=True,
            crate_version="4.6.3",
        )
        assert f"-Cprocessors={ceiled}" in cmd

    @pytest.mark.parametrize("master", [True, False])
    @pytest.mark.parametrize("data", [True, False])
    def test_master_data_flags(self, master, data):
        cmd = get_statefulset_crate_command(
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
            is_master=master,
            is_data=data,
            crate_version="4.6.3",
        )
        assert f"-Cnode.master={str(master).lower()}" in cmd
        assert f"-Cnode.data={str(data).lower()}" in cmd

    @pytest.mark.parametrize("ssl", [True, False])
    def test_ssl(self, ssl):
        cmd = get_statefulset_crate_command(
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
            has_ssl=ssl,
            is_master=True,
            is_data=True,
            crate_version="4.6.3",
        )
        if ssl:
            assert "-Cssl.http.enabled=true" in cmd
            assert "-Cssl.psql.enabled=true" in cmd
        else:
            assert "-Cssl.http.enabled=true" not in cmd
            assert "-Cssl.psql.enabled=true" not in cmd

    def test_node_and_cluster_settings_may_override(self):
        cmd = get_statefulset_crate_command(
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
                },
                "settings": {
                    "auth.host_based.enabled": "node-override",
                    "node.attr.node_setting": "node-override",
                    "node.attr.some_node_setting": "node",
                },
            },
            cluster_settings={
                "auth.host_based.enabled": "cluster-override",
                "node.master": "cluster-override",
                "node.attr.some_cluster_setting": "cluster",
            },
            has_ssl=False,
            is_master=True,
            is_data=True,
            crate_version="4.6.3",
        )
        assert "-Cauth.host_based.enabled=node-override" in cmd
        assert "-Cnode.attr.node_setting=node-override" in cmd
        assert "-Cnode.master=cluster-override" in cmd
        assert "-Cnode.attr.some_node_setting=node" in cmd
        assert "-Cnode.attr.some_cluster_setting=cluster" in cmd

    @pytest.mark.parametrize(
        "provider, url, header",
        [
            (
                CloudProvider.AWS,
                "-X PUT 'http://169.254.169.254/latest/api/token'",
                " -H 'X-aws-ec2-metadata-token-ttl-seconds: 120' | xargs -I {} curl -s 'http://169.254.169.254/latest/meta-data/placement/availability-zone' -H 'X-aws-ec2-metadata-token: {}'",  # noqa
            ),
            (
                CloudProvider.AZURE,
                "'http://169.254.169.254/metadata/instance/compute/zone?api-version=2020-06-01&format=text'",  # noqa
                " -H 'Metadata: true'",
            ),
            (
                CloudProvider.GCP,
                "'http://169.254.169.254/computeMetadata/v1/instance/zone'",
                " -H 'Metadata-Flavor: Google' | rev | cut -d '/' -f 1 | rev",
            ),
        ],
    )
    def test_zone_attr(self, provider, url, header):
        with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", provider):
            cmd = get_statefulset_crate_command(
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
            )
        assert f"-Cnode.attr.zone=$(curl -s {url}{header})" in cmd

    @pytest.mark.parametrize(
        "node_settings, cluster_settings",
        [
            ({"node.attr.zone": "test"}, None),
            ({}, {"node.attr.zone": "test"}),
        ],
    )
    def test_zone_attr_with_override(self, node_settings, cluster_settings):
        with mock.patch(
            "crate.operator.create.config.CLOUD_PROVIDER", CloudProvider.AZURE
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
                node_spec={
                    "resources": {
                        "requests": {"cpu": 1},
                        "limits": {"cpu": 1},
                        "disk": {"count": 1},
                    },
                    "settings": node_settings,
                },
                cluster_settings=cluster_settings,
                has_ssl=False,
                is_master=True,
                is_data=True,
                crate_version="4.6.3",
            )
        assert "-Cnode.attr.zone=test" in cmd

    @pytest.mark.parametrize(
        "crate_version, jwt_expected",
        [
            (CRATE_VERSION_WITH_JWT, True),
            (CRATE_VERSION, False),
        ],
    )
    def test_jwt_auth(self, crate_version, jwt_expected):
        cmd = get_statefulset_crate_command(
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
            has_ssl=True,
            is_master=True,
            is_data=True,
            crate_version=crate_version,
        )
        if jwt_expected:
            assert "-Cauth.host_based.config.98.method=jwt" in cmd
            assert "-Cauth.host_based.config.98.protocol=http" in cmd
            assert "-Cauth.host_based.config.98.ssl=on" in cmd
        else:
            assert "-Cauth.host_based.config.98.method=jwt" not in cmd
            assert "-Cauth.host_based.config.98.protocol=http" not in cmd
            assert "-Cauth.host_based.config.98.ssl=on" not in cmd


class TestStatefulSetCrateEnv:
    def test_without_ssl(self, faker):
        memory = "123Mi"
        heap_ratio = 0.456
        node_spec = {"resources": {"memory": memory, "heapRatio": heap_ratio}}
        e_heap_size, e_java_opts = get_statefulset_crate_env(
            node_spec, 1234, 5678, None
        )
        assert e_heap_size.name == "CRATE_HEAP_SIZE"
        assert e_heap_size.value == "58812530"
        assert e_java_opts.name == "CRATE_JAVA_OPTS"
        assert "-XX:+HeapDumpOnOutOfMemoryError" in e_java_opts.value

    def test_with_ssl(self, faker):
        memory = "123Mi"
        heap_ratio = 0.456
        node_spec = {"resources": {"memory": memory, "heapRatio": heap_ratio}}
        keystore_key_password_key = faker.domain_word()
        keystore_key_password_name = faker.domain_word()
        keystore_password_key = faker.domain_word()
        keystore_password_name = faker.domain_word()
        ssl = {
            "keystoreKeyPassword": {
                "secretKeyRef": {
                    "key": keystore_key_password_key,
                    "name": keystore_key_password_name,
                }
            },
            "keystorePassword": {
                "secretKeyRef": {
                    "key": keystore_password_key,
                    "name": keystore_password_name,
                }
            },
        }
        (e_heap_size, e_java_opts, e_key_pw, e_pw) = get_statefulset_crate_env(
            node_spec, 1234, 5678, ssl
        )
        assert e_heap_size.name == "CRATE_HEAP_SIZE"
        assert e_heap_size.value == "58812530"
        assert e_java_opts.name == "CRATE_JAVA_OPTS"
        assert "-XX:+HeapDumpOnOutOfMemoryError" in e_java_opts.value
        assert e_key_pw.name == "KEYSTORE_KEY_PASSWORD"
        assert e_key_pw.value_from.secret_key_ref.key == keystore_key_password_key
        assert e_key_pw.value_from.secret_key_ref.name == keystore_key_password_name
        assert e_pw.name == "KEYSTORE_PASSWORD"
        assert e_pw.value_from.secret_key_ref.key == keystore_password_key
        assert e_pw.value_from.secret_key_ref.name == keystore_password_name


class TestStatefulSetCrateVolumeMounts:
    def test_without_ssl(self, faker):
        disks = faker.pyint(min_value=1, max_value=5)
        node_spec = {"resources": {"disk": {"count": disks}}}
        vm_jmxdir, vm_resource, *vm_data = get_statefulset_crate_volume_mounts(
            node_spec, None
        )
        assert vm_jmxdir.name == "jmxdir"
        assert vm_resource.name == "debug"
        assert [(vm.mount_path, vm.name) for vm in vm_data] == [
            (f"/data/data{i}", f"{DATA_PVC_NAME_PREFIX}{i}") for i in range(disks)
        ]

    def test_with_ssl(self, faker):
        disks = faker.pyint(min_value=1, max_value=5)
        node_spec = {"resources": {"disk": {"count": disks}}}
        vm_jmxdir, vm_resource, *vm_data, vm_ssl = get_statefulset_crate_volume_mounts(
            node_spec, {}
        )
        assert vm_jmxdir.name == "jmxdir"
        assert vm_resource.name == "debug"
        assert [(vm.mount_path, vm.name) for vm in vm_data] == [
            (f"/data/data{i}", f"{DATA_PVC_NAME_PREFIX}{i}") for i in range(disks)
        ]
        assert vm_ssl.name == "keystore"


class TestStatefulSetInitContainers:
    def test(self):
        c_init_sysctl, c_fetch_jmx, c_heapdump = get_statefulset_init_containers(
            "foo/bar:1.2.3"
        )
        assert c_init_sysctl.name == "init-sysctl"
        assert c_fetch_jmx.name == "fetch-jmx-exporter"
        assert c_heapdump.name == "mkdir-heapdump"


class TestStatefulSetInitContainerChownTrue:
    @pytest.mark.parametrize("provider", [None, "aws", "azure"])
    def test(self, provider):
        with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", provider):
            _, _, c_heapdump = get_statefulset_init_containers("foo/bar:1.2.3")
        assert c_heapdump.name == "mkdir-heapdump"
        chown_ignore_rc = "|| true"
        if provider == "aws":
            assert chown_ignore_rc in c_heapdump.command[2]
        if provider in {"azure", None}:
            assert chown_ignore_rc not in c_heapdump.command[2]


class TestStatefulSetPVC:
    def test(self, faker):
        count = faker.pyint(min_value=1, max_value=5)
        s = faker.numerify("%!!") + ".0" + faker.lexify("?i", "KMG")
        storage_class = faker.domain_word()
        node_spec = {
            "resources": {
                "disk": {
                    "count": count,
                    "size": s + "B",
                    "storageClass": storage_class,
                }
            }
        }
        pvcs = get_statefulset_pvc(None, node_spec)
        expected_pvcs = [f"{DATA_PVC_NAME_PREFIX}{i}" for i in range(count)]
        expected_pvcs.append("debug")
        expected_sizes = [s] * count
        expected_sizes.append(format_bitmath(config.DEBUG_VOLUME_SIZE))
        expected_storage_class_name = [storage_class] * count
        expected_storage_class_name.append(config.DEBUG_VOLUME_STORAGE_CLASS)
        assert [pvc.metadata.name for pvc in pvcs] == expected_pvcs
        assert [
            pvc.spec.resources.requests["storage"] for pvc in pvcs
        ] == expected_sizes
        assert [
            pvc.spec.storage_class_name for pvc in pvcs
        ] == expected_storage_class_name


class TestStatefulSetVolumes:
    def test_without_ssl(self, faker):
        name = faker.domain_word()
        v_sql_exporter, v_jmx = get_statefulset_volumes(name, None)
        assert v_sql_exporter.name == "crate-sql-exporter"
        assert v_jmx.name == "jmxdir"

    def test_with_ssl(self, faker):
        name = faker.domain_word()
        keystore_key = faker.domain_word()
        keystore_name = faker.domain_word()
        ssl = {
            "keystore": {"secretKeyRef": {"key": keystore_key, "name": keystore_name}}
        }
        v_sql_exporter, v_jmx, v_keystore = get_statefulset_volumes(name, ssl)
        assert v_sql_exporter.name == "crate-sql-exporter"
        assert v_jmx.name == "jmxdir"
        assert v_keystore.name == "keystore"
        assert v_keystore.secret.secret_name == keystore_name
        assert v_keystore.secret.items[0].to_dict() == {
            "key": keystore_key,
            "mode": None,
            "path": "keystore.jks",
        }


@pytest.mark.k8s
@pytest.mark.asyncio
class TestStatefulSet:
    async def does_statefulset_exist(
        self, apps: AppsV1Api, namespace: str, name: str
    ) -> bool:
        stss = await apps.list_namespaced_stateful_set(namespace=namespace)
        return name in (s.metadata.name for s in stss.items)

    async def test_create(self, faker, namespace, api_client):
        apps = AppsV1Api(api_client)
        core = CoreV1Api(api_client)
        name = faker.domain_word()
        cluster_name = faker.domain_word()
        node_name = faker.domain_word()
        await create_statefulset(
            None,
            namespace.metadata.name,
            name,
            {
                LABEL_MANAGED_BY: "crate-operator",
                LABEL_NAME: name,
                LABEL_PART_OF: "cratedb",
                LABEL_COMPONENT: "cratedb",
            },
            True,
            True,
            cluster_name,
            node_name,
            f"data-{node_name}-",
            {
                "replicas": 3,
                "resources": {
                    "requests": {
                        "cpu": 0.5,
                        "memory": "1Gi",
                    },
                    "limits": {
                        "cpu": 0.5,
                        "memory": "1Gi",
                    },
                    "heapRatio": 0.4,
                    "disk": {"count": 1, "size": "16Gi", "storageClass": "default"},
                },
            },
            ["master-1", "master-2", "master-3"],
            3,
            3,
            10000,
            20000,
            30000,
            40000,
            50000,
            f"crate:{CRATE_VERSION}",
            {
                "keystore": {"secretKeyRef": {"key": "keystore", "name": "sslcert"}},
                "keystoreKeyPassword": {
                    "secretKeyRef": {"key": "keystore-key-password", "name": "sslcert"}
                },
                "keystorePassword": {
                    "secretKeyRef": {"key": "keystore-password", "name": "sslcert"}
                },
            },
            {},
            [],
            logging.getLogger(__name__),
        )
        await assert_wait_for(
            True,
            self.does_statefulset_exist,
            apps,
            namespace.metadata.name,
            f"crate-data-{node_name}-{name}",
        )
        await assert_wait_for(
            True,
            do_pods_exist,
            core,
            namespace.metadata.name,
            {f"crate-data-{node_name}-{name}-{i}" for i in range(3)},
        )


class TestServiceModels:
    @pytest.mark.parametrize("dns", [None, "mycluster.example.com"])
    @pytest.mark.parametrize("provider", [None, "aws", "azure"])
    def test_get_data_service(self, provider, dns, faker):
        name = faker.domain_word()
        http = faker.port_number()
        psql = faker.port_number()
        with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", provider):
            service = get_data_service(None, name, None, http, psql, dns)
        annotation_keys = service.metadata.annotations.keys()
        if provider == "aws":
            assert (
                "service.beta.kubernetes.io/aws-load-balancer-connection-draining-enabled"  # noqa
                in annotation_keys
            )
            assert (
                "service.beta.kubernetes.io/aws-load-balancer-connection-draining-timeout"  # noqa
                in annotation_keys
            )
            assert (
                "service.beta.kubernetes.io/aws-load-balancer-connection-idle-timeout"  # noqa
                in annotation_keys
            )
            assert (
                "service.beta.kubernetes.io/aws-load-balancer-type"  # noqa
                in annotation_keys
            )
        if provider == "azure":
            assert (
                "service.beta.kubernetes.io/azure-load-balancer-disable-tcp-reset"
                in annotation_keys
            )
            assert (
                "service.beta.kubernetes.io/azure-load-balancer-tcp-idle-timeout"
                in annotation_keys
            )
        if dns:
            assert (
                service.metadata.annotations[
                    "external-dns.alpha.kubernetes.io/hostname"
                ]
                == dns
            )


@pytest.mark.k8s
@pytest.mark.asyncio
class TestServices:
    async def do_services_exist(
        self, core: CoreV1Api, namespace: str, expected: Set[str]
    ) -> bool:
        services = await core.list_namespaced_service(namespace)
        return expected.issubset({s.metadata.name for s in services.items})

    @pytest.mark.parametrize(
        "annotations",
        [
            None,
            {"some/annotation.id": "what"},
            {
                "some/annotation.id": "what",
                "external-dns.alpha.kubernetes.io/hostname": "override",
            },
        ],
    )
    async def test_create(self, faker, namespace, api_client, annotations):
        core = CoreV1Api(api_client)
        name = faker.domain_word()
        host = faker.domain_name()
        await create_services(
            None,
            namespace.metadata.name,
            name,
            {},
            1,
            2,
            3,
            host,
            logging.getLogger(__name__),
            additional_annotations=annotations,
        )

        expected_annotations = {"external-dns.alpha.kubernetes.io/hostname": host}
        if annotations:
            expected_annotations.update(annotations)

        await assert_wait_for(
            True,
            self.do_services_exist,
            core,
            namespace.metadata.name,
            {f"crate-{name}", f"crate-discovery-{name}"},
        )
        service = await core.read_namespaced_service(
            f"crate-{name}", namespace.metadata.name
        )
        assert service.spec.load_balancer_source_ranges is None
        assert service.metadata.annotations == expected_annotations

    async def test_create_with_source_ranges(self, faker, namespace, api_client):
        core = CoreV1Api(api_client)
        name = faker.domain_word()
        await create_services(
            None,
            namespace.metadata.name,
            name,
            {},
            1,
            2,
            3,
            faker.domain_name(),
            logging.getLogger(__name__),
            source_ranges=["192.168.1.1/32", "10.0.0.0/8"],
        )

        await assert_wait_for(
            True,
            self.do_services_exist,
            core,
            namespace.metadata.name,
            {f"crate-{name}", f"crate-discovery-{name}"},
        )
        service = await core.read_namespaced_service(
            f"crate-{name}", namespace.metadata.name
        )
        assert service.spec.load_balancer_source_ranges == [
            "192.168.1.1/32",
            "10.0.0.0/8",
        ]


@pytest.mark.k8s
@pytest.mark.asyncio
class TestSystemUser:
    async def does_secret_exist(
        self, core: CoreV1Api, namespace: str, name: str
    ) -> bool:
        secrets = await core.list_namespaced_secret(namespace)
        return name in (s.metadata.name for s in secrets.items)

    async def test_create(self, faker, namespace, api_client):
        core = CoreV1Api(api_client)
        name = faker.domain_word()
        password = faker.password(length=12)
        with mock.patch("crate.operator.create.gen_password", return_value=password):
            await create_system_user(
                None, namespace.metadata.name, name, {}, logging.getLogger(__name__)
            )
        await assert_wait_for(
            True,
            self.does_secret_exist,
            core,
            namespace.metadata.name,
            f"user-system-{name}",
        )
        secrets = (await core.list_namespaced_secret(namespace.metadata.name)).items
        secret = next(
            filter(lambda x: x.metadata.name == f"user-system-{name}", secrets)
        )
        assert b64decode(secret.data["password"]) == password


@pytest.mark.k8s
@pytest.mark.asyncio
class TestCreateCustomResource:
    async def does_statefulset_exist(
        self, apps: AppsV1Api, namespace: str, name: str
    ) -> bool:
        stss = await apps.list_namespaced_stateful_set(namespace=namespace)
        return name in (s.metadata.name for s in stss.items)

    async def do_services_exist(
        self, core: CoreV1Api, namespace: str, expected: Set[str]
    ) -> bool:
        services = await core.list_namespaced_service(namespace)
        return expected.issubset({s.metadata.name for s in services.items})

    async def does_deployment_exist(
        self, apps: AppsV1Api, namespace: str, name: str
    ) -> bool:
        deployments = await apps.list_namespaced_deployment(namespace=namespace)
        return name in (d.metadata.name for d in deployments.items)

    async def does_ingress_exist(
        self, networking: NetworkingV1Api, namespace: str, name: str
    ) -> bool:
        ingresses = await networking.list_namespaced_ingress(namespace=namespace)
        return name in (i.metadata.name for i in ingresses.items)

    async def test_create_minimal(self, faker, namespace, kopf_runner, api_client):
        apps = AppsV1Api(api_client)
        coapi = CustomObjectsApi(api_client)
        core = CoreV1Api(api_client)
        name = faker.domain_word()

        await start_cluster(name, namespace, core, coapi, 1, wait_for_healthy=False)
        await assert_wait_for(
            True,
            self.does_statefulset_exist,
            apps,
            namespace.metadata.name,
            f"crate-data-hot-{name}",
        )
        await assert_wait_for(
            True,
            do_pods_exist,
            core,
            namespace.metadata.name,
            {f"crate-data-hot-{name}-0"},
        )

    async def test_decommission_settings(
        self, faker, namespace, kopf_runner, api_client
    ):
        apps = AppsV1Api(api_client)
        coapi = CustomObjectsApi(api_client)
        core = CoreV1Api(api_client)
        rbac = RbacAuthorizationV1Api(api_client)
        name = faker.domain_word()

        await start_cluster(name, namespace, core, coapi, 1, wait_for_healthy=False)
        await assert_wait_for(
            True,
            self.does_statefulset_exist,
            apps,
            namespace.metadata.name,
            f"crate-data-hot-{name}",
        )
        await assert_wait_for(
            True,
            do_pods_exist,
            core,
            namespace.metadata.name,
            {f"crate-data-hot-{name}-0"},
        )
        statefulset = await apps.read_namespaced_stateful_set(
            f"crate-data-hot-{name}", namespace.metadata.name
        )
        assert (
            statefulset.spec.template.spec.termination_grace_period_seconds
            == TERMINATION_GRACE_PERIOD_SECONDS
        )

        role = await rbac.read_namespaced_role(f"crate-{name}", namespace.metadata.name)
        assert any(
            rule
            for rule in role.rules
            if "statefulsets" in rule.resources and "list" in rule.verbs
        ), "Role does not contain the 'list' verb for 'statefulsets'"

        rolebinding = await rbac.read_namespaced_role_binding(
            f"crate-{name}", namespace.metadata.name
        )
        assert any(
            subject
            for subject in rolebinding.subjects
            if subject.kind == "ServiceAccount" and subject.name == "default"
        ), "RoleBinding does not contain the expected ServiceAccount subject"

    async def test_create_with_svc_annotations(
        self, faker, namespace, kopf_runner, api_client
    ):
        coapi = CustomObjectsApi(api_client)
        core = CoreV1Api(api_client)
        name = faker.domain_word()

        await start_cluster(
            name,
            namespace,
            core,
            coapi,
            1,
            wait_for_healthy=False,
            additional_cluster_spec={
                "service": {"annotations": {"some/annotation": "some.value"}}
            },
        )
        await assert_wait_for(
            True,
            self.do_services_exist,
            core,
            namespace.metadata.name,
            {f"crate-{name}"},
        )
        service = await core.read_namespaced_service(
            f"crate-{name}", namespace.metadata.name
        )
        assert service.metadata.annotations["some/annotation"] == "some.value"

    async def test_create_with_grand_central_backend_enabled(
        self, faker, namespace, kopf_runner, api_client
    ):
        apps = AppsV1Api(api_client)
        coapi = CustomObjectsApi(api_client)
        core = CoreV1Api(api_client)
        networking = NetworkingV1Api(api_client)
        name = faker.domain_word()

        await start_cluster(
            name,
            namespace,
            core,
            coapi,
            1,
            wait_for_healthy=False,
            additional_cluster_spec={
                "externalDNS": "my-crate-cluster.aks1.eastus.azure.cratedb-dev.net."
            },
            grand_central_spec={
                "backendEnabled": True,
                "backendImage": "cloud.registry.cr8.net/crate/grand-central:latest",
                "apiUrl": "https://my-cratedb-api.cloud/",
                "jwkUrl": "https://my-cratedb-api.cloud/api/v2/meta/jwk/",
            },
        )
        await assert_wait_for(
            True,
            self.does_deployment_exist,
            apps,
            namespace.metadata.name,
            f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
        )
        deploy = await apps.read_namespaced_deployment(
            f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}", namespace.metadata.name
        )
        assert (
            deploy.spec.template.spec.containers[0].image
            == "cloud.registry.cr8.net/crate/grand-central:latest"
        )
        na = deploy.spec.template.spec.affinity.node_affinity
        selector = na.required_during_scheduling_ignored_during_execution
        terms = selector.node_selector_terms[0]
        expressions = terms.match_expressions
        assert [e.to_dict() for e in expressions] == [
            {
                "key": "cratedb",
                "operator": "In",
                "values": ["shared"],
            }
        ]
        await assert_wait_for(
            True,
            self.do_services_exist,
            core,
            namespace.metadata.name,
            {f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}"},
        )

        await assert_wait_for(
            True,
            self.does_ingress_exist,
            networking,
            namespace.metadata.name,
            f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
        )
        ingress = await networking.read_namespaced_ingress(
            f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}", namespace.metadata.name
        )
        assert (
            ingress.metadata.annotations["external-dns.alpha.kubernetes.io/hostname"]
            == "my-crate-cluster.gc.aks1.eastus.azure.cratedb-dev.net"
        )

        # Test Prometheus
        assert (
            deploy.spec.template.metadata.annotations["prometheus.io/scrape"] == "true"
        )
        assert deploy.spec.template.metadata.annotations["prometheus.io/port"] == str(
            GRAND_CENTRAL_PROMETHEUS_PORT
        )
        app_prometheus_port = next(
            (
                port.container_port
                for port in deploy.spec.template.spec.containers[0].ports
                if port.name == "prometheus"
            ),
            None,
        )
        assert app_prometheus_port == GRAND_CENTRAL_PROMETHEUS_PORT

        service = await core.read_namespaced_service(
            namespace=namespace.metadata.name,
            name=f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}",
        )
        svc_prometheus_port = next(
            (port for port in service.spec.ports if port.name == "prometheus"),
            None,
        )
        assert svc_prometheus_port
        assert svc_prometheus_port.port == GRAND_CENTRAL_PROMETHEUS_PORT
        assert svc_prometheus_port.target_port == GRAND_CENTRAL_PROMETHEUS_PORT

    async def test_preserve_unknown_object_keys(
        self, faker, namespace, cratedb_crd, api_client
    ):
        # We don't actually run the operator in this test, since we only want
        # to retrieve the custom resource from K8s again and make sure that
        # all object keys are still present.
        coapi = CustomObjectsApi(api_client)
        name = faker.domain_word()

        await coapi.create_namespaced_custom_object(
            group=API_GROUP,
            version="v1",
            plural=RESOURCE_CRATEDB,
            namespace=namespace.metadata.name,
            body={
                "apiVersion": "cloud.crate.io/v1",
                "kind": "CrateDB",
                "metadata": {"name": name},
                "spec": {
                    "cluster": {
                        "imageRegistry": "crate",
                        "name": "my-crate-cluster",
                        "settings": {"s.c.s": "1"},
                        "version": CRATE_VERSION,
                    },
                    "nodes": {
                        "data": [
                            {
                                "annotations": {"s.n.d.0.a": "1"},
                                "name": "data",
                                "labels": {"s.n.d.0.l": "1"},
                                "replicas": 1,
                                "resources": {
                                    "requests": {
                                        "cpu": 0.5,
                                        "memory": "1Gi",
                                    },
                                    "limits": {
                                        "cpu": 0.5,
                                        "memory": "1Gi",
                                    },
                                    "heapRatio": 0.25,
                                    "disk": {
                                        "storageClass": "default",
                                        "size": "16GiB",
                                        "count": 1,
                                    },
                                },
                                "settings": {"s.n.d.0.s": "1"},
                            },
                        ],
                        "master": {
                            "annotations": {"s.n.m.a": "1"},
                            "labels": {"s.n.m.l": "1"},
                            "replicas": 3,
                            "resources": {
                                "requests": {
                                    "cpu": 0.5,
                                    "memory": "1Gi",
                                },
                                "limits": {
                                    "cpu": 0.5,
                                    "memory": "1Gi",
                                },
                                "heapRatio": 0.25,
                                "disk": {
                                    "storageClass": "default",
                                    "size": "16GiB",
                                    "count": 1,
                                },
                            },
                            "settings": {"s.n.m.s": "1"},
                        },
                    },
                },
                "status": {"foo": "bar", "buz": {"lorem": "ipsum"}},
            },
        )

        resource = await coapi.get_namespaced_custom_object(
            group=API_GROUP,
            version="v1",
            plural=RESOURCE_CRATEDB,
            namespace=namespace.metadata.name,
            name=name,
        )
        assert resource["spec"]["cluster"]["settings"] == {"s.c.s": "1"}
        assert resource["spec"]["nodes"]["data"][0]["annotations"] == {"s.n.d.0.a": "1"}
        assert resource["spec"]["nodes"]["data"][0]["labels"] == {"s.n.d.0.l": "1"}
        assert resource["spec"]["nodes"]["data"][0]["settings"] == {"s.n.d.0.s": "1"}
        assert resource["spec"]["nodes"]["master"]["annotations"] == {"s.n.m.a": "1"}
        assert resource["spec"]["nodes"]["master"]["labels"] == {"s.n.m.l": "1"}
        assert resource["spec"]["nodes"]["master"]["settings"] == {"s.n.m.s": "1"}
        assert resource["status"] == {"foo": "bar", "buz": {"lorem": "ipsum"}}


def test_sql_exporter_config():
    result = get_sql_exporter_config(None, "test-name", None)
    assert result.metadata.name == "crate-sql-exporter-test-name"

    _, _, filenames = next(walk("crate/operator/data"))

    assert len(result.data) == len(filenames)
    for filename in filenames:
        assert filename in result.data.keys()
        assert result.data[filename] is not None


@pytest.mark.parametrize(
    ("node_spec", "is_shared"),
    [
        (
            {
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                    "requests": {"cpu": 0.5, "memory": "8589934592"},
                },
                "nodepool": "dedicated",
            },
            False,
        ),
        (
            {
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                },
                "nodepool": "dedicated",
            },
            False,
        ),
        (
            {
                "resources": {"cpus": 0.5, "memory": "8589934592"},
                "nodepool": "dedicated",
            },
            False,
        ),
        (
            {
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                    "requests": {"cpu": 0.25, "memory": "8589934592"},
                },
                "nodepool": "shared",
            },
            True,
        ),
    ],
)
def test_is_shared_resources_cluster(node_spec, is_shared):
    assert is_shared_resources_cluster(node_spec) == is_shared


@pytest.mark.parametrize(
    ("node_spec", "expected_requests_cpu"),
    [
        (
            {
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                    "requests": {"cpu": 0.5, "memory": "8589934592"},
                },
            },
            0.5,
        ),
        (
            {
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                },
            },
            0.5,
        ),
        (
            {
                "resources": {"cpus": 0.5, "memory": "8589934592"},
            },
            0.5,
        ),
        (
            {
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                    "requests": {"cpu": 0.25, "memory": "8589934592"},
                },
            },
            0.25,
        ),
    ],
)
def test_get_cluster_resource_requests(node_spec, expected_requests_cpu):
    assert (
        get_cluster_resource_requests(
            node_spec, resource_type="cpu", fallback_key="cpus"
        )
        == expected_requests_cpu
    )


@pytest.mark.parametrize(
    ("node_spec", "expected_limits_cpu"),
    [
        (
            {
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                    "requests": {"cpu": 0.5, "memory": "8589934592"},
                },
            },
            0.5,
        ),
        (
            {
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                },
            },
            0.5,
        ),
        (
            {
                "resources": {"cpus": 0.5, "memory": "8589934592"},
            },
            0.5,
        ),
        (
            {
                "resources": {
                    "limits": {"cpu": 0.5, "memory": "8589934592"},
                    "requests": {"cpu": 0.25, "memory": "8589934592"},
                },
            },
            0.5,
        ),
    ],
)
def test_get_cluster_resource_limits(node_spec, expected_limits_cpu):
    assert (
        get_cluster_resource_limits(node_spec, resource_type="cpu", fallback_key="cpus")
        == expected_limits_cpu
    )


@pytest.mark.asyncio
async def test_download_dc_util():
    url = f"{DCUTIL_BASE_URL}/{DCUTIL_BINARY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"
