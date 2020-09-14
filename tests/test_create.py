# CrateDB Kubernetes Operator
# Copyright (C) 2020 Crate.IO GmbH
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

import asyncio
import logging
import string
from typing import Set
from unittest import mock

import pytest
from kubernetes_asyncio.client import AppsV1Api, CoreV1Api, CustomObjectsApi

from crate.operator.constants import (
    API_GROUP,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_PART_OF,
    RESOURCE_CRATEDB,
)
from crate.operator.create import (
    create_debug_volume,
    create_services,
    create_sql_exporter_config,
    create_statefulset,
    create_system_user,
    get_data_service,
    get_statefulset_affinity,
    get_statefulset_containers,
    get_statefulset_crate_command,
    get_statefulset_crate_env,
    get_statefulset_crate_volume_mounts,
    get_statefulset_init_containers,
    get_statefulset_pvc,
    get_statefulset_volumes,
)
from crate.operator.utils.formatting import b64decode

from .utils import assert_wait_for


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

    async def test_create(self, faker, namespace):
        core = CoreV1Api()
        name = faker.domain_word()
        await create_sql_exporter_config(
            core, None, namespace.metadata.name, name, {}, logging.getLogger(__name__)
        )
        await assert_wait_for(
            True,
            self.does_configmap_exist,
            core,
            namespace.metadata.name,
            f"crate-sql-exporter-{name}",
        )


@pytest.mark.k8s
@pytest.mark.asyncio
class TestDebugVolume:
    async def does_pv_exist(self, core: CoreV1Api, name: str) -> bool:
        pvs = await core.list_persistent_volume()
        return name in (pv.metadata.name for pv in pvs.items)

    async def does_pvc_exist(self, core: CoreV1Api, namespace: str, name: str) -> bool:
        pvcs = await core.list_persistent_volume_claim_for_all_namespaces()
        return (namespace, name) in (
            (pvc.metadata.namespace, pvc.metadata.name) for pvc in pvcs.items
        )

    async def test_create(self, faker, namespace, cleanup_handler):
        core = CoreV1Api()
        name = faker.domain_word()

        # Clean up persistent volume after the test
        cleanup_handler.append(
            core.delete_persistent_volume(
                name=f"temp-pv-{namespace.metadata.name}-{name}"
            )
        )

        pv, pvc = await asyncio.gather(
            *create_debug_volume(
                core,
                None,
                namespace.metadata.name,
                name,
                {},
                logging.getLogger(__name__),
            )
        )
        await assert_wait_for(
            True, self.does_pv_exist, core, f"temp-pv-{namespace.metadata.name}-{name}",
        )
        await assert_wait_for(
            True,
            self.does_pvc_exist,
            core,
            namespace.metadata.name,
            f"local-resource-{name}",
        )


class TestStatefulSetAffinity:
    def test_testing_true(self, faker):
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", True):
            affinity = get_statefulset_affinity(name, logging.getLogger(__name__))

        assert affinity is None

    def test_testing_false(self, faker):
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", False):
            affinity = get_statefulset_affinity(name, logging.getLogger(__name__))

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

    def test_cloud_provider_aws(self, faker):
        name = faker.domain_word()
        with mock.patch("crate.operator.create.config.TESTING", False):
            with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", "aws"):
                affinity = get_statefulset_affinity(name, logging.getLogger(__name__))

        apa = affinity.pod_anti_affinity
        terms = apa.preferred_during_scheduling_ignored_during_execution[0]
        expressions = terms.pod_affinity_term.label_selector.match_expressions
        assert [e.to_dict() for e in expressions] == [
            {
                "key": "app.kubernetes.io/component",
                "operator": "In",
                "values": ["cratedb"],
            },
            {"key": "app.kubernetes.io/name", "operator": "In", "values": [name]},
        ]
        assert (
            terms.pod_affinity_term.topology_key
            == "failure-domain.beta.kubernetes.io/zone"
        )


class TestStatefulSetContainers:
    def test(self, faker, random_string):
        cpus = faker.pyfloat(min_value=0)
        memory = faker.numerify("%!!") + ".0" + faker.lexify("?i", "KMG")
        node_spec = {"resources": {"cpus": cpus, "memory": memory}}
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
        print(c_crate.resources.to_dict())
        assert c_crate.resources.to_dict() == {
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
            crate_node_name_prefix="data-node-",
            cluster_name="my-cluster",
            node_name="node",
            node_spec={"resources": {"cpus": 1, "disk": {"count": 1}}},
            cluster_settings=None,
            has_ssl=False,
            is_master=True,
            is_data=True,
        )
        assert ["/docker-entrypoint.sh", "crate"] == cmd[0:2]

    def test_cluster_name(self, random_string):
        cluster_name = random_string()
        cmd = get_statefulset_crate_command(
            namespace="some-namespace",
            name="cluster1",
            master_nodes=["data-node-0", "data-node-1", "data-node-2"],
            total_nodes_count=3,
            crate_node_name_prefix="data-node-",
            cluster_name=cluster_name,
            node_name="node",
            node_spec={"resources": {"cpus": 1, "disk": {"count": 1}}},
            cluster_settings=None,
            has_ssl=False,
            is_master=True,
            is_data=True,
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
            crate_node_name_prefix=crate_node_name_prefix,
            cluster_name="my-cluster",
            node_name=node_name,
            node_spec={"resources": {"cpus": 1, "disk": {"count": 1}}},
            cluster_settings=None,
            has_ssl=False,
            is_master=True,
            is_data=True,
        )
        assert (
            f"-Cnode.name={crate_node_name_prefix}$(hostname | rev | cut -d- -f1 | rev)"
            in cmd
        )
        assert f"-Cnode.attr.node_name={node_name}" in cmd

    @pytest.mark.parametrize(
        "total, quorum", [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (123, 62)]
    )
    def test_node_counts(self, total, quorum):
        cmd = get_statefulset_crate_command(
            namespace="some-namespace",
            name="cluster1",
            master_nodes=["node-0", "node-1", "node-2"],
            total_nodes_count=total,
            crate_node_name_prefix="node-",
            cluster_name="my-cluster",
            node_name="node",
            node_spec={"resources": {"cpus": 1, "disk": {"count": 1}}},
            cluster_settings=None,
            has_ssl=False,
            is_master=True,
            is_data=True,
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
            crate_node_name_prefix="node-",
            cluster_name="my-cluster",
            node_name="node",
            node_spec={"resources": {"cpus": 1, "disk": {"count": count}}},
            cluster_settings=None,
            has_ssl=False,
            is_master=True,
            is_data=True,
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
            crate_node_name_prefix="node-",
            cluster_name="my-cluster",
            node_name="node",
            node_spec={"resources": {"cpus": cpus, "disk": {"count": 1}}},
            cluster_settings=None,
            has_ssl=False,
            is_master=True,
            is_data=True,
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
            crate_node_name_prefix="node-",
            cluster_name="my-cluster",
            node_name="node",
            node_spec={"resources": {"cpus": 1, "disk": {"count": 1}}},
            cluster_settings=None,
            has_ssl=False,
            is_master=master,
            is_data=data,
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
            crate_node_name_prefix="node-",
            cluster_name="my-cluster",
            node_name="node",
            node_spec={"resources": {"cpus": 1, "disk": {"count": 1}}},
            cluster_settings=None,
            has_ssl=ssl,
            is_master=True,
            is_data=True,
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
            crate_node_name_prefix="node-",
            cluster_name="my-cluster",
            node_name="node",
            node_spec={
                "resources": {"cpus": 1, "disk": {"count": 1}},
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
        )
        assert "-Cauth.host_based.enabled=node-override" in cmd
        assert "-Cnode.attr.node_setting=node-override" in cmd
        assert "-Cnode.master=cluster-override" in cmd
        assert "-Cnode.attr.some_node_setting=node" in cmd
        assert "-Cnode.attr.some_cluster_setting=cluster" in cmd


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
        assert vm_resource.name == "resource"
        assert [(vm.mount_path, vm.name) for vm in vm_data] == [
            (f"/data/data{i}", f"data{i}") for i in range(disks)
        ]

    def test_with_ssl(self, faker):
        disks = faker.pyint(min_value=1, max_value=5)
        node_spec = {"resources": {"disk": {"count": disks}}}
        vm_jmxdir, vm_resource, *vm_data, vm_ssl = get_statefulset_crate_volume_mounts(
            node_spec, {}
        )
        assert vm_jmxdir.name == "jmxdir"
        assert vm_resource.name == "resource"
        assert [(vm.mount_path, vm.name) for vm in vm_data] == [
            (f"/data/data{i}", f"data{i}") for i in range(disks)
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
        assert [pvc.metadata.name for pvc in pvcs] == [f"data{i}" for i in range(count)]
        assert [pvc.spec.resources.requests["storage"] for pvc in pvcs] == [s] * count
        assert [pvc.spec.storage_class_name for pvc in pvcs] == [storage_class] * count


class TestStatefulSetVolumes:
    def test_without_ssl(self, faker):
        name = faker.domain_word()
        v_sql_exporter, v_resource, v_jmx = get_statefulset_volumes(name, None)
        assert v_sql_exporter.name == "crate-sql-exporter"
        assert v_resource.name == "resource"
        assert v_resource.persistent_volume_claim.claim_name == f"local-resource-{name}"
        assert v_jmx.name == "jmxdir"

    def test_with_ssl(self, faker):
        name = faker.domain_word()
        keystore_key = faker.domain_word()
        keystore_name = faker.domain_word()
        ssl = {
            "keystore": {"secretKeyRef": {"key": keystore_key, "name": keystore_name}}
        }
        v_sql_exporter, v_resource, v_jmx, v_keystore = get_statefulset_volumes(
            name, ssl
        )
        assert v_sql_exporter.name == "crate-sql-exporter"
        assert v_resource.name == "resource"
        assert v_resource.persistent_volume_claim.claim_name == f"local-resource-{name}"
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

    async def do_pods_exist(
        self, core: CoreV1Api, namespace: str, expected: Set[str]
    ) -> bool:
        pods = await core.list_namespaced_pod(namespace=namespace)
        return expected.issubset({p.metadata.name for p in pods.items})

    async def test_create(self, faker, namespace):
        apps = AppsV1Api()
        core = CoreV1Api()
        name = faker.domain_word()
        cluster_name = faker.domain_word()
        node_name = faker.domain_word()
        await create_statefulset(
            apps,
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
                    "cpus": 0.5,
                    "memory": "1Gi",
                    "heapRatio": 0.4,
                    "disk": {"count": 1, "size": "16Gi", "storageClass": "default"},
                },
            },
            ["master-1", "master-2", "master-3"],
            3,
            10000,
            20000,
            30000,
            40000,
            50000,
            "crate:4.1.5",
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
            self.do_pods_exist,
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

    async def test_create(self, faker, namespace):
        core = CoreV1Api()
        name = faker.domain_word()
        s_data, s_discovery = await asyncio.gather(
            *create_services(
                core,
                None,
                namespace.metadata.name,
                name,
                {},
                1,
                2,
                3,
                faker.domain_name(),
                logging.getLogger(__name__),
            )
        )
        await assert_wait_for(
            True,
            self.do_services_exist,
            core,
            namespace.metadata.name,
            {f"crate-{name}", f"crate-discovery-{name}"},
        )


@pytest.mark.k8s
@pytest.mark.asyncio
class TestSystemUser:
    async def does_secret_exist(
        self, core: CoreV1Api, namespace: str, name: str
    ) -> bool:
        secrets = await core.list_namespaced_secret(namespace)
        return name in (s.metadata.name for s in secrets.items)

    async def test_create(self, faker, namespace):
        core = CoreV1Api()
        name = faker.domain_word()
        password = faker.password(length=12)
        with mock.patch("crate.operator.create.gen_password", return_value=password):
            secret = await create_system_user(
                core,
                None,
                namespace.metadata.name,
                name,
                {},
                logging.getLogger(__name__),
            )
        await assert_wait_for(
            True,
            self.does_secret_exist,
            core,
            namespace.metadata.name,
            f"user-system-{name}",
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

    async def do_pods_exist(
        self, core: CoreV1Api, namespace: str, expected: Set[str]
    ) -> bool:
        pods = await core.list_namespaced_pod(namespace=namespace)
        return expected.issubset({p.metadata.name for p in pods.items})

    async def test_create_minimal(self, faker, namespace, cleanup_handler, kopf_runner):
        apps = AppsV1Api()
        coapi = CustomObjectsApi()
        core = CoreV1Api()
        name = faker.domain_word()

        # Clean up persistent volume after the test
        cleanup_handler.append(
            core.delete_persistent_volume(
                name=f"temp-pv-{namespace.metadata.name}-{name}"
            )
        )
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
                        "version": "4.1.5",
                    },
                    "nodes": {
                        "data": [
                            {
                                "name": "data",
                                "replicas": 3,
                                "resources": {
                                    "cpus": 0.5,
                                    "memory": "1Gi",
                                    "heapRatio": 0.25,
                                    "disk": {
                                        "storageClass": "default",
                                        "size": "16GiB",
                                        "count": 1,
                                    },
                                },
                            }
                        ]
                    },
                },
            },
        )
        await assert_wait_for(
            True,
            self.does_statefulset_exist,
            apps,
            namespace.metadata.name,
            f"crate-data-data-{name}",
        )
        await assert_wait_for(
            True,
            self.do_pods_exist,
            core,
            namespace.metadata.name,
            {f"crate-data-data-{name}-{i}" for i in range(3)},
        )
