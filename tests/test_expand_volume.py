# CrateDB Kubernetes Operator
# Copyright (C) 2022 Crate.IO GmbH
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

import bitmath
import pytest
from kubernetes_asyncio.client import (
    CoreV1Api,
    CustomObjectsApi,
    StorageV1Api,
    V1Namespace,
)

from crate.operator.constants import (
    API_GROUP,
    DATA_PVC_NAME_PREFIX,
    KOPF_STATE_STORE_PREFIX,
    RESOURCE_CRATEDB,
)
from crate.operator.cratedb import connection_factory
from crate.operator.utils.formatting import convert_to_bytes

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    create_test_sys_jobs_table,
    is_cluster_healthy,
    is_kopf_handler_finished,
    start_cluster,
)


@pytest.mark.parametrize(
    "input,expected",
    [
        ("16Gi", 17179869184),
        ("256GiB", 274877906944),
        ("34359738368", 34359738368),
        (68719476736, 68719476736),
    ],
)
def test_convert_to_bytes(input, expected):
    assert convert_to_bytes(input) == bitmath.Byte(expected)


@pytest.mark.k8s
@pytest.mark.asyncio
async def test_expand_cluster_storage(
    faker,
    namespace,
    kopf_runner,
    api_client,
):
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    storage = StorageV1Api(api_client)
    name = faker.domain_word()
    number_of_nodes = 2

    expansion_supported = await _is_volume_expansion_supported(storage)
    if expansion_supported is not True:
        pytest.skip("The `default` StorageClass does not support volume expansion.")

    host, password = await start_cluster(
        name,
        namespace,
        core,
        coapi,
        number_of_nodes,
    )

    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        number_of_nodes,
        err_msg="Cluster wasn't healthy after 5 minutes.",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    conn_factory = connection_factory(host, password)
    await create_test_sys_jobs_table(conn_factory)

    await _expand_volume(coapi, name, namespace, "32Gi")

    # we just check if the PVC has been patched with the correct new disk size. We
    # do not wait for the PVC to be really resized because there is no guarantee
    # it is supported.
    await assert_wait_for(
        True,
        _all_pvcs_resized,
        core,
        namespace.metadata.name,
        "32Gi",
        err_msg="Volume expansion has not finished.",
        timeout=DEFAULT_TIMEOUT * 3,
    )

    # assert the cluster has been scaled up again to the initial number of nodes
    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(host, password),
        number_of_nodes,
        err_msg="Cluster wasn't back up again after 5 minutes.",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Volume Expansion handler has not finished.",
        timeout=DEFAULT_TIMEOUT,
    )


async def _all_pvcs_resized(
    core: CoreV1Api, namespace: str, expected_size: str
) -> bool:
    pvcs = await core.list_namespaced_persistent_volume_claim(namespace=namespace)
    for pvc in pvcs.items:
        if pvc.metadata.name.startswith(DATA_PVC_NAME_PREFIX) and convert_to_bytes(
            pvc.spec.resources.requests["storage"]
        ) != convert_to_bytes(expected_size):
            return False

    return True


async def _is_volume_expansion_supported(storage: StorageV1Api) -> bool:
    sc = await storage.read_storage_class(name="default")

    return sc.allow_volume_expansion is True


async def _expand_volume(
    coapi: CustomObjectsApi, name: str, namespace: V1Namespace, disk_size: str
):
    patch_body = [
        {
            "op": "replace",
            "path": "/spec/nodes/data/0/resources/disk/size",
            "value": disk_size,
        }
    ]
    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=patch_body,
    )
