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

import pytest
from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi

from crate.operator.constants import (
    API_GROUP,
    KOPF_STATE_STORE_PREFIX,
    RESOURCE_CRATEDB,
)
from crate.operator.utils.kubeapi import get_public_host_for_testing

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    is_kopf_handler_finished,
    start_cluster,
)

pytestmark = [pytest.mark.k8s, pytest.mark.asyncio]


@pytest.mark.parametrize(
    "initial, updated",
    [
        (None, ["0.0.0.0/0", "192.168.1.1/32"]),
        (["0.0.0.0/0"], ["0.0.0.0/0", "192.168.1.1/32"]),
        (["0.0.0.0/0"], []),
    ],
)
async def test_update_cidrs(
    initial, updated, faker, namespace, kopf_runner, api_client
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
        wait_for_healthy=True,
        additional_cluster_spec={"allowedCIDRs": initial},
    )

    await asyncio.wait_for(
        get_public_host_for_testing(core, namespace.metadata.name, name),
        # It takes a while to retrieve an external IP on AKS.
        timeout=DEFAULT_TIMEOUT * 5,
    )

    await coapi.patch_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        name=name,
        body=[
            {
                "op": "replace",
                "path": "/spec/cluster/allowedCIDRs",
                "value": updated,
            }
        ],
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/service_cidr_changes/spec.cluster.allowedCIDRs",
        err_msg="Scaling has not finished",
        timeout=DEFAULT_TIMEOUT,
    )

    await assert_wait_for(
        True,
        _are_source_ranges_updated,
        core,
        name,
        namespace.metadata.name,
        updated,
        err_msg="Source ranges have not been updated to the expected ones",
        timeout=DEFAULT_TIMEOUT,
    )


async def _are_source_ranges_updated(core, name, namespace, cidr_list):
    service = await core.read_namespaced_service(f"crate-{name}", namespace)
    actual = cidr_list if len(cidr_list) > 0 else None
    return service.spec.load_balancer_source_ranges == actual
