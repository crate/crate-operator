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
from crate.operator.operations import is_lb_service_present
from crate.operator.utils.formatting import convert_to_bytes
from crate.operator.utils.kubeapi import get_host
from crate.operator.webhooks import WebhookEvent, WebhookStatus

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    create_test_sys_jobs_table,
    is_cluster_healthy,
    is_kopf_handler_finished,
    start_cluster,
    was_notification_sent,
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
@mock.patch("crate.operator.webhooks.webhook_client._send")
async def test_expand_cluster_storage(
    mock_send_notification,
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

    notification_success_call = mock.call(
        WebhookEvent.FEEDBACK,
        WebhookStatus.SUCCESS,
        namespace.metadata.name,
        name,
        feedback_data=mock.ANY,
        unsafe=mock.ANY,
        logger=mock.ANY,
    )

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

    # Make sure we wait for the cluster to be suspended
    await assert_wait_for(
        False,
        is_lb_service_present,
        core,
        namespace.metadata.name,
        name,
        err_msg="Load balancer check 3 timed out",
        timeout=DEFAULT_TIMEOUT,
    )

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

    # The host needs to be retrieved again because the IP address has changed.
    # This is due to suspending and resuming the cluster recreates the load balancer.
    # Make sure we wait for the cluster to be suspended.
    host = await get_host(core, namespace.metadata.name, name)

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
        timeout=DEFAULT_TIMEOUT * 5,
    )

    assert await was_notification_sent(
        mock_send_notification=mock_send_notification, call=notification_success_call
    ), "A success notification was expected but was not sent"


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
