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
from unittest import mock

import bitmath
import kopf
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
from crate.operator.utils.kubeapi import get_host
from crate.operator.webhooks import WebhookEvent, WebhookStatus

from .utils import (
    DEFAULT_TIMEOUT,
    assert_wait_for,
    create_test_sys_jobs_table,
    is_cluster_healthy,
    is_kopf_handler_finished,
    require_connection,
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
        retry=mock.ANY,
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
        connection_factory(*require_connection(host, password)),
        number_of_nodes,
        err_msg="Cluster wasn't healthy after 5 minutes.",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    conn_factory = connection_factory(*require_connection(host, password))
    await create_test_sys_jobs_table(conn_factory)

    await _expand_volume(coapi, name, namespace, "32Gi")

    # we just check if the PVC has been patched with the correct new disk size. We
    # do not wait for the PVC to be really resized because there is no guarantee
    # it is supported on the cluster we are running the tests on.
    await assert_wait_for(
        True,
        _all_pvcs_resized,
        core,
        namespace.metadata.name,
        "32Gi",
        err_msg="Volume expansion has not finished.",
        timeout=DEFAULT_TIMEOUT * 3,
    )

    await assert_wait_for(
        True,
        is_kopf_handler_finished,
        coapi,
        name,
        namespace.metadata.name,
        f"{KOPF_STATE_STORE_PREFIX}/cluster_update",
        err_msg="Cluster update has not finished",
        timeout=DEFAULT_TIMEOUT * 5,
    )

    # The host needs to be retrieved again because the IP address has changed.
    # This is due to suspending and resuming the cluster recreates the load balancer.
    # Make sure we wait for the cluster to be suspended.
    host = await get_host(core, namespace.metadata.name, name)

    # assert the cluster has been scaled up again to the initial number of nodes
    await assert_wait_for(
        True,
        is_cluster_healthy,
        connection_factory(*require_connection(host, password)),
        number_of_nodes,
        err_msg="Cluster wasn't back up again after 5 minutes.",
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


def _fake_pvc(*, requests_storage, capacity_storage, resize_pending=False):
    pvc = mock.Mock()
    pvc.spec.resources.requests = {"storage": requests_storage}
    pvc.status.capacity = {"storage": capacity_storage}
    condition = mock.Mock()
    condition.type = "FileSystemResizePending"
    pvc.status.conditions = [condition] if resize_pending else []
    return pvc


class TestExpandVolumePerGroup:
    @pytest.mark.asyncio
    @mock.patch("crate.operator.expand_volume.send_operation_progress_notification")
    @mock.patch(
        "crate.operator.expand_volume.get_pvcs_in_namespace",
        new_callable=mock.AsyncMock,
    )
    async def test_expands_master_pvcs_by_label(self, mock_get_pvcs, _mock_notify):
        from crate.operator.expand_volume import expand_volume

        mock_get_pvcs.return_value = [{"uid": "u", "name": "data0-crate-master-c-0"}]
        core = mock.Mock()
        # Already physically resized, so no retry is raised.
        core.read_namespaced_persistent_volume_claim = mock.AsyncMock(
            return_value=_fake_pvc(
                requests_storage="137438953472", capacity_storage="274877906944"
            )
        )
        core.patch_namespaced_persistent_volume_claim = mock.AsyncMock()

        await expand_volume(core, "ns", "c", [("master", "274877906944")], mock.Mock())

        # PVCs were looked up by the master node label.
        get_pvcs_call = mock_get_pvcs.await_args
        assert get_pvcs_call is not None
        assert get_pvcs_call.args[3] == "master"
        # The PVC was patched to the new size.
        patch_call = core.patch_namespaced_persistent_volume_claim.await_args
        assert patch_call is not None
        patch_body = patch_call.kwargs["body"]
        assert patch_body["spec"]["resources"]["requests"]["storage"] == "274877906944"

    @pytest.mark.asyncio
    @mock.patch("crate.operator.expand_volume.send_operation_progress_notification")
    @mock.patch(
        "crate.operator.expand_volume.get_pvcs_in_namespace",
        new_callable=mock.AsyncMock,
    )
    async def test_retries_while_resize_in_progress(self, mock_get_pvcs, _mock_notify):
        from crate.operator.expand_volume import expand_volume

        mock_get_pvcs.return_value = [{"uid": "u", "name": "data0-crate-master-c-0"}]
        core = mock.Mock()
        core.read_namespaced_persistent_volume_claim = mock.AsyncMock(
            return_value=_fake_pvc(
                requests_storage="137438953472", capacity_storage="137438953472"
            )
        )
        core.patch_namespaced_persistent_volume_claim = mock.AsyncMock()

        with pytest.raises(kopf.TemporaryError):
            await expand_volume(
                core, "ns", "c", [("master", "274877906944")], mock.Mock()
            )

    @pytest.mark.asyncio
    @mock.patch("crate.operator.expand_volume.send_operation_progress_notification")
    @mock.patch(
        "crate.operator.expand_volume.get_pvcs_in_namespace",
        new_callable=mock.AsyncMock,
    )
    async def test_noop_when_already_at_target_size(self, mock_get_pvcs, _mock_notify):
        from crate.operator.expand_volume import expand_volume

        mock_get_pvcs.return_value = [{"uid": "u", "name": "data0-crate-master-c-0"}]
        core = mock.Mock()
        core.read_namespaced_persistent_volume_claim = mock.AsyncMock(
            return_value=_fake_pvc(
                requests_storage="274877906944", capacity_storage="274877906944"
            )
        )
        core.patch_namespaced_persistent_volume_claim = mock.AsyncMock()

        await expand_volume(core, "ns", "c", [("master", "274877906944")], mock.Mock())

        core.patch_namespaced_persistent_volume_claim.assert_not_awaited()


def _disk_diff_data(old_size, new_size):
    return kopf.DiffItem(
        kopf.DiffOperation.CHANGE,
        ("spec", "nodes", "data"),
        [{"name": "hot", "resources": {"disk": {"size": old_size}}}],
        [{"name": "hot", "resources": {"disk": {"size": new_size}}}],
    )


def _disk_diff_master(old_size, new_size):
    return kopf.DiffItem(
        kopf.DiffOperation.CHANGE,
        ("spec", "nodes", "master", "resources", "disk", "size"),
        old_size,
        new_size,
    )


class TestCollectDiskExpansions:
    def test_collects_master_and_data_groups(self):
        from crate.operator.expand_volume import collect_disk_expansions

        diff = kopf.Diff(
            [_disk_diff_data("100", "200"), _disk_diff_master("50", "120")]
        )

        expansions = collect_disk_expansions(diff, logging.getLogger(__name__))

        assert ("hot", "200") in expansions
        assert ("master", "120") in expansions

    def test_skips_data_groups_with_unchanged_size(self):
        from crate.operator.expand_volume import collect_disk_expansions

        diff = kopf.Diff([_disk_diff_data("200", "200")])

        assert collect_disk_expansions(diff, logging.getLogger(__name__)) == []

    def test_master_only_disk_change(self):
        from crate.operator.expand_volume import collect_disk_expansions

        diff = kopf.Diff([_disk_diff_master("50", "120")])

        assert collect_disk_expansions(diff, logging.getLogger(__name__)) == [
            ("master", "120")
        ]
