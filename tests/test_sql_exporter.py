# CrateDB Kubernetes Operator
#
# Licensed to Crate.IO GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
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

import itertools
import logging
from typing import Optional
from unittest import mock

import kopf
import pytest
from kubernetes_asyncio.client import (
    AppsV1Api,
    CoreV1Api,
    CustomObjectsApi,
    V1ConfigMap,
    V1Container,
    V1ContainerStatus,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1PodStatus,
    V1PodTemplateSpec,
    V1StatefulSet,
    V1StatefulSetSpec,
)
from kubernetes_asyncio.client.exceptions import ApiException

from crate.operator import sql_exporter
from crate.operator.config import config
from crate.operator.constants import (
    LABEL_COMPONENT,
    LABEL_NAME,
    SQL_EXPORTER_CONFIG_HASH_ANNOTATION,
    SQL_EXPORTER_CONFIG_HASH_KEY,
    SQL_EXPORTER_CONTAINER,
)
from crate.operator.create import get_sql_exporter_config

from .utils import DEFAULT_TIMEOUT, assert_wait_for, start_cluster

OLD_IMAGE = "sql_exporter:old"
NEW_IMAGE = "sql_exporter:new"
NEW_HASH = "new-hash"
LOGGER = logging.getLogger(__name__)


def make_pod(
    name: str = "crate-data-hot-c-0",
    image: str = OLD_IMAGE,
    config_hash: Optional[str] = NEW_HASH,
    restart_count: int = 0,
    ready: bool = True,
    phase: str = "Running",
) -> V1Pod:
    annotations = (
        {SQL_EXPORTER_CONFIG_HASH_ANNOTATION: config_hash} if config_hash else None
    )
    return V1Pod(
        metadata=V1ObjectMeta(name=name, annotations=annotations),
        spec=V1PodSpec(
            containers=[
                V1Container(name=SQL_EXPORTER_CONTAINER, image=image),
                V1Container(name="crate", image="crate:1"),
            ]
        ),
        status=V1PodStatus(
            phase=phase,
            container_statuses=[
                V1ContainerStatus(
                    name=SQL_EXPORTER_CONTAINER,
                    image=image,
                    image_id="",
                    ready=ready,
                    restart_count=restart_count,
                )
            ],
        ),
    )


def make_statefulset(
    image: str = OLD_IMAGE, config_hash: Optional[str] = None
) -> V1StatefulSet:
    annotations = {"prometheus.io/scrape": "true"}
    if config_hash:
        annotations[SQL_EXPORTER_CONFIG_HASH_ANNOTATION] = config_hash
    return V1StatefulSet(
        metadata=V1ObjectMeta(name="crate-data-hot-c"),
        spec=V1StatefulSetSpec(
            selector=mock.Mock(),
            service_name="cratedb",
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(annotations=annotations),
                spec=V1PodSpec(
                    containers=[
                        V1Container(name=SQL_EXPORTER_CONTAINER, image=image),
                        V1Container(name="crate", image="crate:1"),
                    ]
                ),
            ),
        ),
    )


@pytest.fixture
def apis():
    core = mock.AsyncMock()
    apps = mock.AsyncMock()
    api_client = mock.MagicMock()
    api_client.return_value.__aenter__ = mock.AsyncMock()
    api_client.return_value.__aexit__ = mock.AsyncMock(return_value=False)
    with (
        mock.patch.object(sql_exporter, "GlobalApiClient", api_client),
        mock.patch.object(sql_exporter, "CoreV1Api", return_value=core),
        mock.patch.object(sql_exporter, "AppsV1Api", return_value=apps),
        mock.patch.object(sql_exporter.asyncio, "sleep", mock.AsyncMock()),
        mock.patch.object(sql_exporter, "_exec", mock.AsyncMock()) as exec_,
    ):
        yield mock.Mock(core=core, apps=apps, exec=exec_)


async def test_pod_up_to_date_is_left_alone(apis):
    await sql_exporter.update_pod(
        "ns", make_pod(image=NEW_IMAGE), NEW_IMAGE, NEW_HASH, LOGGER
    )

    apis.core.patch_namespaced_pod.assert_not_called()
    apis.exec.assert_not_called()


@pytest.mark.parametrize("phase", ["Pending", "Failed"])
async def test_pod_not_running_is_skipped(apis, phase):
    await sql_exporter.update_pod(
        "ns", make_pod(phase=phase), NEW_IMAGE, NEW_HASH, LOGGER
    )

    apis.core.patch_namespaced_pod.assert_not_called()


async def test_pod_image_is_patched_in_place(apis):
    apis.core.read_namespaced_pod.side_effect = [
        make_pod(image=NEW_IMAGE, restart_count=0, ready=False),
        make_pod(image=NEW_IMAGE, restart_count=1, ready=True),
    ]

    await sql_exporter.update_pod("ns", make_pod(), NEW_IMAGE, NEW_HASH, LOGGER)

    apis.core.patch_namespaced_pod.assert_awaited_once_with(
        namespace="ns",
        name="crate-data-hot-c-0",
        body={
            "metadata": {
                "annotations": {SQL_EXPORTER_CONFIG_HASH_ANNOTATION: NEW_HASH}
            },
            "spec": {
                "containers": [{"name": SQL_EXPORTER_CONTAINER, "image": NEW_IMAGE}]
            },
        },
    )
    assert apis.core.read_namespaced_pod.await_count == 2
    apis.exec.assert_not_called()


async def test_pod_image_is_reverted_when_exporter_does_not_start(apis):
    apis.core.read_namespaced_pod.side_effect = itertools.chain(
        [ApiException(status=500)],
        itertools.repeat(make_pod(image=NEW_IMAGE, ready=False)),
    )

    with pytest.raises(kopf.PermanentError):
        await sql_exporter.update_pod("ns", make_pod(), NEW_IMAGE, NEW_HASH, LOGGER)

    assert apis.core.patch_namespaced_pod.await_args_list[-1] == mock.call(
        namespace="ns",
        name="crate-data-hot-c-0",
        body={
            "spec": {
                "containers": [{"name": SQL_EXPORTER_CONTAINER, "image": OLD_IMAGE}]
            }
        },
    )


async def test_pod_config_is_reloaded_once_projected(apis):
    apis.exec.side_effect = ["old-hash\n", f"{NEW_HASH}\n", None]
    apis.core.read_namespaced_pod.return_value = make_pod(
        image=NEW_IMAGE, config_hash="old-hash", restart_count=3
    )
    pod = make_pod(image=NEW_IMAGE, config_hash="old-hash", restart_count=2)

    await sql_exporter.update_pod("ns", pod, NEW_IMAGE, NEW_HASH, LOGGER)

    assert [call.args[2] for call in apis.exec.await_args_list] == [
        ["cat", f"/config/{SQL_EXPORTER_CONFIG_HASH_KEY}"],
        ["cat", f"/config/{SQL_EXPORTER_CONFIG_HASH_KEY}"],
        ["/bin/sh", "-c", "kill 1"],
    ]
    apis.core.patch_namespaced_pod.assert_awaited_once_with(
        namespace="ns",
        name="crate-data-hot-c-0",
        body={
            "metadata": {"annotations": {SQL_EXPORTER_CONFIG_HASH_ANNOTATION: NEW_HASH}}
        },
    )


async def test_pod_config_not_projected_raises_temporary_error(apis):
    apis.exec.return_value = "old-hash"
    pod = make_pod(image=NEW_IMAGE, config_hash=None)

    with pytest.raises(kopf.TemporaryError):
        await sql_exporter.update_pod("ns", pod, NEW_IMAGE, NEW_HASH, LOGGER)

    apis.core.patch_namespaced_pod.assert_not_called()
    assert ["/bin/sh", "-c", "kill 1"] not in [
        call.args[2] for call in apis.exec.await_args_list
    ]


async def test_pod_image_is_updated_without_projected_config(apis):
    apis.exec.return_value = None
    apis.core.read_namespaced_pod.return_value = make_pod(
        image=NEW_IMAGE, config_hash=None, restart_count=1
    )

    with pytest.raises(kopf.TemporaryError):
        await sql_exporter.update_pod(
            "ns", make_pod(config_hash=None), NEW_IMAGE, NEW_HASH, LOGGER
        )

    apis.core.patch_namespaced_pod.assert_awaited_once_with(
        namespace="ns",
        name="crate-data-hot-c-0",
        body={
            "spec": {
                "containers": [{"name": SQL_EXPORTER_CONTAINER, "image": NEW_IMAGE}]
            }
        },
    )


async def test_pod_config_reload_without_restart_is_not_recorded(apis):
    apis.exec.return_value = NEW_HASH
    apis.core.read_namespaced_pod.return_value = make_pod(image=NEW_IMAGE)
    pod = make_pod(image=NEW_IMAGE, config_hash=None)

    with pytest.raises(kopf.TemporaryError):
        await sql_exporter.update_pod("ns", pod, NEW_IMAGE, NEW_HASH, LOGGER)

    apis.core.patch_namespaced_pod.assert_not_called()


async def test_pod_gone_is_skipped(apis):
    apis.core.patch_namespaced_pod.side_effect = ApiException(status=404)

    await sql_exporter.update_pod("ns", make_pod(), NEW_IMAGE, NEW_HASH, LOGGER)


async def test_pods_are_updated_one_after_the_other(apis):
    apis.core.list_namespaced_pod.return_value.items = [
        make_pod(name="crate-data-hot-c-1"),
        make_pod(name="crate-data-hot-c-0"),
    ]
    events = []

    async def patch_pod(namespace, name, body):
        events.append(("patch", name))

    async def read_pod(namespace, name):
        if ("patch", name) not in events:
            return make_pod(name=name, restart_count=5)
        events.append(("ready", name))
        return make_pod(name=name, image=NEW_IMAGE, restart_count=6)

    apis.core.patch_namespaced_pod.side_effect = patch_pod
    apis.core.read_namespaced_pod.side_effect = read_pod

    await sql_exporter.update_pods("ns", "c", NEW_IMAGE, NEW_HASH, LOGGER)

    apis.core.list_namespaced_pod.assert_awaited_once_with(
        namespace="ns", label_selector=f"{LABEL_COMPONENT}=cratedb,{LABEL_NAME}=c"
    )
    assert events == [
        ("patch", "crate-data-hot-c-0"),
        ("ready", "crate-data-hot-c-0"),
        ("patch", "crate-data-hot-c-1"),
        ("ready", "crate-data-hot-c-1"),
    ]


async def test_statefulset_is_patched(apis):
    apis.apps.list_namespaced_stateful_set.return_value.items = [make_statefulset()]

    await sql_exporter.update_statefulsets("ns", "c", NEW_IMAGE, NEW_HASH, LOGGER)

    apis.apps.patch_namespaced_stateful_set.assert_awaited_once_with(
        namespace="ns",
        name="crate-data-hot-c",
        body={
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {SQL_EXPORTER_CONFIG_HASH_ANNOTATION: NEW_HASH}
                    },
                    "spec": {
                        "containers": [
                            {"name": SQL_EXPORTER_CONTAINER, "image": NEW_IMAGE}
                        ]
                    },
                }
            }
        },
    )


async def test_statefulset_up_to_date_is_left_alone(apis):
    apis.apps.list_namespaced_stateful_set.return_value.items = [
        make_statefulset(image=NEW_IMAGE, config_hash=NEW_HASH)
    ]

    await sql_exporter.update_statefulsets("ns", "c", NEW_IMAGE, NEW_HASH, LOGGER)

    apis.apps.patch_namespaced_stateful_set.assert_not_called()


async def test_config_map_is_patched_and_obsolete_files_removed(apis):
    apis.core.read_namespaced_config_map.return_value = V1ConfigMap(
        data={"sql-exporter.yaml": "old", "gone-collector.yaml": "x"}
    )
    data = {"sql-exporter.yaml": "new", SQL_EXPORTER_CONFIG_HASH_KEY: NEW_HASH}

    assert await sql_exporter.update_config_map("ns", "c", data, LOGGER) is True

    apis.core.patch_namespaced_config_map.assert_awaited_once_with(
        namespace="ns",
        name="crate-sql-exporter-c",
        body={"data": {"gone-collector.yaml": None, **data}},
    )


async def test_config_map_up_to_date_is_left_alone(apis):
    data = {"sql-exporter.yaml": "new", SQL_EXPORTER_CONFIG_HASH_KEY: NEW_HASH}
    apis.core.read_namespaced_config_map.return_value = V1ConfigMap(data=dict(data))

    assert await sql_exporter.update_config_map("ns", "c", data, LOGGER) is True

    apis.core.patch_namespaced_config_map.assert_not_called()


async def test_cluster_without_config_map_is_skipped(apis):
    apis.core.read_namespaced_config_map.side_effect = ApiException(status=404)

    await sql_exporter.update_sql_exporter("ns", "c", LOGGER)

    apis.core.list_namespaced_pod.assert_not_called()
    apis.apps.list_namespaced_stateful_set.assert_not_called()


async def test_pods_are_updated_before_statefulsets(apis):
    expected = get_sql_exporter_config(None, "c", {})
    apis.core.read_namespaced_config_map.return_value = V1ConfigMap(data=expected.data)
    apis.core.list_namespaced_pod.return_value.items = []
    apis.apps.list_namespaced_stateful_set.return_value.items = []
    calls = mock.Mock()
    calls.attach_mock(apis.core.list_namespaced_pod, "pods")
    calls.attach_mock(apis.apps.list_namespaced_stateful_set, "statefulsets")

    await sql_exporter.update_sql_exporter("ns", "c", LOGGER)

    assert [call[0] for call in calls.mock_calls] == ["pods", "statefulsets", "pods"]


def test_config_hash_follows_the_configuration():
    first = get_sql_exporter_config(None, "a", {})
    second = get_sql_exporter_config(None, "b", {})

    assert len(first.data[SQL_EXPORTER_CONFIG_HASH_KEY]) == 64
    assert first.data[SQL_EXPORTER_CONFIG_HASH_KEY] == (
        second.data[SQL_EXPORTER_CONFIG_HASH_KEY]
    )


async def _sql_exporter_state(core: CoreV1Api, namespace: str, name: str):
    pods = await core.list_namespaced_pod(
        namespace=namespace, label_selector=f"{LABEL_NAME}={name}"
    )
    pod = pods.items[0]
    statuses = {s.name: s for s in pod.status.container_statuses}
    return pod, statuses


async def _is_sql_exporter_restarted(
    core: CoreV1Api, namespace: str, name: str, image: str, restart_count: int
) -> bool:
    pod, statuses = await _sql_exporter_state(core, namespace, name)
    exporter = statuses[SQL_EXPORTER_CONTAINER]
    return (
        pod.spec.containers[0].image == image
        and exporter.ready
        and exporter.restart_count == restart_count
    )


@pytest.mark.k8s
async def test_update_sql_exporter(faker, namespace, kopf_runner, api_client):
    apps = AppsV1Api(api_client)
    coapi = CustomObjectsApi(api_client)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    ns = namespace.metadata.name
    image = "burningalchemist/sql_exporter:0.24.8"
    assert image != config.SQL_EXPORTER_IMAGE

    await start_cluster(name, namespace, core, coapi, 1)
    pod_before, _ = await _sql_exporter_state(core, ns, name)

    with mock.patch.object(config, "SQL_EXPORTER_IMAGE", image):
        await sql_exporter.update_sql_exporter(ns, name, LOGGER)

    await assert_wait_for(
        True,
        _is_sql_exporter_restarted,
        core,
        ns,
        name,
        image,
        1,
        err_msg="SQL exporter image has not been updated.",
        timeout=DEFAULT_TIMEOUT,
    )
    statefulsets = await apps.list_namespaced_stateful_set(
        namespace=ns, label_selector=f"{LABEL_NAME}={name}"
    )
    assert [
        sts.spec.template.spec.containers[0].image for sts in statefulsets.items
    ] == [image]

    changed = get_sql_exporter_config(None, name, {})
    changed.data["sql-exporter.yaml"] += "\n# changed\n"
    changed.data[SQL_EXPORTER_CONFIG_HASH_KEY] = "changed"
    with (
        mock.patch.object(config, "SQL_EXPORTER_IMAGE", image),
        mock.patch.object(
            sql_exporter, "get_sql_exporter_config", return_value=changed
        ),
    ):
        await sql_exporter.update_sql_exporter(ns, name, LOGGER)

    await assert_wait_for(
        True,
        _is_sql_exporter_restarted,
        core,
        ns,
        name,
        image,
        2,
        err_msg="SQL exporter configuration has not been reloaded.",
        timeout=DEFAULT_TIMEOUT,
    )
    pod_after, statuses = await _sql_exporter_state(core, ns, name)
    assert pod_after.metadata.uid == pod_before.metadata.uid
    assert statuses["crate"].restart_count == 0
    assert pod_after.metadata.annotations[SQL_EXPORTER_CONFIG_HASH_ANNOTATION] == (
        "changed"
    )
