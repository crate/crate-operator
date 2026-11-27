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

import asyncio
import itertools
import json
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
    V1ContainerState,
    V1ContainerStateWaiting,
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
    SQL_EXPORTER_PREVIOUS_CONFIG_ANNOTATION,
    SQL_EXPORTER_ROLLOUT_ANNOTATION,
    CloudProvider,
)
from crate.operator.create import get_sql_exporter_config

from .utils import DEFAULT_TIMEOUT, assert_wait_for, start_cluster

OLD_IMAGE = "sql_exporter:old"
NEW_IMAGE = "sql_exporter:new"
NEW_HASH = "new-hash"
LOGGER = logging.getLogger(__name__)


def make_pod(
    name: str = "crate-data-hot-c-0",
    uid: str = "uid-0",
    rollout_from: Optional[str] = None,
    image: str = OLD_IMAGE,
    config_hash: Optional[str] = NEW_HASH,
    restart_count: int = 0,
    ready: bool = True,
    phase: str = "Running",
    waiting_reason: Optional[str] = None,
) -> V1Pod:
    annotations = {}
    if config_hash:
        annotations[SQL_EXPORTER_CONFIG_HASH_ANNOTATION] = config_hash
    if rollout_from:
        annotations[SQL_EXPORTER_ROLLOUT_ANNOTATION] = rollout_from
    return V1Pod(
        metadata=V1ObjectMeta(name=name, uid=uid, annotations=annotations or None),
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
                    state=V1ContainerState(
                        waiting=(
                            V1ContainerStateWaiting(reason=waiting_reason)
                            if waiting_reason
                            else None
                        )
                    ),
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


@pytest.mark.parametrize(
    "enabled, provider, expected",
    [
        (True, CloudProvider.AWS, True),
        (True, None, True),
        (False, CloudProvider.AWS, False),
        (True, CloudProvider.OPENSHIFT, False),
    ],
)
def test_auto_update_is_not_run_on_openshift(enabled, provider, expected):
    with (
        mock.patch.object(config, "SQL_EXPORTER_AUTO_UPDATE", enabled),
        mock.patch.object(config, "CLOUD_PROVIDER", provider),
    ):
        assert sql_exporter.sql_exporter_auto_update_enabled() is expected


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

    assert [
        c.kwargs["body"] for c in apis.core.patch_namespaced_pod.await_args_list
    ] == [
        {
            "metadata": {"annotations": {SQL_EXPORTER_ROLLOUT_ANNOTATION: OLD_IMAGE}},
            "spec": {
                "containers": [{"name": SQL_EXPORTER_CONTAINER, "image": NEW_IMAGE}]
            },
        },
        {
            "metadata": {
                "annotations": {
                    SQL_EXPORTER_ROLLOUT_ANNOTATION: None,
                    SQL_EXPORTER_CONFIG_HASH_ANNOTATION: NEW_HASH,
                }
            }
        },
    ]
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
            "metadata": {"annotations": {SQL_EXPORTER_ROLLOUT_ANNOTATION: None}},
            "spec": {
                "containers": [{"name": SQL_EXPORTER_CONTAINER, "image": OLD_IMAGE}]
            },
        },
    )


async def test_pod_config_is_reloaded_once_projected(apis):
    apis.exec.side_effect = ["old-hash\n", f"{NEW_HASH}\n", None]
    apis.core.read_namespaced_pod.side_effect = [
        make_pod(image=NEW_IMAGE, config_hash="old-hash", restart_count=3),
        make_pod(image=NEW_IMAGE, config_hash="old-hash", restart_count=4),
    ]
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

    assert [
        c.kwargs["body"] for c in apis.core.patch_namespaced_pod.await_args_list
    ] == [
        {
            "metadata": {"annotations": {SQL_EXPORTER_ROLLOUT_ANNOTATION: OLD_IMAGE}},
            "spec": {
                "containers": [{"name": SQL_EXPORTER_CONTAINER, "image": NEW_IMAGE}]
            },
        },
        {"metadata": {"annotations": {SQL_EXPORTER_ROLLOUT_ANNOTATION: None}}},
    ]


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


async def test_pod_replaced_while_waiting_is_not_reverted(apis):
    apis.core.read_namespaced_pod.return_value = make_pod(
        image=OLD_IMAGE, uid="uid-1", restart_count=0
    )

    await sql_exporter.update_pod("ns", make_pod(), NEW_IMAGE, NEW_HASH, LOGGER)

    assert len(apis.core.patch_namespaced_pod.await_args_list) == 1


async def test_pod_restart_before_the_baseline_does_not_count(apis):
    apis.exec.side_effect = [f"{NEW_HASH}\n", None]
    apis.core.read_namespaced_pod.side_effect = [
        make_pod(image=NEW_IMAGE, config_hash="old-hash", restart_count=3),
        make_pod(image=NEW_IMAGE, config_hash="old-hash", restart_count=3),
        make_pod(image=NEW_IMAGE, config_hash="old-hash", restart_count=4),
    ]
    pod = make_pod(image=NEW_IMAGE, config_hash="old-hash", restart_count=2)

    await sql_exporter.update_pod("ns", pod, NEW_IMAGE, NEW_HASH, LOGGER)

    assert apis.core.read_namespaced_pod.await_count == 3


async def test_unverified_rollout_is_confirmed_before_it_is_cleared(apis):
    apis.core.read_namespaced_pod.return_value = make_pod(image=NEW_IMAGE)
    pod = make_pod(image=NEW_IMAGE, rollout_from=OLD_IMAGE)

    await sql_exporter.update_pod("ns", pod, NEW_IMAGE, NEW_HASH, LOGGER)

    apis.core.patch_namespaced_pod.assert_awaited_once_with(
        namespace="ns",
        name="crate-data-hot-c-0",
        body={"metadata": {"annotations": {SQL_EXPORTER_ROLLOUT_ANNOTATION: None}}},
    )


async def test_unverified_rollout_is_reverted_when_exporter_is_not_ready(apis):
    apis.core.read_namespaced_pod.return_value = make_pod(image=NEW_IMAGE, ready=False)
    pod = make_pod(image=NEW_IMAGE, rollout_from=OLD_IMAGE)

    with pytest.raises(kopf.PermanentError):
        await sql_exporter.update_pod("ns", pod, NEW_IMAGE, NEW_HASH, LOGGER)

    assert apis.core.patch_namespaced_pod.await_args.kwargs["body"]["spec"] == {
        "containers": [{"name": SQL_EXPORTER_CONTAINER, "image": OLD_IMAGE}]
    }


async def test_stale_pod_that_is_not_running_is_reported(apis):
    assert await sql_exporter.update_pod(
        "ns", make_pod(phase="Pending"), NEW_IMAGE, NEW_HASH, LOGGER
    )


async def test_up_to_date_pod_that_is_not_running_is_not_reported(apis):
    assert not await sql_exporter.update_pod(
        "ns", make_pod(image=NEW_IMAGE, phase="Pending"), NEW_IMAGE, NEW_HASH, LOGGER
    )


async def test_pending_stale_pod_requeues_the_update(apis):
    expected = get_sql_exporter_config(None, "c", {})
    apis.core.read_namespaced_config_map.return_value = V1ConfigMap(data=expected.data)
    apis.core.list_namespaced_pod.return_value.items = [make_pod(phase="Pending")]
    apis.core.read_namespaced_pod.return_value = make_pod(phase="Pending")
    apis.apps.list_namespaced_stateful_set.return_value.items = []

    with pytest.raises(kopf.TemporaryError):
        await sql_exporter.update_sql_exporter("ns", "c", LOGGER)


@pytest.mark.parametrize("reason", sorted(sql_exporter.IMAGE_PULL_FAILURES))
async def test_image_that_cannot_be_pulled_is_reverted_without_waiting(apis, reason):
    apis.core.read_namespaced_pod.return_value = make_pod(
        image=NEW_IMAGE, ready=False, waiting_reason=reason
    )

    with pytest.raises(kopf.PermanentError):
        await sql_exporter.update_pod("ns", make_pod(), NEW_IMAGE, NEW_HASH, LOGGER)

    assert apis.core.read_namespaced_pod.await_count == 1
    assert apis.core.patch_namespaced_pod.await_args.kwargs["body"]["spec"] == {
        "containers": [{"name": SQL_EXPORTER_CONTAINER, "image": OLD_IMAGE}]
    }


async def test_image_that_is_being_pulled_is_waited_for(apis):
    apis.core.read_namespaced_pod.side_effect = [
        make_pod(image=NEW_IMAGE, ready=False, waiting_reason="ContainerCreating"),
        make_pod(image=NEW_IMAGE, restart_count=1),
    ]

    await sql_exporter.update_pod("ns", make_pod(), NEW_IMAGE, NEW_HASH, LOGGER)

    assert apis.core.read_namespaced_pod.await_count == 2


def _cluster_with_unstartable_image(apis, pods):
    """
    Cluster whose ConfigMap is outdated and whose stale pods never start the
    new image.
    """
    new = get_sql_exporter_config(None, "c", {}).data
    old = {"sql-exporter.yaml": "old", "gone-collector.yaml": "x"}
    apis.exec.return_value = new[SQL_EXPORTER_CONFIG_HASH_KEY]
    apis.core.read_namespaced_config_map.side_effect = [
        V1ConfigMap(data=old),
        V1ConfigMap(data=new),
    ]
    apis.core.list_namespaced_pod.return_value.items = pods
    started = set()

    async def patch_pod(namespace, name, body):
        if "spec" in body:
            started.add(name)

    async def read_pod(namespace, name):
        if name in started:
            return make_pod(name=name, image=NEW_IMAGE, ready=False)
        return pods_by_name[name]

    pods_by_name = {pod.metadata.name: pod for pod in pods}
    apis.core.patch_namespaced_pod.side_effect = patch_pod
    apis.core.read_namespaced_pod.side_effect = read_pod
    return old, new


async def test_config_map_is_restored_when_the_first_rollout_is_reverted(apis):
    old, new = _cluster_with_unstartable_image(
        apis, [make_pod(name="crate-data-hot-c-0", config_hash="old-hash")]
    )

    with mock.patch.object(config, "SQL_EXPORTER_IMAGE", NEW_IMAGE):
        with pytest.raises(kopf.PermanentError):
            await sql_exporter.update_sql_exporter("ns", "c", LOGGER)

    assert apis.core.patch_namespaced_config_map.await_args_list[-1] == mock.call(
        namespace="ns",
        name="crate-sql-exporter-c",
        body={
            "metadata": {
                "annotations": {
                    SQL_EXPORTER_PREVIOUS_CONFIG_ANNOTATION: json.dumps(new)
                }
            },
            "data": {**{key: None for key in new if key not in old}, **old},
        },
    )
    apis.apps.patch_namespaced_stateful_set.assert_not_called()


async def test_config_map_is_kept_when_a_pod_already_uses_it(apis):
    new_hash = get_sql_exporter_config(None, "c", {}).data[SQL_EXPORTER_CONFIG_HASH_KEY]
    _cluster_with_unstartable_image(
        apis,
        [
            make_pod(name="crate-data-hot-c-0", image=NEW_IMAGE, config_hash=new_hash),
            make_pod(name="crate-data-hot-c-1", config_hash="old-hash"),
        ],
    )

    with mock.patch.object(config, "SQL_EXPORTER_IMAGE", NEW_IMAGE):
        with pytest.raises(kopf.PermanentError):
            await sql_exporter.update_sql_exporter("ns", "c", LOGGER)

    apis.core.patch_namespaced_config_map.assert_awaited_once()


async def test_cluster_updates_beyond_the_limit_are_requeued():
    release = asyncio.Event()

    async def update(namespace, name, logger):
        await release.wait()

    with (
        mock.patch.object(sql_exporter, "_update_slots", None),
        mock.patch.object(config, "SQL_EXPORTER_UPDATE_CONCURRENCY", 2),
        mock.patch.object(sql_exporter, "_update_sql_exporter", update),
    ):
        running = [
            asyncio.ensure_future(
                sql_exporter.update_sql_exporter("ns", f"c{i}", LOGGER)
            )
            for i in range(2)
        ]
        await asyncio.sleep(0)

        with pytest.raises(kopf.TemporaryError):
            await sql_exporter.update_sql_exporter("ns", "c2", LOGGER)

        release.set()
        await asyncio.gather(*running)
        await sql_exporter.update_sql_exporter("ns", "c2", LOGGER)


async def test_statefulset_is_not_patched_while_a_stale_pod_is_not_running(apis):
    expected = get_sql_exporter_config(None, "c", {})
    apis.core.read_namespaced_config_map.return_value = V1ConfigMap(data=expected.data)
    apis.core.list_namespaced_pod.return_value.items = [make_pod(phase="Pending")]
    apis.core.read_namespaced_pod.return_value = make_pod(phase="Pending")
    apis.apps.list_namespaced_stateful_set.return_value.items = [make_statefulset()]

    with pytest.raises(kopf.TemporaryError):
        await sql_exporter.update_sql_exporter("ns", "c", LOGGER)

    apis.apps.patch_namespaced_stateful_set.assert_not_called()


async def test_pod_without_projected_config_does_not_hold_back_the_others(apis):
    apis.exec.return_value = "old-hash"
    pods = {
        "crate-data-hot-c-0": make_pod(
            name="crate-data-hot-c-0", image=NEW_IMAGE, config_hash=None
        ),
        "crate-data-hot-c-1": make_pod(name="crate-data-hot-c-1"),
    }
    apis.core.list_namespaced_pod.return_value.items = list(pods.values())
    patched = []

    async def patch_pod(namespace, name, body):
        if "spec" in body:
            patched.append(name)

    async def read_pod(namespace, name):
        if name in patched:
            return make_pod(name=name, image=NEW_IMAGE, restart_count=1)
        return pods[name]

    apis.core.patch_namespaced_pod.side_effect = patch_pod
    apis.core.read_namespaced_pod.side_effect = read_pod

    with pytest.raises(kopf.TemporaryError):
        await sql_exporter.update_pods("ns", "c", NEW_IMAGE, NEW_HASH, LOGGER)

    assert patched == ["crate-data-hot-c-1"]


async def test_config_map_replaced_by_an_earlier_attempt_returns_the_original(apis):
    data = {"sql-exporter.yaml": "new", SQL_EXPORTER_CONFIG_HASH_KEY: NEW_HASH}
    original = {"sql-exporter.yaml": "old"}
    apis.core.read_namespaced_config_map.return_value = V1ConfigMap(
        data=dict(data),
        metadata=V1ObjectMeta(
            annotations={SQL_EXPORTER_PREVIOUS_CONFIG_ANNOTATION: json.dumps(original)}
        ),
    )

    assert await sql_exporter.update_config_map("ns", "c", data, LOGGER) == original
    apis.core.patch_namespaced_config_map.assert_not_called()


async def test_revert_goes_back_to_the_image_recorded_by_the_interrupted_rollout(apis):
    apis.core.read_namespaced_pod.return_value = make_pod(image="other", ready=False)
    pod = make_pod(image="interrupted", rollout_from=OLD_IMAGE)

    with pytest.raises(kopf.PermanentError):
        await sql_exporter.update_pod("ns", pod, "other", NEW_HASH, LOGGER)

    assert apis.core.patch_namespaced_pod.await_args.kwargs["body"]["spec"] == {
        "containers": [{"name": SQL_EXPORTER_CONTAINER, "image": OLD_IMAGE}]
    }


async def test_exec_that_hangs_is_given_up_on():
    async def hang(**_):
        await asyncio.sleep(60)

    core_ws = mock.Mock(connect_get_namespaced_pod_exec=hang)
    with (
        mock.patch.object(sql_exporter, "WsApiClient"),
        mock.patch.object(sql_exporter, "CoreV1Api", return_value=core_ws),
        mock.patch.object(sql_exporter, "EXEC_TIMEOUT", 0.01),
    ):
        assert await sql_exporter._exec("ns", "pod", ["true"], LOGGER) is None


async def test_pods_are_updated_one_after_the_other(apis):
    apis.core.list_namespaced_pod.return_value.items = [
        make_pod(name="crate-data-hot-c-1"),
        make_pod(name="crate-data-hot-c-0"),
    ]
    events = []

    async def patch_pod(namespace, name, body):
        if "spec" in body:
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

    previous = await sql_exporter.update_config_map("ns", "c", data, LOGGER)

    assert previous == {"sql-exporter.yaml": "old", "gone-collector.yaml": "x"}
    apis.core.patch_namespaced_config_map.assert_awaited_once_with(
        namespace="ns",
        name="crate-sql-exporter-c",
        body={
            "metadata": {
                "annotations": {
                    SQL_EXPORTER_PREVIOUS_CONFIG_ANNOTATION: json.dumps(
                        {"sql-exporter.yaml": "old", "gone-collector.yaml": "x"}
                    )
                }
            },
            "data": {"gone-collector.yaml": None, **data},
        },
    )


async def test_config_map_up_to_date_is_left_alone(apis):
    data = {"sql-exporter.yaml": "new", SQL_EXPORTER_CONFIG_HASH_KEY: NEW_HASH}
    apis.core.read_namespaced_config_map.return_value = V1ConfigMap(data=dict(data))

    assert await sql_exporter.update_config_map("ns", "c", data, LOGGER) == data

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
