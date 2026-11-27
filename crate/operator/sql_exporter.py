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
import logging
from typing import Any, Dict, List, Optional

import kopf
from aiohttp.client_exceptions import WSServerHandshakeError
from kubernetes_asyncio.client import (
    AppsV1Api,
    CoreV1Api,
    V1ContainerStatus,
    V1Pod,
    V1StatefulSet,
)
from kubernetes_asyncio.client.exceptions import ApiException
from kubernetes_asyncio.stream import WsApiClient

from crate.operator.config import config
from crate.operator.constants import (
    LABEL_COMPONENT,
    LABEL_NAME,
    SQL_EXPORTER_CONFIG_HASH_ANNOTATION,
    SQL_EXPORTER_CONFIG_HASH_KEY,
    SQL_EXPORTER_CONTAINER,
    SQL_EXPORTER_ROLLOUT_ANNOTATION,
)
from crate.operator.create import get_sql_exporter_config
from crate.operator.utils.k8s_api_client import GlobalApiClient

CONFIG_MOUNT_PATH = "/config"
RETRY_DELAY = 60
#: The kubelet refreshes ConfigMap volumes on its sync period, which takes up
#: to about two minutes.
CONFIG_PROJECTION_TIMEOUT = 180
RESTART_TIMEOUT = 300
POLL_INTERVAL = 5
#: Waiting reasons of a container that will not start without outside help.
IMAGE_PULL_FAILURES = {"ImagePullBackOff", "InvalidImageName", "ErrImageNeverPull"}

_update_slots: Optional[asyncio.Semaphore] = None


class _PodReplaced(Exception):
    """The pod being rolled out is gone or was recreated under the same name."""


def _label_selector(name: str) -> str:
    return f"{LABEL_COMPONENT}=cratedb,{LABEL_NAME}={name}"


def _container(containers: Optional[List[Any]]) -> Optional[Any]:
    return next((c for c in containers or [] if c.name == SQL_EXPORTER_CONTAINER), None)


def _annotations(metadata: Any) -> Dict[str, str]:
    return (metadata.annotations if metadata else None) or {}


def _image_patch(image: str) -> Dict[str, Any]:
    return {"containers": [{"name": SQL_EXPORTER_CONTAINER, "image": image}]}


def _hash_patch(config_hash: Optional[str]) -> Dict[str, Any]:
    if config_hash is None:
        return {}
    return {
        "metadata": {"annotations": {SQL_EXPORTER_CONFIG_HASH_ANNOTATION: config_hash}}
    }


async def update_sql_exporter(
    namespace: str, name: str, logger: logging.Logger
) -> None:
    """
    Bring the SQL exporter of an existing cluster to the image and the
    configuration this operator ships.

    Running pods are handled one at a time, and only their ``sql-exporter``
    container is restarted. They are handled before the StatefulSets, so that
    an image that cannot be started is caught on a single pod before it
    reaches the pod template.
    """
    global _update_slots
    if _update_slots is None:
        _update_slots = asyncio.Semaphore(config.SQL_EXPORTER_UPDATE_CONCURRENCY)
    # A slot is held from the ConfigMap until the pods are checked, so that
    # clusters waiting for one still run with a matching image and config.
    async with _update_slots:
        await _update_sql_exporter(namespace, name, logger)


async def _update_sql_exporter(
    namespace: str, name: str, logger: logging.Logger
) -> None:
    image = config.SQL_EXPORTER_IMAGE
    config_map = get_sql_exporter_config(None, name, {})
    config_hash = None
    previous_data = None
    if config_map is not None:
        previous_data = await update_config_map(
            namespace, name, config_map.data, logger
        )
        if previous_data is None:
            logger.info("No SQL exporter ConfigMap found, skipping.")
            return
        config_hash = config_map.data[SQL_EXPORTER_CONFIG_HASH_KEY]

    try:
        await update_pods(namespace, name, image, config_hash, logger)
    except kopf.PermanentError:
        if config_map is not None and previous_data != config_map.data:
            await _restore_config_map(
                namespace, name, previous_data or {}, config_hash, logger
            )
        raise
    await update_statefulsets(namespace, name, image, config_hash, logger)
    # Pods recreated during the first pass came from the old pod template.
    # `OnDelete` leaves a pod that is not running yet on its old exporter, so
    # come back once it is.
    if await update_pods(namespace, name, image, config_hash, logger):
        raise kopf.TemporaryError(
            "Pods that are not running yet still have to be checked for the "
            "SQL exporter update.",
            delay=RETRY_DELAY,
        )


async def _restore_config_map(
    namespace: str,
    name: str,
    previous_data: Dict[str, str],
    config_hash: Optional[str],
    logger: logging.Logger,
) -> None:
    """
    Put back the ConfigMap that was replaced for a rollout that was reverted.

    A pod that already runs the new configuration needs it on its next start,
    so nothing is restored once one exists.
    """
    async with GlobalApiClient() as api_client:
        core = CoreV1Api(api_client)
        pods = await core.list_namespaced_pod(
            namespace=namespace, label_selector=_label_selector(name)
        )
    if any(
        _annotations(pod.metadata).get(SQL_EXPORTER_CONFIG_HASH_ANNOTATION)
        == config_hash
        for pod in pods.items
    ):
        logger.warning("Keeping the new SQL exporter ConfigMap, a pod already uses it.")
        return
    logger.warning("Restoring the previous SQL exporter ConfigMap.")
    await update_config_map(namespace, name, previous_data, logger)


async def update_config_map(
    namespace: str, name: str, data: Dict[str, str], logger: logging.Logger
) -> Optional[Dict[str, str]]:
    """
    :returns: The data the ConfigMap had before, or ``None`` if the cluster has
        no SQL exporter ConfigMap.
    """
    config_map_name = f"crate-sql-exporter-{name}"
    async with GlobalApiClient() as api_client:
        core = CoreV1Api(api_client)
        try:
            existing = await core.read_namespaced_config_map(
                namespace=namespace, name=config_map_name
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise

        existing_data = existing.data or {}
        if existing_data == data:
            return existing_data

        body: Dict[str, Any] = {
            "data": {**{key: None for key in existing_data if key not in data}, **data}
        }
        logger.info("Updating SQL exporter ConfigMap %s", config_map_name)
        await core.patch_namespaced_config_map(
            namespace=namespace, name=config_map_name, body=body
        )
    return existing_data


async def update_statefulsets(
    namespace: str,
    name: str,
    image: str,
    config_hash: Optional[str],
    logger: logging.Logger,
) -> None:
    async with GlobalApiClient() as api_client:
        apps = AppsV1Api(api_client)
        statefulsets = await apps.list_namespaced_stateful_set(
            namespace=namespace, label_selector=_label_selector(name)
        )
        statefulset: V1StatefulSet
        for statefulset in statefulsets.items:
            template = statefulset.spec.template
            container = _container(template.spec.containers)
            if container is None:
                continue

            body: Dict[str, Any] = {}
            if container.image != image:
                body["spec"] = _image_patch(image)
            if config_hash is not None and (
                _annotations(template.metadata).get(SQL_EXPORTER_CONFIG_HASH_ANNOTATION)
                != config_hash
            ):
                body.update(_hash_patch(config_hash))
            if not body:
                continue

            logger.info(
                "Updating SQL exporter in StatefulSet %s", statefulset.metadata.name
            )
            await apps.patch_namespaced_stateful_set(
                namespace=namespace,
                name=statefulset.metadata.name,
                body={"spec": {"template": body}},
            )


async def update_pods(
    namespace: str,
    name: str,
    image: str,
    config_hash: Optional[str],
    logger: logging.Logger,
) -> bool:
    """
    :returns: ``True`` if a pod that needs the update is not running yet.
    """
    async with GlobalApiClient() as api_client:
        core = CoreV1Api(api_client)
        pods = await core.list_namespaced_pod(
            namespace=namespace, label_selector=_label_selector(name)
        )

    waiting = False
    for pod_name in sorted(pod.metadata.name for pod in pods.items):
        # Earlier pods can take minutes, and the restart count has to be current.
        pod = await _read_pod(namespace, pod_name)
        if pod is not None:
            waiting |= await update_pod(namespace, pod, image, config_hash, logger)
    return waiting


async def update_pod(
    namespace: str,
    pod: V1Pod,
    image: str,
    config_hash: Optional[str],
    logger: logging.Logger,
) -> bool:
    """
    :returns: ``True`` if the pod needs the update but is not running yet.
    """
    pod_name = pod.metadata.name
    container = _container(pod.spec.containers)
    if container is None or pod.metadata.deletion_timestamp is not None:
        return False

    unverified_from = _annotations(pod.metadata).get(SQL_EXPORTER_ROLLOUT_ANNOTATION)
    stale_image = container.image != image
    stale_config = config_hash is not None and (
        _annotations(pod.metadata).get(SQL_EXPORTER_CONFIG_HASH_ANNOTATION)
        != config_hash
    )
    if unverified_from is None and not stale_image and not stale_config:
        return False

    status = _container_status(pod)
    if status is None or pod.status.phase != "Running":
        return True

    try:
        if unverified_from is not None and not stale_image:
            # An earlier run patched the image and stopped before it saw the
            # exporter start.
            if not await _wait_for_restart(namespace, pod_name, pod.metadata.uid, None):
                await _revert_image(namespace, pod_name, image, unverified_from, logger)
            await _patch_pod(namespace, pod_name, _rollout_done_patch(None))
            return False

        old_image = container.image
        projected = True
        if config_hash is not None and stale_config:
            projected = await _wait_for_projected_config(
                namespace, pod_name, config_hash, logger
            )
        config_pending = kopf.TemporaryError(
            f"Pod {pod_name} has not received the new SQL exporter configuration yet.",
            delay=RETRY_DELAY,
        )
        # A new image is rolled out regardless: the check above needs a working
        # exporter container, which the new image may be what provides.
        if not projected and not stale_image:
            raise config_pending

        if stale_config and projected:
            # The wait above can take minutes, so the baseline is stale by now.
            refreshed = await _read_pod(namespace, pod_name)
            if refreshed is None or refreshed.metadata.uid != pod.metadata.uid:
                raise _PodReplaced()
            pod = refreshed
            status = _container_status(pod)
            if status is None:
                return False

        logger.info("Restarting SQL exporter in pod %s", pod_name)
        if stale_image:
            await _patch_pod(
                namespace,
                pod_name,
                {
                    "metadata": {
                        "annotations": {SQL_EXPORTER_ROLLOUT_ANNOTATION: old_image}
                    },
                    "spec": _image_patch(image),
                },
            )
        else:
            await _exec(namespace, pod_name, ["/bin/sh", "-c", "kill 1"], logger)

        restarted = await _wait_for_restart(
            namespace, pod_name, pod.metadata.uid, status.restart_count
        )

        if not restarted and stale_image:
            await _revert_image(namespace, pod_name, image, old_image, logger)
        if not restarted:
            raise kopf.TemporaryError(
                f"SQL exporter in pod {pod_name} did not restart in time.",
                delay=RETRY_DELAY,
            )

        if stale_image:
            await _patch_pod(
                namespace,
                pod_name,
                _rollout_done_patch(config_hash if projected else None),
            )
            if not projected:
                raise config_pending
        else:
            await _patch_pod(namespace, pod_name, _hash_patch(config_hash))
    except (_PodReplaced, ApiException) as e:
        if isinstance(e, ApiException) and e.status != 404:
            raise
        # A pod that is being recreated starts from the StatefulSet's template.
        logger.info("Pod %s is gone, skipping.", pod_name)
    return False


async def _revert_image(
    namespace: str,
    pod_name: str,
    image: str,
    previous: str,
    logger: logging.Logger,
) -> None:
    logger.warning(
        "SQL exporter in pod %s did not start with image %s, reverting to %s",
        pod_name,
        image,
        previous,
    )
    await _patch_pod(
        namespace,
        pod_name,
        {
            "metadata": {"annotations": {SQL_EXPORTER_ROLLOUT_ANNOTATION: None}},
            "spec": _image_patch(previous),
        },
    )
    # Retrying would take the pod out of the Service again and again.
    raise kopf.PermanentError(
        f"SQL exporter in pod {pod_name} did not start with image {image}."
    )


def _rollout_done_patch(config_hash: Optional[str]) -> Dict[str, Any]:
    annotations: Dict[str, Optional[str]] = {SQL_EXPORTER_ROLLOUT_ANNOTATION: None}
    if config_hash is not None:
        annotations[SQL_EXPORTER_CONFIG_HASH_ANNOTATION] = config_hash
    return {"metadata": {"annotations": annotations}}


async def _read_pod(namespace: str, pod_name: str) -> Optional[V1Pod]:
    try:
        async with GlobalApiClient() as api_client:
            core = CoreV1Api(api_client)
            return await core.read_namespaced_pod(namespace=namespace, name=pod_name)
    except ApiException as e:
        if e.status == 404:
            return None
        raise


def _container_status(pod: V1Pod) -> Optional[V1ContainerStatus]:
    statuses = pod.status.container_statuses if pod.status else None
    return _container(statuses)


async def _patch_pod(namespace: str, pod_name: str, body: Dict[str, Any]) -> None:
    async with GlobalApiClient() as api_client:
        core = CoreV1Api(api_client)
        await core.patch_namespaced_pod(namespace=namespace, name=pod_name, body=body)


async def _exec(
    namespace: str, pod_name: str, command: List[str], logger: logging.Logger
) -> Optional[str]:
    async with WsApiClient() as ws_api_client:
        core_ws = CoreV1Api(ws_api_client)
        try:
            return await core_ws.connect_get_namespaced_pod_exec(
                namespace=namespace,
                name=pod_name,
                command=command,
                container=SQL_EXPORTER_CONTAINER,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
        except (ApiException, WSServerHandshakeError) as e:
            logger.warning(
                "Exec into SQL exporter of pod %s failed. Status: %s",
                pod_name,
                e.status,
            )
            return None


async def _wait_for_projected_config(
    namespace: str, pod_name: str, config_hash: str, logger: logging.Logger
) -> bool:
    command = ["cat", f"{CONFIG_MOUNT_PATH}/{SQL_EXPORTER_CONFIG_HASH_KEY}"]
    for _ in range(CONFIG_PROJECTION_TIMEOUT // POLL_INTERVAL):
        result = await _exec(namespace, pod_name, command, logger)
        if result is not None and result.strip() == config_hash:
            return True
        await asyncio.sleep(POLL_INTERVAL)
    return False


async def _wait_for_restart(
    namespace: str, pod_name: str, uid: Optional[str], restart_count: Optional[int]
) -> bool:
    """
    Wait for the exporter to be ready after a restart past ``restart_count``,
    or just to be ready if that is ``None``.

    :raises _PodReplaced: if the pod is gone or has another UID than ``uid``.
    """
    for _ in range(RESTART_TIMEOUT // POLL_INTERVAL):
        await asyncio.sleep(POLL_INTERVAL)
        try:
            pod = await _read_pod(namespace, pod_name)
        except ApiException:
            # Giving up here would skip reverting an image that does not start.
            continue
        if pod is None or pod.metadata.uid != uid:
            raise _PodReplaced()
        status = _container_status(pod)
        waiting = status.state.waiting if status and status.state else None
        if waiting is not None and waiting.reason in IMAGE_PULL_FAILURES:
            return False
        if (
            status
            and status.ready
            and (restart_count is None or status.restart_count > restart_count)
        ):
            return True
    return False
