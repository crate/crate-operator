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
    image = config.SQL_EXPORTER_IMAGE
    config_map = get_sql_exporter_config(None, name, {})
    config_hash = None
    if config_map is not None:
        if not await update_config_map(namespace, name, config_map.data, logger):
            logger.info("No SQL exporter ConfigMap found, skipping.")
            return
        config_hash = config_map.data[SQL_EXPORTER_CONFIG_HASH_KEY]

    await update_pods(namespace, name, image, config_hash, logger)
    await update_statefulsets(namespace, name, image, config_hash, logger)
    # Pods recreated during the first pass came from the old pod template.
    await update_pods(namespace, name, image, config_hash, logger)


async def update_config_map(
    namespace: str, name: str, data: Dict[str, str], logger: logging.Logger
) -> bool:
    """
    :returns: ``False`` if the cluster has no SQL exporter ConfigMap.
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
                return False
            raise

        existing_data = existing.data or {}
        if existing_data == data:
            return True

        body: Dict[str, Any] = {
            "data": {**{key: None for key in existing_data if key not in data}, **data}
        }
        logger.info("Updating SQL exporter ConfigMap %s", config_map_name)
        await core.patch_namespaced_config_map(
            namespace=namespace, name=config_map_name, body=body
        )
    return True


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
) -> None:
    async with GlobalApiClient() as api_client:
        core = CoreV1Api(api_client)
        pods = await core.list_namespaced_pod(
            namespace=namespace, label_selector=_label_selector(name)
        )

    for pod_name in sorted(pod.metadata.name for pod in pods.items):
        # Earlier pods can take minutes, and the restart count has to be current.
        try:
            async with GlobalApiClient() as api_client:
                core = CoreV1Api(api_client)
                pod = await core.read_namespaced_pod(namespace=namespace, name=pod_name)
        except ApiException as e:
            if e.status == 404:
                continue
            raise
        await update_pod(namespace, pod, image, config_hash, logger)


async def update_pod(
    namespace: str,
    pod: V1Pod,
    image: str,
    config_hash: Optional[str],
    logger: logging.Logger,
) -> None:
    pod_name = pod.metadata.name
    container = _container(pod.spec.containers)
    status = _container_status(pod)
    if (
        container is None
        or status is None
        or pod.metadata.deletion_timestamp is not None
        or pod.status.phase != "Running"
    ):
        return

    old_image = container.image
    stale_image = old_image != image
    stale_config = config_hash is not None and (
        _annotations(pod.metadata).get(SQL_EXPORTER_CONFIG_HASH_ANNOTATION)
        != config_hash
    )
    if not stale_image and not stale_config:
        return

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

    logger.info("Restarting SQL exporter in pod %s", pod_name)
    try:
        if stale_image:
            await _patch_pod(
                namespace,
                pod_name,
                {
                    **(_hash_patch(config_hash) if projected else {}),
                    "spec": _image_patch(image),
                },
            )
        else:
            await _exec(namespace, pod_name, ["/bin/sh", "-c", "kill 1"], logger)

        restarted = await _wait_for_restart(namespace, pod_name, status.restart_count)

        if not restarted and stale_image:
            logger.warning(
                "SQL exporter in pod %s did not start with image %s, reverting to %s",
                pod_name,
                image,
                old_image,
            )
            await _patch_pod(namespace, pod_name, {"spec": _image_patch(old_image)})
            # Retrying would take the pod out of the Service again and again.
            raise kopf.PermanentError(
                f"SQL exporter in pod {pod_name} did not start with image {image}."
            )
        if not restarted:
            raise kopf.TemporaryError(
                f"SQL exporter in pod {pod_name} did not restart in time.",
                delay=RETRY_DELAY,
            )

        if not stale_image:
            await _patch_pod(namespace, pod_name, _hash_patch(config_hash))
        elif not projected:
            raise config_pending
    except ApiException as e:
        if e.status != 404:
            raise
        # A pod that is being recreated starts from the StatefulSet's template.
        logger.info("Pod %s is gone, skipping.", pod_name)


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


async def _wait_for_restart(namespace: str, pod_name: str, restart_count: int) -> bool:
    for _ in range(RESTART_TIMEOUT // POLL_INTERVAL):
        await asyncio.sleep(POLL_INTERVAL)
        try:
            async with GlobalApiClient() as api_client:
                core = CoreV1Api(api_client)
                pod = await core.read_namespaced_pod(namespace=namespace, name=pod_name)
        except ApiException as e:
            if e.status == 404:
                raise
            # Giving up here would skip reverting an image that does not start.
            continue
        status = _container_status(pod)
        if status and status.ready and status.restart_count > restart_count:
            return True
    return False
