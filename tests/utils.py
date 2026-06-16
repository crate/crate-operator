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

import asyncio
import json
import logging
import os
import random
import time
from functools import reduce
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple
from unittest import mock

import aiohttp
import psycopg2
from aiopg import Connection
from kubernetes_asyncio.client import (
    BatchV1Api,
    CoreV1Api,
    CustomObjectsApi,
    V1DeleteOptions,
    V1Namespace,
)
from kubernetes_asyncio.client.exceptions import ApiException

from crate.operator.backup import create_backups
from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    BACKUP_METRICS_DEPLOYMENT_NAME,
    DATA_NODE_NAME,
    GRAND_CENTRAL_RESOURCE_PREFIX,
    KOPF_STATE_STORE_PREFIX,
    LABEL_COMPONENT,
    LABEL_MANAGED_BY,
    LABEL_NAME,
    LABEL_PART_OF,
    RESOURCE_CRATEDB,
)
from crate.operator.cratedb import (
    connection_factory,
    get_cluster_settings,
    get_healthiness,
    get_number_of_nodes,
)
from crate.operator.operations import get_pods_in_deployment
from crate.operator.utils.kubeapi import (
    get_service_public_hostname,
    get_system_user_password,
)

logger = logging.getLogger(__name__)

CRATE_VERSION = "5.6.5"
CRATE_VERSION_WITH_JWT = "5.7.3"
CRATE_VERSION_WITH_GLOBAL_JWT_CONFIG = "5.10.1"
DEFAULT_TIMEOUT = 60
# Cluster create/bootstrap on a busy parallel CI cluster can take several
# minutes (image pulls, JVM start, node contention), so the bring-up waits get
# generous headroom -- the operator already retries the bootstrap exec.
CLUSTER_CREATE_TIMEOUT = DEFAULT_TIMEOUT * 10


# Control-plane errors that are transient under load -- AKS/EKS apiserver
# Priority & Fairness rate limiting (429) and brief apiserver unavailability
# (5xx). These should never fail a polling wait outright; the loop just retries.
_TRANSIENT_API_STATUSES = frozenset({429, 500, 502, 503, 504})


def _is_transient(exc: BaseException) -> bool:
    """Whether ``exc`` is a transient control-plane error worth retrying.

    Single source of truth for the "is this transient?" policy shared by the
    polling path (``_poll``) and the one-shot retry path (``retry_on_throttle``).
    """
    if isinstance(exc, ApiException):
        return exc.status in _TRANSIENT_API_STATUSES
    return isinstance(exc, (aiohttp.ClientError, asyncio.TimeoutError))


class _Throttled:
    """Sentinel returned by a poll that hit a transient control-plane error.

    It never compares equal to a caller's expected ``condition``, so the wait
    loop simply polls again until the condition is met or the timeout elapses --
    a one-off 429 from rate limiting no longer reds the test.
    """


_THROTTLED = _Throttled()


async def _poll(coro_func, *args, **kwargs):
    try:
        return await coro_func(*args, **kwargs)
    except Exception as e:
        if not _is_transient(e):
            raise
        logging.getLogger(__name__).debug(
            "Transient error during wait (%s); retrying.", e
        )
        return _THROTTLED


async def assert_wait_for(
    condition, coro_func, *args, err_msg="", timeout=DEFAULT_TIMEOUT, delay=2, **kwargs
):
    # Poll through _poll() so a transient control-plane error (429/5xx/connection
    # drop) is swallowed and retried within the existing timeout window instead
    # of failing the whole wait on a single throttled request.
    ret_val = await _poll(coro_func, *args, **kwargs)
    duration = 0.0
    while ret_val is not condition:
        await asyncio.sleep(delay)
        ret_val = await _poll(coro_func, *args, **kwargs)
        if ret_val is not condition and duration > timeout:
            break
        else:
            duration += delay
    assert ret_val is condition, err_msg


async def does_namespace_exist(core, namespace: str) -> bool:
    # A single GET (404 == gone) rather than listing every namespace in the
    # cluster on each poll -- the list call scales with parallel test namespaces
    # and is exactly the kind of request the apiserver throttles under load.
    try:
        await core.read_namespace(name=namespace)
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        raise


def _retry_after_seconds(exc: BaseException) -> Optional[float]:
    """Parse the ``Retry-After`` header AKS/EKS attach to a 429, if present."""
    headers = getattr(exc, "headers", None)
    if not headers:
        return None
    try:
        return float(headers.get("Retry-After"))
    except (TypeError, ValueError):
        return None


async def retry_on_throttle(coro_func, *args, deadline: float = 90.0, **kwargs):
    """Call an async k8s API method, retrying transient control-plane errors
    until ``deadline`` seconds have elapsed.

    Retries on 429 (AKS/EKS Priority & Fairness rejects request *bursts* this way
    -- always safe to retry, the request is rejected before it executes), on 5xx,
    and on connection drops. Honours the server's ``Retry-After`` header when
    present (APF typically sets it to ~1s); otherwise backs off exponentially
    with **full jitter** so the parallel workers don't resynchronise and re-
    collide on the shared limiter. Use for one-shot calls (create/delete);
    polling waits are already covered by ``assert_wait_for``.
    """
    start = time.monotonic()
    backoff = 1.0
    while True:
        try:
            return await coro_func(*args, **kwargs)
        except Exception as e:
            if not _is_transient(e):
                raise
            last_exc = e
        if time.monotonic() - start >= deadline:
            # Throttling outlasted our budget -- surface the real error.
            raise last_exc
        retry_after = _retry_after_seconds(last_exc)
        if retry_after is not None:
            sleep = retry_after
        else:
            # Full jitter: sleep uniformly in [0, backoff] so concurrent callers
            # spread out instead of retrying in lockstep.
            sleep = random.uniform(0, backoff)
            backoff = min(backoff * 2, 10.0)
        await asyncio.sleep(sleep)


async def no_cratedbs_remain(coapi: CustomObjectsApi, namespace: str) -> bool:
    """Return ``True`` once no CrateDB CRs are left in ``namespace``.

    A lingering CR means kopf is still holding its finalizer (and its per-cluster
    ping timer); used to gate namespace teardown on the operator having fully
    processed the deletion. Only ever driven by ``assert_wait_for``, so transient
    control-plane errors are absorbed by ``_poll`` -- this just maps a 404
    (namespace/CRD already gone) to "nothing left".
    """
    try:
        objs = await coapi.list_namespaced_custom_object(
            group=API_GROUP,
            version="v1",
            plural=RESOURCE_CRATEDB,
            namespace=namespace,
        )
    except ApiException as e:
        if e.status == 404:
            return True
        raise
    return len(objs.get("items", [])) == 0


async def delete_cratedbs_and_wait(
    coapi: CustomObjectsApi, namespace: str, timeout: float = 120
) -> None:
    """Delete every CrateDB CR in ``namespace`` *through kopf* and wait for it
    to disappear.

    Deleting the CR (rather than only the namespace) lets the session-scoped,
    all-namespace operator run its ``@kopf.on.delete`` handler, clear the
    finalizer and **cancel the per-cluster ping timer**. Skipping this leaks a
    5s timer that keeps hitting the API server for a phantom cluster for the rest
    of the run, throttling the shared client and flaking other tests.

    Best-effort: any API error here is logged, never raised -- the namespace
    delete that follows still reclaims the CR, so teardown must not red a
    passing test.
    """
    log = logging.getLogger(__name__)
    try:
        objs = await retry_on_throttle(
            coapi.list_namespaced_custom_object,
            group=API_GROUP,
            version="v1",
            plural=RESOURCE_CRATEDB,
            namespace=namespace,
        )
        for obj in objs.get("items", []):
            try:
                await retry_on_throttle(
                    coapi.delete_namespaced_custom_object,
                    group=API_GROUP,
                    version="v1",
                    plural=RESOURCE_CRATEDB,
                    namespace=namespace,
                    name=obj["metadata"]["name"],
                    body=V1DeleteOptions(),
                )
            except ApiException as e:
                if e.status != 404:
                    raise
        await assert_wait_for(
            True,
            no_cratedbs_remain,
            coapi,
            namespace,
            timeout=timeout,
        )
    except ApiException as e:
        if e.status != 404:
            log.warning(
                "Best-effort CrateDB teardown for namespace %s hit a %s; the "
                "namespace delete will still reclaim it.",
                namespace,
                e.status,
            )
    except AssertionError:
        log.warning(
            "CrateDB CR(s) in namespace %s were not finalized within %ss; the "
            "ping timer may briefly leak until the namespace finishes deleting.",
            namespace,
            timeout,
        )


async def do_pods_exist(core: CoreV1Api, namespace: str, expected: Set[str]) -> bool:
    pods = await core.list_namespaced_pod(namespace=namespace)
    return expected.issubset({p.metadata.name for p in pods.items})


async def do_pod_ids_exist(core: CoreV1Api, namespace: str, pod_ids: Set[str]) -> bool:
    pods = await core.list_namespaced_pod(namespace=namespace)
    return bool(pod_ids.intersection({p.metadata.uid for p in pods.items}))


async def start_cluster(
    name: str,
    namespace: V1Namespace,
    core: CoreV1Api,
    coapi: CustomObjectsApi,
    hot_nodes: int = 0,
    crate_version: str = CRATE_VERSION,
    wait_for_healthy: bool = True,
    wait_for_lb: bool = True,
    additional_cluster_spec: Optional[Mapping[str, Any]] = None,
    users: Optional[List[Mapping[str, Any]]] = None,
    resource_requests: Optional[Mapping[str, Any]] = None,
    backups_spec: Optional[Mapping[str, Any]] = None,
    grand_central_spec: Optional[Mapping[str, Any]] = None,
    master_nodes: int = 0,
    master_resources: Optional[Mapping[str, Any]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    additional_cluster_spec = additional_cluster_spec or {}
    body: dict = {
        "apiVersion": "cloud.crate.io/v1",
        "kind": "CrateDB",
        "metadata": {
            "name": name,
            "annotations": {
                "testing": f"{os.getpid()}",
                "test-name": os.environ.get("PYTEST_CURRENT_TEST", "")
                .split(":")[-1]
                .split(" ")[0],
            },
        },
        "spec": {
            "cluster": {
                "imageRegistry": "crate",
                "name": "my-crate-cluster",
                "version": crate_version,
                **additional_cluster_spec,  # type: ignore
            },
            "nodes": {
                "data": [
                    {
                        "name": DATA_NODE_NAME,
                        "replicas": hot_nodes,
                        "resources": {
                            "limits": {
                                "cpu": 2,
                                "memory": "4Gi",
                            },
                            "heapRatio": 0.25,
                            "disk": {
                                "storageClass": config.DEBUG_VOLUME_STORAGE_CLASS,
                                "size": "16GiB",
                                "count": 1,
                            },
                        },
                    },
                ]
            },
        },
    }

    if resource_requests:
        if "limits" in resource_requests and "requests" in resource_requests:
            body["spec"]["nodes"]["data"][0]["resources"]["limits"] = {
                "cpu": resource_requests.get("limits", {}).get("cpu", "2"),
                "memory": resource_requests.get("limits", {}).get("memory", "4Gi"),
            }
            body["spec"]["nodes"]["data"][0]["resources"]["requests"] = {
                "cpu": resource_requests.get("requests", {}).get("cpu", "2"),
                "memory": resource_requests.get("requests", {}).get("memory", "4Gi"),
            }
        else:
            body["spec"]["nodes"]["data"][0]["resources"]["requests"] = {
                "cpu": resource_requests.get("cpu", "2"),
                "memory": resource_requests.get("memory", "4Gi"),
            }

    if backups_spec:
        body["spec"]["backups"] = backups_spec

    if users:
        body["spec"]["users"] = users

    if grand_central_spec:
        body["spec"]["grandCentral"] = grand_central_spec

    if master_nodes:
        # Dedicated master nodes are a single object (no ``name``), unlike the
        # data list. Defaults are deliberately small so a master cluster fits a
        # local minikube; callers may override via ``master_resources``.
        body["spec"]["nodes"]["master"] = master_resources or {
            "replicas": master_nodes,
            "resources": {
                # cpu must be a number; small requests so several masters fit a
                # local minikube while limits allow a usable heap.
                "limits": {"cpu": 1, "memory": "2Gi"},
                "requests": {"cpu": 0.5, "memory": "512Mi"},
                "heapRatio": 0.25,
                "disk": {
                    "storageClass": config.DEBUG_VOLUME_STORAGE_CLASS,
                    "size": "16GiB",
                    "count": 1,
                },
            },
        }

    await coapi.create_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace.metadata.name,
        body=body,
    )

    if wait_for_healthy and not wait_for_lb:
        raise ValueError("wait_for_healthy=True requires wait_for_lb=True")

    if wait_for_lb:
        await assert_wait_for(
            True,
            is_lb_service_ready,
            core,
            namespace.metadata.name,
            f"crate-{name}",
            err_msg="Lb service was not ready.",
            timeout=CLUSTER_CREATE_TIMEOUT,
        )

        host = await asyncio.wait_for(
            get_service_public_hostname(core, namespace.metadata.name, name),
            timeout=CLUSTER_CREATE_TIMEOUT,
        )
        password = await get_system_user_password(core, namespace.metadata.name, name)
    else:
        host, password = None, None

    if wait_for_healthy:
        # The timeouts are pretty high here since in Azure it's sometimes
        # non-deterministic how long provisioning a pod will actually take.
        assert host is not None and password is not None
        await assert_wait_for(
            True,
            is_kopf_handler_finished,
            coapi,
            name,
            namespace.metadata.name,
            f"{KOPF_STATE_STORE_PREFIX}/cluster_create",
            err_msg="Cluster has not finished bootstrapping",
            timeout=CLUSTER_CREATE_TIMEOUT,
        )

        await assert_wait_for(
            True,
            is_cluster_healthy,
            connection_factory(host, password),
            # Dedicated masters are full cluster members, so they count towards
            # the expected node total alongside the data nodes.
            hot_nodes + master_nodes,
            err_msg="Cluster wasn't healthy after 10 minutes.",
            timeout=CLUSTER_CREATE_TIMEOUT,
        )

    return host, password


async def start_backup_metrics(
    name: str,
    namespace: V1Namespace,
    faker,
):
    backups_spec = {
        "aws": {
            "accessKeyId": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
            "basePath": faker.uri_path() + "/",
            "cron": "1 2 3 4 5",
            "region": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
            "bucket": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
            "secretAccessKey": {
                "secretKeyRef": {
                    "key": faker.domain_word(),
                    "name": faker.domain_word(),
                },
            },
        },
    }

    await create_backups(
        None,
        namespace.metadata.name,
        name,
        {
            LABEL_COMPONENT: "backup",
            LABEL_MANAGED_BY: "crate-operator",
            LABEL_NAME: name,
            LABEL_PART_OF: "cratedb",
        },
        32581,
        23851,
        backups_spec,
        None,
        True,
        logger,
    )


async def is_cluster_healthy(
    conn_factory: Callable[[], Connection], expected_num_nodes: int
):
    try:
        async with conn_factory() as conn:
            async with conn.cursor() as cursor:
                num_nodes = await get_number_of_nodes(cursor)
                healthines = await get_healthiness(cursor)
                return expected_num_nodes == num_nodes and healthines in {1, None}
    except (psycopg2.DatabaseError, asyncio.exceptions.TimeoutError):
        return False


async def is_kopf_handler_finished(
    coapi: CustomObjectsApi, name, namespace: str, handler_name: str
):
    cratedb = await coapi.get_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace,
        name=name,
    )

    handler_status = cratedb["metadata"].get("annotations", {}).get(handler_name, None)
    return handler_status is None


async def _check_kopf_handler_status(
    coapi: CustomObjectsApi, name, namespace: str, handler_name: str
):
    """
    Returns True if handler succeeded, False if still running, None if not yet seen.
    Raises AssertionError if handler failed permanently.
    """
    cratedb = await coapi.get_namespaced_custom_object(
        group=API_GROUP,
        version="v1",
        plural=RESOURCE_CRATEDB,
        namespace=namespace,
        name=name,
    )
    annotations = cratedb["metadata"].get("annotations", {})
    raw = annotations.get(handler_name)

    if raw is None:
        return None

    try:
        status = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning(
            "Handler '%s' annotation is not valid JSON: %s", handler_name, raw
        )
        return None

    if status.get("failure"):
        raise AssertionError(
            f"Handler '{handler_name}' failed: {status.get('message')}"
        )

    result = status.get("success", False) is True
    return result


def _summarize_container_state(cs) -> str:
    """
    Render a container status compactly, surfacing the actionable waiting/
    terminated *reason* (``ImagePullBackOff``, ``CrashLoopBackOff``,
    ``ErrImagePull``, ``CreateContainerError`` …) rather than a raw blob.
    """
    state = cs.state
    if state is not None and state.waiting is not None:
        detail = f"waiting/{state.waiting.reason}"
        if state.waiting.message:
            detail += f": {state.waiting.message}"
    elif state is not None and state.terminated is not None:
        t = state.terminated
        detail = f"terminated/{t.reason} exit={t.exit_code}"
    elif state is not None and state.running is not None:
        detail = "running"
    else:
        detail = "unknown"
    return f"{cs.name}:{detail} ready={cs.ready} restarts={cs.restart_count}"


async def describe_pods(core: CoreV1Api, namespace: str) -> str:
    """
    A compact, per-pod summary of container states for a namespace, used to
    explain *why* a handler is stuck when a wait times out (e.g. a pod sitting
    in ``ImagePullBackOff`` or ``CrashLoopBackOff``). Falls back to pod
    conditions when no container has been created yet (the ``Pending`` case).
    """
    try:
        pods = await core.list_namespaced_pod(namespace=namespace)
    except Exception as e:  # diagnostics must never mask the real failure
        return f"<could not list pods in {namespace}: {e}>"
    lines = []
    for p in pods.items:
        st = p.status
        states = [
            _summarize_container_state(cs)
            for cs in (st.init_container_statuses or []) + (st.container_statuses or [])
        ]
        if not states:
            states = [
                f"{c.type}={c.status}"
                + (f"({c.reason})" if c.status != "True" and c.reason else "")
                for c in (st.conditions or [])
            ] or ["<no container status>"]
        lines.append(f"  {p.metadata.name} phase={st.phase}: {', '.join(states)}")
    return "\n".join(lines) if lines else "  <no pods>"


async def wait_for_kopf_handler(
    coapi: CustomObjectsApi,
    name: str,
    namespace: str,
    handler_name: str,
    timeout: float = DEFAULT_TIMEOUT,
    delay: float = 2,
):
    """
    Wait for a kopf handler to complete, handling kopf's annotation cleanup lifecycle.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    seen = False

    while asyncio.get_running_loop().time() < deadline:
        result = await _check_kopf_handler_status(coapi, name, namespace, handler_name)
        if result is True:
            logger.info("Handler '%s' finished (success=True)", handler_name)
            return
        if result is None and seen:
            logger.info("Handler '%s' finished (annotation cleaned up)", handler_name)
            return
        if result is not None:
            seen = True
        await asyncio.sleep(delay)

    # The wait gives the cluster the full timeout to come up; if it still has
    # not, include the pods' container states so the failure points at the
    # cause (image pull, crash loop, unschedulable) instead of a bare timeout.
    pod_states = await describe_pods(CoreV1Api(coapi.api_client), namespace)
    raise AssertionError(
        f"Handler '{handler_name}' did not finish within {timeout}s.\n"
        f"Pod states:\n{pod_states}"
    )


async def create_test_sys_jobs_table(conn_factory):
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            table_name = config.JOBS_TABLE
            logger.info(f"Creating {table_name}")
            await cursor.execute(
                f"CREATE TABLE {table_name} (id INTEGER, stmt VARCHAR) "
                "WITH (number_of_replicas='0-all')"
            )


async def insert_test_snapshot_job(conn_factory):
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            table_name = config.JOBS_TABLE
            logger.info(f"Creating {table_name}")
            await cursor.execute(
                f"INSERT INTO {table_name} (id, stmt) VALUES (1, 'CREATE SNAPSHOT ...')"
            )
            await cursor.execute(f"REFRESH TABLE {table_name}")


async def clear_test_snapshot_jobs(conn_factory):
    async with conn_factory() as conn:
        async with conn.cursor() as cursor:
            table_name = config.JOBS_TABLE
            logger.info(f"Creating {table_name}")
            await cursor.execute(f"DELETE FROM {table_name}")
            await cursor.execute(f"REFRESH TABLE {table_name}")


async def create_fake_snapshot_job(api_client, name, namespace):
    """
    As the name implies, this creates a k8s job that looks like a snapshot job.
    It pulls busybox and sleeps for 60s, long enough for scaling to block.
    """
    batch = BatchV1Api(api_client)
    body = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"cluster-backup-{name}",
            "labels": {
                "app.kubernetes.io/component": "backup",
                "app.kubernetes.io/managed-by": "crate-operator",
                "app.kubernetes.io/name": name,
                "app.kubernetes.io/part-of": "cratedb",
            },
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "busybox",
                            "image": "busybox",
                            "command": ["sleep", "60"],
                        }
                    ],
                    "restartPolicy": "Never",
                }
            },
        },
    }
    await batch.create_namespaced_job(namespace, body)


async def create_fake_cronjob(api_client, name, namespace):
    """
    As the name implies, this creates a scheduled CronJob.

    This can be used in tests to check for cronjobs existing, and their statuses.
    """
    batch = BatchV1Api(api_client)
    body = {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": f"create-snapshot-{name}",
            "labels": {
                "app.kubernetes.io/component": "backup",
                "app.kubernetes.io/managed-by": "crate-operator",
                "app.kubernetes.io/name": name,
                "app.kubernetes.io/part-of": "cratedb",
            },
        },
        "spec": {
            "jobTemplate": {
                "metadata": {"name": name},
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "busybox",
                                    "image": "busybox",
                                    "command": ["sleep", "60"],
                                }
                            ],
                            "restartPolicy": "Never",
                        }
                    },
                },
            },
            "schedule": "* * 1 1 0",
        },
    }
    await batch.create_namespaced_cron_job(namespace, body)


async def delete_fake_snapshot_job(api_client, name, namespace):
    batch = BatchV1Api(api_client)
    await batch.delete_namespaced_job(f"cluster-backup-{name}", namespace)


async def cluster_routing_allocation_enable_equals(
    conn_factory: Callable[[], Connection], expected_value: str
) -> bool:
    try:
        async with conn_factory() as conn:
            async with conn.cursor() as cursor:
                cluster_settings = await get_cluster_settings(cursor)

                value = (
                    cluster_settings.get("cluster", {})
                    .get("routing", {})
                    .get("allocation", {})
                    .get("enable", "")
                )
                return value == expected_value
    except (psycopg2.DatabaseError, asyncio.exceptions.TimeoutError):
        return False


async def was_notification_sent(
    mock_send_notification: mock.AsyncMock, call: mock._Call
):
    if mock_send_notification.call_count == 0:
        return False

    try:
        mock_send_notification.assert_has_calls([call], any_order=False)
        return True
    except AssertionError:
        return False


async def is_cronjob_schedule_matching(
    batch: BatchV1Api, namespace: str, name: str, schedule: str
) -> bool:
    cronjob = await batch.read_namespaced_cron_job(namespace=namespace, name=name)
    return cronjob.spec.schedule == schedule


async def mocked_coro_func_called_with(
    mocked_coro_func: mock.AsyncMock, call: mock._Call
) -> bool:
    if mocked_coro_func.call_count == 0:
        return False

    try:
        mocked_coro_func.assert_has_calls([call], any_order=False)
        return True
    except AssertionError:
        return False


async def cluster_setting_equals(
    conn_factory: Callable[[], Connection],
    setting: str,
    expected_value: Dict[Any, Any],
) -> bool:
    try:
        async with conn_factory() as conn:
            async with conn.cursor() as cursor:
                cluster_settings = await get_cluster_settings(cursor)
                value = reduce(
                    lambda k, v: k.get(v, {}), setting.split("."), cluster_settings
                )
                return value == expected_value
    except (psycopg2.DatabaseError, asyncio.exceptions.TimeoutError):
        return False


async def does_backup_metrics_pod_exist(
    core: CoreV1Api, name: str, namespace: V1Namespace
) -> bool:
    backup_metrics_pods = await get_pods_in_deployment(core, namespace, name)
    backup_metrics_name = BACKUP_METRICS_DEPLOYMENT_NAME.format(name=name)
    return any(p["name"].startswith(backup_metrics_name) for p in backup_metrics_pods)


async def does_grand_central_pod_exist(
    core: CoreV1Api, name: str, namespace: V1Namespace
) -> bool:
    pods = await get_pods_in_deployment(
        core, namespace, f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}"
    )
    return any(
        p["name"].startswith(f"{GRAND_CENTRAL_RESOURCE_PREFIX}-{name}") for p in pods
    )


async def is_cronjob_enabled(batch: BatchV1Api, namespace: str, name: str) -> bool:
    cronjob = await batch.read_namespaced_cron_job(namespace=namespace, name=name)
    return cronjob.spec.suspend is False


async def is_lb_service_ready(core: CoreV1Api, namespace: str, expected: str) -> bool:
    services = await core.list_namespaced_service(namespace)
    service = next(
        (svc for svc in services.items if svc.metadata.name == expected),
        None,
    )
    if (
        service
        and service.status
        and service.status.load_balancer
        and service.status.load_balancer.ingress
        and service.status.load_balancer.ingress[0]
    ):
        return True
    else:
        return False


def require_connection(host: Optional[str], password: Optional[str]) -> tuple[str, str]:
    assert host is not None, "host is None"
    assert password is not None, "password is None"
    return host, password
