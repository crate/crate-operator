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

from kubernetes_asyncio.client import StorageV1Api, V1ObjectMeta, V1StorageClass

from crate.operator.constants import BACKOFF_TIME
from crate.operator.utils.kubeapi import call_kubeapi

logger = logging.getLogger(__name__)

LOCAL_FS_STORAGE_CLASS_NAME = "crate-operator-local-fs"


async def assert_wait_for(
    condition, coro_func, *args, err_msg="", timeout=BACKOFF_TIME, **kwargs
):
    ret_val = await coro_func(*args, **kwargs)
    duration = 0.0
    base = 2.0
    count = 0
    while ret_val is not condition:
        count += 1
        delay = base ** (count * 0.5)
        await asyncio.sleep(delay)
        ret_val = await coro_func(*args, **kwargs)
        if ret_val is not condition and duration > timeout:
            break
        else:
            duration += delay
    assert ret_val is condition, err_msg


async def does_namespace_exist(core, namespace: str) -> bool:
    namespaces = await core.list_namespace()
    return namespace in (ns.metadata.name for ns in namespaces.items)


async def create_local_fs_storage_class():
    sapi = StorageV1Api()
    await call_kubeapi(
        sapi.create_storage_class,
        logger,
        continue_on_conflict=True,
        body=V1StorageClass(
            metadata=V1ObjectMeta(name=LOCAL_FS_STORAGE_CLASS_NAME),
            provisioner="kubernetes.io/no-provisioner",
            reclaim_policy="Delete",
            volume_binding_mode="WaitForFirstConsumer",
        ),
    )
