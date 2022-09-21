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

import datetime
import hashlib

import kopf

from crate.operator.config import get_backoff
from crate.operator.operations import (
    AfterClusterUpdateSubHandler,
    BeforeClusterUpdateSubHandler,
    RestartSubHandler,
)
from crate.operator.upgrade import AfterUpgradeSubHandler, UpgradeSubHandler


async def upgrade_cratedb(
    namespace: str,
    name: str,
    patch: kopf.Patch,
    status: kopf.Status,
    diff: kopf.Diff,
    started: datetime.datetime,
    handler_id: str,
    field: str,
):
    context = status.get(handler_id)
    hash_string = str(diff) + str(started)
    hash = hashlib.md5(hash_string.encode("utf-8")).hexdigest()
    if not context:
        context = {"ref": hash}
    elif context.get("ref", "") != hash:
        context["ref"] = hash

    depends_on = [f"{handler_id}/{field}/before_cluster_update"]

    kopf.register(
        fn=BeforeClusterUpdateSubHandler(namespace, name, hash, context)(),
        id="before_cluster_update",
        backoff=get_backoff(),
    )

    kopf.register(
        fn=UpgradeSubHandler(
            namespace, name, hash, context, depends_on=depends_on.copy()
        )(),
        id="upgrade",
        backoff=get_backoff(),
    )
    depends_on.append(f"{handler_id}/{field}/upgrade")

    kopf.register(
        fn=RestartSubHandler(
            namespace, name, hash, context, depends_on=depends_on.copy()
        )(),
        id="restart",
        backoff=get_backoff(),
    )
    depends_on.append(f"{handler_id}/{field}/restart")

    kopf.register(
        fn=AfterUpgradeSubHandler(
            namespace, name, hash, context, depends_on=depends_on.copy()
        )(),
        id="after_upgrade",
        backoff=get_backoff(),
    )
    depends_on.append(f"{handler_id}/{field}/after_upgrade")

    kopf.register(
        fn=AfterClusterUpdateSubHandler(
            namespace,
            name,
            hash,
            context,
            depends_on=depends_on.copy(),
            run_on_dep_failures=True,
        )(),
        id="after_cluster_update",
        backoff=get_backoff(),
    )

    patch.status["upgrade"] = context
