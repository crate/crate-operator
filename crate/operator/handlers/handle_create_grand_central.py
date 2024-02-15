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

import kopf

from crate.operator.grand_central import create_grand_central_backend
from crate.operator.operations import get_cratedb_resource
from crate.operator.utils.kopf import subhandler_partial

logger = logging.getLogger(__name__)


async def create_grand_central(
    namespace: str,
    name: str,
    old: kopf.Body,
    new: kopf.Body,
    logger: logging.Logger,
):
    if (old is None or not old.get("backendEnabled")) and (
        new is not None and new.get("backendEnabled")
    ):
        cratedb = await get_cratedb_resource(namespace, name)
        kopf.register(
            fn=subhandler_partial(
                create_grand_central_backend,
                namespace,
                name,
                cratedb["spec"],
                cratedb["metadata"],
                logger,
            ),
            id="create_grand_central",
        )
