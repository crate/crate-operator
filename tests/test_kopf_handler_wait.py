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

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.utils import wait_for_kopf_handler

HANDLER = "operator.cloud.crate.io/cluster_create"

# Short enough to keep the timeout cases quick, long enough for several polls.
TIMEOUT = 0.3
DELAY = 0.02


def _cratedb(annotation=None, status=None):
    annotations = {HANDLER: json.dumps(annotation)} if annotation else {}
    return {"metadata": {"annotations": annotations}, "status": status or {}}


def _coapi(*bodies):
    """A CustomObjectsApi whose reads return ``bodies`` in order, repeating the
    last one once exhausted."""
    coapi = MagicMock()
    remaining = list(bodies)

    async def _get(**_kwargs):
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    coapi.get_namespaced_custom_object = AsyncMock(side_effect=_get)
    return coapi


def _no_pods():
    return patch(
        "tests.utils.describe_pods", new_callable=AsyncMock, return_value="  <no pods>"
    )


async def _wait(coapi):
    await wait_for_kopf_handler(coapi, "n", "ns", HANDLER, timeout=TIMEOUT, delay=DELAY)


@pytest.mark.asyncio
async def test_returns_when_handler_finished_before_first_poll():
    """The regression: kopf purges the annotation in the same patch that ends
    the cycle, so a wait that starts afterwards only ever sees it absent."""
    status = {"cluster_create/bootstrap": {"success": True}}
    await _wait(_coapi(_cratedb(status=status)))


@pytest.mark.asyncio
async def test_returns_when_annotation_is_purged_while_polling():
    await _wait(
        _coapi(
            _cratedb(annotation={"started": "2026-08-11T04:32:30+00:00"}),
            _cratedb(status={"cluster_create": {}}),
        )
    )


@pytest.mark.asyncio
async def test_times_out_while_handler_is_still_running():
    coapi = _coapi(_cratedb(annotation={"started": "2026-08-11T04:32:30+00:00"}))
    with _no_pods():
        with pytest.raises(AssertionError, match="did not finish within"):
            await _wait(coapi)


@pytest.mark.asyncio
async def test_times_out_when_handler_never_started():
    """An empty status must not be read as "already done"."""
    with _no_pods():
        with pytest.raises(AssertionError, match="did not finish within"):
            await _wait(_coapi(_cratedb()))


@pytest.mark.asyncio
async def test_ignores_status_of_an_unrelated_handler():
    coapi = _coapi(_cratedb(status={"cluster_update/restart": {"success": True}}))
    with _no_pods():
        with pytest.raises(AssertionError, match="did not finish within"):
            await _wait(coapi)


@pytest.mark.asyncio
async def test_raises_on_permanent_failure():
    coapi = _coapi(_cratedb(annotation={"failure": True, "message": "boom"}))
    with pytest.raises(AssertionError, match="boom"):
        await _wait(coapi)
