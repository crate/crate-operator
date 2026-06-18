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
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crate.operator.bootstrap import _wait_for_crate_container_ready


def _pod(crate_ready: bool):
    return SimpleNamespace(
        status=SimpleNamespace(
            phase="Running" if crate_ready else "Pending",
            container_statuses=[
                SimpleNamespace(
                    name="crate",
                    ready=crate_ready,
                    restart_count=0,
                    state="running" if crate_ready else "waiting",
                )
            ],
            init_container_statuses=[],
        )
    )


def _patched(read_side_effect=None, read_return=None):
    """Patch the GlobalApiClient/CoreV1Api used by the helper so read_namespaced_pod
    yields the given pod(s)."""
    core = MagicMock()
    core.read_namespaced_pod = AsyncMock(
        side_effect=read_side_effect, return_value=read_return
    )
    return (
        patch("crate.operator.bootstrap.GlobalApiClient", return_value=AsyncMock()),
        patch("crate.operator.bootstrap.CoreV1Api", return_value=core),
        core,
    )


@pytest.mark.asyncio
async def test_returns_true_immediately_when_ready():
    gac, capi, _ = _patched(read_return=_pod(True))
    with gac, capi, patch("asyncio.sleep", new_callable=AsyncMock) as sleep:
        assert await _wait_for_crate_container_ready("ns", "pod", MagicMock()) is True
        sleep.assert_not_awaited()  # ready first poll -> no waiting


@pytest.mark.asyncio
async def test_returns_true_after_container_becomes_ready():
    gac, capi, core = _patched(read_side_effect=[_pod(False), _pod(False), _pod(True)])
    with gac, capi, patch("asyncio.sleep", new_callable=AsyncMock):
        assert await _wait_for_crate_container_ready("ns", "pod", MagicMock()) is True
        assert core.read_namespaced_pod.await_count == 3


@pytest.mark.asyncio
async def test_logs_pod_state_on_timeout():
    """On timeout the helper must log the pod phase + container state so a stuck
    start is diagnosable from the run."""
    logger = MagicMock(spec=logging.Logger)
    gac, capi, _ = _patched(read_return=_pod(False))
    with gac, capi, patch("asyncio.sleep", new_callable=AsyncMock):
        assert await _wait_for_crate_container_ready("ns", "pod", logger) is False
    diag = [
        c.args[0]
        for c in logger.warning.call_args_list
        if "not ready after" in c.args[0]
    ]
    assert diag, "expected a 'not ready after' diagnostic log on timeout"
    assert "phase=" in diag[-1] and "containers=" in diag[-1]


@pytest.mark.asyncio
async def test_returns_false_when_never_ready_within_budget():
    gac, capi, _ = _patched(read_return=_pod(False))
    with gac, capi, patch("asyncio.sleep", new_callable=AsyncMock):
        result = await _wait_for_crate_container_ready(
            "ns", "pod", MagicMock(spec=logging.Logger)
        )
        assert result is False
