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

from unittest.mock import AsyncMock, patch

import pytest
from aiohttp.client_exceptions import ClientConnectorError
from kubernetes_asyncio.client.exceptions import ApiException

from tests.utils import retry_on_throttle


def _make(status, headers):
    e = ApiException(status=status, reason="boom")
    e.headers = headers
    return e


@pytest.mark.asyncio
async def test_retries_429_then_succeeds():
    call = AsyncMock(side_effect=[_make(429, {}), _make(429, {}), "ok"])
    with patch("asyncio.sleep", new_callable=AsyncMock) as sleep:
        result = await retry_on_throttle(call)
    assert result == "ok"
    assert call.await_count == 3
    assert sleep.await_count == 2  # slept before each retry


@pytest.mark.asyncio
async def test_honours_retry_after_header():
    call = AsyncMock(side_effect=[_make(429, {"Retry-After": "3"}), "ok"])
    with patch("asyncio.sleep", new_callable=AsyncMock) as sleep:
        result = await retry_on_throttle(call)
    assert result == "ok"
    sleep.assert_awaited_once_with(3.0)


@pytest.mark.asyncio
async def test_retries_5xx_and_connection_errors():
    fake_key = AsyncMock()
    fake_key.ssl = None
    call = AsyncMock(
        side_effect=[
            _make(503, {}),
            ClientConnectorError(fake_key, OSError("reset")),
            "ok",
        ]
    )
    with patch("asyncio.sleep", new_callable=AsyncMock):
        result = await retry_on_throttle(call)
    assert result == "ok"
    assert call.await_count == 3


@pytest.mark.asyncio
async def test_non_transient_error_raises_immediately():
    call = AsyncMock(side_effect=_make(403, {}))
    with patch("asyncio.sleep", new_callable=AsyncMock) as sleep:
        with pytest.raises(ApiException) as exc:
            await retry_on_throttle(call)
    assert exc.value.status == 403
    assert call.await_count == 1
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_gives_up_after_deadline_and_reraises():
    call = AsyncMock(side_effect=_make(429, {}))
    # Advance the clock past the deadline on the first elapsed-check so the
    # second failure is surfaced rather than retried forever.
    with (
        patch("asyncio.sleep", new_callable=AsyncMock),
        patch("tests.utils.time.monotonic", side_effect=[0.0, 100.0, 100.0]),
    ):
        with pytest.raises(ApiException) as exc:
            await retry_on_throttle(call, deadline=90.0)
    assert exc.value.status == 429
