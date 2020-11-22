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

import json
import logging

import pytest
from aiohttp import web
from aiohttp.helpers import BasicAuth
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from crate.operator.webhooks import (
    WebhookClient,
    WebhookEvent,
    WebhookPayload,
    WebhookScalePayload,
    WebhookStatus,
    WebhookUpgradePayload,
)


def test_payload_serialization_scale():
    p = WebhookPayload(
        event=WebhookEvent.SCALE,
        status=WebhookStatus.FAILURE,
        namespace="some-namespace",
        cluster="some-cluster",
        scale_data=WebhookScalePayload(
            old_data_replicas={"a": 1},
            new_data_replicas={"a": 2},
            old_master_replicas=3,
            new_master_replicas=4,
        ),
        upgrade_data=None,
    )
    assert json.loads(json.dumps(p)) == {
        "event": "scale",
        "status": "failure",
        "namespace": "some-namespace",
        "cluster": "some-cluster",
        "scale_data": {
            "old_data_replicas": {"a": 1},
            "new_data_replicas": {"a": 2},
            "old_master_replicas": 3,
            "new_master_replicas": 4,
        },
        "upgrade_data": None,
    }


def test_payload_serialization_upgrade():
    p = WebhookPayload(
        event=WebhookEvent.UPGRADE,
        status=WebhookStatus.FAILURE,
        namespace="some-namespace",
        cluster="some-cluster",
        scale_data=None,
        upgrade_data=WebhookUpgradePayload(
            old_registry="a", new_registry="b", old_version="c", new_version="d"
        ),
    )
    assert json.loads(json.dumps(p)) == {
        "event": "upgrade",
        "status": "failure",
        "namespace": "some-namespace",
        "cluster": "some-cluster",
        "scale_data": None,
        "upgrade_data": {
            "old_registry": "a",
            "new_registry": "b",
            "old_version": "c",
            "new_version": "d",
        },
    }


def test_not_configured():
    c = WebhookClient()
    assert c.configured is False


async def test_configure():
    c = WebhookClient()
    c.configure("http://localhost:1234/some/path", "itsme", "secr3t password")
    assert c.configured is True
    assert c._url == "http://localhost:1234/some/path"
    assert c._session._default_headers["Content-Type"] == "application/json"
    assert c._session._default_headers["User-Agent"].startswith("cratedb-operator/")
    assert c._session._default_auth.login == "itsme"
    assert c._session._default_auth.password == "secr3t password"


@pytest.mark.asyncio
class TestWebhookClientSending(AioHTTPTestCase):
    async def get_application(self):
        async def webhook_handler(request: web.Request):
            payload = await request.json()
            auth = BasicAuth.decode(request.headers["Authorization"])
            return web.json_response(
                {"username": auth.login, "password": auth.password, "payload": payload}
            )

        async def error_handler(request: web.Request):
            return web.Response(status=418)

        app = web.Application()
        app.router.add_post("/some/path/", webhook_handler)
        app.router.add_post("/error/", error_handler)
        return app

    @unittest_run_loop
    async def test_send_scale_notification(self):
        client = WebhookClient()
        client.configure(
            f"{self.server.scheme}://{self.server.host}:{self.server.port}/some/path/",
            "itsme",
            "secr3t password",
        )
        response = await client.send_scale_notification(
            WebhookStatus.SUCCESS,
            "my-namespace",
            "my-cluster",
            WebhookScalePayload(
                old_data_replicas={"a": 1},
                new_data_replicas={"a": 2},
                old_master_replicas=3,
                new_master_replicas=4,
            ),
            logging.getLogger(__name__),
        )
        assert response.status == 200
        data = await response.json()
        assert data == {
            "username": "itsme",
            "password": "secr3t password",
            "payload": {
                "event": "scale",
                "status": "success",
                "namespace": "my-namespace",
                "cluster": "my-cluster",
                "scale_data": {
                    "old_data_replicas": {"a": 1},
                    "new_data_replicas": {"a": 2},
                    "old_master_replicas": 3,
                    "new_master_replicas": 4,
                },
                "upgrade_data": None,
            },
        }

    @unittest_run_loop
    async def test_send_upgrade_notification(self):
        client = WebhookClient()
        client.configure(
            f"{self.server.scheme}://{self.server.host}:{self.server.port}/some/path/",
            "itsme",
            "secr3t password",
        )
        response = await client.send_upgrade_notification(
            WebhookStatus.SUCCESS,
            "my-namespace",
            "my-cluster",
            WebhookUpgradePayload(
                old_registry="a", new_registry="b", old_version="c", new_version="d"
            ),
            logging.getLogger(__name__),
        )
        assert response.status == 200
        data = await response.json()
        assert data == {
            "username": "itsme",
            "password": "secr3t password",
            "payload": {
                "event": "upgrade",
                "status": "success",
                "namespace": "my-namespace",
                "cluster": "my-cluster",
                "scale_data": None,
                "upgrade_data": {
                    "old_registry": "a",
                    "new_registry": "b",
                    "old_version": "c",
                    "new_version": "d",
                },
            },
        }

    @unittest_run_loop
    async def test_error(self):
        client = WebhookClient()
        client.configure(
            f"{self.server.scheme}://{self.server.host}:{self.server.port}/error/",
            "itsme",
            "secr3t password",
        )
        response = await client.send_upgrade_notification(
            WebhookStatus.SUCCESS,
            "my-namespace",
            "my-cluster",
            WebhookUpgradePayload(
                old_registry="a", new_registry="b", old_version="c", new_version="d"
            ),
            logging.getLogger(__name__),
        )
        assert response.status == 418

    @unittest_run_loop
    async def test_not_configured(self):
        client = WebhookClient()
        response = await client.send_upgrade_notification(
            WebhookStatus.SUCCESS,
            "my-namespace",
            "my-cluster",
            WebhookUpgradePayload(
                old_registry="a", new_registry="b", old_version="c", new_version="d"
            ),
            logging.getLogger(__name__),
        )
        assert response is None
