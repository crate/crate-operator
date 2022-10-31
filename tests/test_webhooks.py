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
import logging

import pytest
from aiohttp import web
from aiohttp.helpers import BasicAuth
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from crate.operator.webhooks import (
    WebhookClient,
    WebhookClusterHealthPayload,
    WebhookEvent,
    WebhookInfoChangedPayload,
    WebhookPayload,
    WebhookScalePayload,
    WebhookStatus,
    WebhookTemporaryFailurePayload,
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
        compute_changed_data=None,
        temporary_failure_data=None,
        info_data=None,
        health_data=None,
        feedback_data=None,
        backup_schedule_changed_data=None,
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
        "compute_changed_data": None,
        "temporary_failure_data": None,
        "info_data": None,
        "health_data": None,
        "feedback_data": None,
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
        compute_changed_data=None,
        temporary_failure_data=None,
        info_data=None,
        health_data=None,
        feedback_data=None,
        backup_schedule_changed_data=None,
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
        "compute_changed_data": None,
        "temporary_failure_data": None,
        "info_data": None,
        "health_data": None,
        "feedback_data": None,
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

    async def setUpAsync(self):
        self.webhook_client = WebhookClient()
        self.webhook_client.configure(
            f"{self.server.scheme}://{self.server.host}:{self.server.port}/some/path/",
            "itsme",
            "secr3t password",
        )

    @unittest_run_loop
    async def test_send_scale_notification(self):
        response = await self.webhook_client.send_notification(
            "my-namespace",
            "my-cluster",
            WebhookEvent.SCALE,
            WebhookScalePayload(
                old_data_replicas={"a": 1},
                new_data_replicas={"a": 2},
                old_master_replicas=3,
                new_master_replicas=4,
            ),
            WebhookStatus.SUCCESS,
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
                "compute_changed_data": None,
                "temporary_failure_data": None,
                "info_data": None,
                "health_data": None,
                "feedback_data": None,
            },
        }

    @unittest_run_loop
    async def test_send_upgrade_notification(self):
        response = await self.webhook_client.send_notification(
            "my-namespace",
            "my-cluster",
            WebhookEvent.UPGRADE,
            WebhookUpgradePayload(
                old_registry="a", new_registry="b", old_version="c", new_version="d"
            ),
            WebhookStatus.SUCCESS,
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
                "compute_changed_data": None,
                "temporary_failure_data": None,
                "info_data": None,
                "health_data": None,
                "feedback_data": None,
            },
        }

    @unittest_run_loop
    async def test_send_delay_notification(self):
        response = await self.webhook_client.send_notification(
            "my-namespace",
            "my-cluster",
            WebhookEvent.DELAY,
            WebhookTemporaryFailurePayload(reason="A snapshot is in progress"),
            WebhookStatus.TEMPORARY_FAILURE,
            logging.getLogger(__name__),
        )
        assert response.status == 200
        data = await response.json()
        assert data == {
            "username": "itsme",
            "password": "secr3t password",
            "payload": {
                "event": "delay",
                "status": "temporary_failure",
                "namespace": "my-namespace",
                "cluster": "my-cluster",
                "scale_data": None,
                "upgrade_data": None,
                "compute_changed_data": None,
                "info_data": None,
                "temporary_failure_data": {
                    "reason": "A snapshot is in progress",
                },
                "health_data": None,
                "feedback_data": None,
            },
        }

    @unittest_run_loop
    async def test_send_info_changed_notification(self):
        response = await self.webhook_client.send_notification(
            "my-namespace",
            "my-cluster",
            WebhookEvent.INFO_CHANGED,
            WebhookInfoChangedPayload(external_ip="192.168.1.10"),
            WebhookStatus.SUCCESS,
            logging.getLogger(__name__),
        )
        assert response.status == 200
        data = await response.json()
        assert data == {
            "username": "itsme",
            "password": "secr3t password",
            "payload": {
                "event": "info_changed",
                "status": "success",
                "namespace": "my-namespace",
                "cluster": "my-cluster",
                "scale_data": None,
                "upgrade_data": None,
                "compute_changed_data": None,
                "info_data": {"external_ip": "192.168.1.10"},
                "temporary_failure_data": None,
                "health_data": None,
                "feedback_data": None,
            },
        }

    @unittest_run_loop
    async def test_send_health_notification(self):
        response = await self.webhook_client.send_notification(
            "my-namespace",
            "my-cluster",
            WebhookEvent.HEALTH,
            WebhookClusterHealthPayload(status="GREEN"),
            WebhookStatus.SUCCESS,
            logging.getLogger(__name__),
        )
        assert response.status == 200
        data = await response.json()
        assert data == {
            "username": "itsme",
            "password": "secr3t password",
            "payload": {
                "event": "health",
                "status": "success",
                "namespace": "my-namespace",
                "cluster": "my-cluster",
                "scale_data": None,
                "upgrade_data": None,
                "compute_changed_data": None,
                "info_data": None,
                "temporary_failure_data": None,
                "feedback_data": None,
                "backup_schedule_changed_data": None,
                "health_data": {"status": "GREEN"},
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
        response = await client.send_notification(
            "my-namespace",
            "my-cluster",
            WebhookEvent.UPGRADE,
            WebhookUpgradePayload(
                old_registry="a", new_registry="b", old_version="c", new_version="d"
            ),
            WebhookStatus.SUCCESS,
            logging.getLogger(__name__),
        )
        assert response.status == 418

    @unittest_run_loop
    async def test_not_configured(self):
        client = WebhookClient()
        response = await client.send_notification(
            "my-namespace",
            "my-cluster",
            WebhookEvent.UPGRADE,
            WebhookUpgradePayload(
                old_registry="a", new_registry="b", old_version="c", new_version="d"
            ),
            WebhookStatus.SUCCESS,
            logging.getLogger(__name__),
        )
        assert response is None
