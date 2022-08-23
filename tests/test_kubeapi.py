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

import pytest
from kubernetes_asyncio.client import (
    ApiException,
    CoreV1Api,
    V1DeleteOptions,
    V1ObjectMeta,
    V1Secret,
)

from crate.operator.utils.formatting import b64decode, b64encode
from crate.operator.utils.kubeapi import call_kubeapi

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.asyncio


@pytest.mark.k8s
async def test_success(faker, namespace, api_client):
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    password = faker.password(length=12)
    await call_kubeapi(
        core.create_namespaced_secret,
        logger,
        namespace=namespace.metadata.name,
        body=V1Secret(
            data={"password": b64encode(password)},
            metadata=V1ObjectMeta(name=name),
            type="Opaque",
        ),
    )
    secret = await core.read_namespaced_secret(
        name=name, namespace=namespace.metadata.name
    )
    assert b64decode(secret.data["password"]) == password


@pytest.mark.k8s
async def test_absent_raises(faker, namespace, api_client):
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    with pytest.raises(ApiException):
        await call_kubeapi(
            core.delete_namespaced_secret,
            logger,
            namespace=namespace.metadata.name,
            name=name,
            body=V1DeleteOptions(),
        )


@pytest.mark.k8s
async def test_absent_logs(faker, namespace, caplog, api_client):
    caplog.set_level(logging.DEBUG, logger=__name__)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    ns = namespace.metadata.name
    await call_kubeapi(
        core.delete_namespaced_secret,
        logger,
        continue_on_absence=True,
        namespace=ns,
        name=name,
        body=V1DeleteOptions(),
    )
    assert (
        f"Failed deleting '{ns}/{name}' because it doesn't exist. Continuing."
        in caplog.messages
    )


@pytest.mark.k8s
async def test_conflict_raises(faker, namespace, api_client):
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    ns = namespace.metadata.name
    password1 = faker.password(length=12)
    password2 = faker.password(length=12)
    await core.create_namespaced_secret(
        namespace=ns,
        body=V1Secret(
            data={"password": b64encode(password1)},
            metadata=V1ObjectMeta(name=name),
            type="Opaque",
        ),
    )
    with pytest.raises(ApiException):
        await call_kubeapi(
            core.create_namespaced_secret,
            logger,
            namespace=ns,
            body=V1Secret(
                data={"password": b64encode(password2)},
                metadata=V1ObjectMeta(name=name),
                type="Opaque",
            ),
        )
    secret = await core.read_namespaced_secret(name=name, namespace=ns)
    assert b64decode(secret.data["password"]) == password1


@pytest.mark.k8s
async def test_conflict_logs(faker, namespace, caplog, api_client):
    caplog.set_level(logging.DEBUG, logger=__name__)
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    ns = namespace.metadata.name
    password1 = faker.password(length=12)
    password2 = faker.password(length=12)
    await core.create_namespaced_secret(
        namespace=ns,
        body=V1Secret(
            data={"password": b64encode(password1)},
            metadata=V1ObjectMeta(name=name),
            type="Opaque",
        ),
    )
    await call_kubeapi(
        core.create_namespaced_secret,
        logger,
        continue_on_conflict=True,
        namespace=ns,
        body=V1Secret(
            data={"password": b64encode(password2)},
            metadata=V1ObjectMeta(name=name),
            type="Opaque",
        ),
    )
    secret = await core.read_namespaced_secret(name=name, namespace=ns)
    assert b64decode(secret.data["password"]) == password1
    assert (
        f"Failed creating V1Secret '{ns}/{name}' because it already exists. Continuing."
        in caplog.messages
    )
