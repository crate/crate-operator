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
async def test_success(faker, namespace):
    core = CoreV1Api()
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
async def test_absent_raises(faker, namespace):
    core = CoreV1Api()
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
async def test_absent_logs(faker, namespace, caplog):
    caplog.set_level(logging.DEBUG, logger=__name__)
    core = CoreV1Api()
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
async def test_conflict_raises(faker, namespace):
    core = CoreV1Api()
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
async def test_conflict_logs(faker, namespace, caplog):
    caplog.set_level(logging.DEBUG, logger=__name__)
    core = CoreV1Api()
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
