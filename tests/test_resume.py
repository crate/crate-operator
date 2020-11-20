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

import pytest
from kubernetes_asyncio.client import CoreV1Api, V1ObjectMeta, V1Secret

from crate.operator.constants import LABEL_USER_PASSWORD
from crate.operator.main import update_cratedb_resource
from crate.operator.utils.formatting import b64encode

pytestmark = [pytest.mark.k8s, pytest.mark.asyncio]


async def test_resume_set_secret_labels(
    faker,
    namespace,
    cleanup_handler,
    kopf_runner,
    api_client,
):
    core = CoreV1Api(api_client)
    name = faker.domain_word()
    password1 = faker.password(length=40)
    password2 = faker.password(length=40)

    cleanup_handler.append(
        core.delete_persistent_volume(name=f"temp-pv-{namespace.metadata.name}-{name}")
    )

    await asyncio.gather(
        core.create_namespaced_secret(
            namespace=namespace.metadata.name,
            body=V1Secret(
                data={"password": b64encode(password1)},
                metadata=V1ObjectMeta(name=f"user-{name}-1"),
                type="Opaque",
            ),
        ),
        core.create_namespaced_secret(
            namespace=namespace.metadata.name,
            body=V1Secret(
                data={"password": b64encode(password2)},
                metadata=V1ObjectMeta(
                    name=f"user-{name}-2", labels={LABEL_USER_PASSWORD: "true"}
                ),
                type="Opaque",
            ),
        ),
    )

    secret_1 = await core.read_namespaced_secret(
        name=f"user-{name}-1", namespace=namespace.metadata.name
    )
    secret_2 = await core.read_namespaced_secret(
        name=f"user-{name}-2", namespace=namespace.metadata.name
    )

    assert secret_1.metadata.labels is None
    assert LABEL_USER_PASSWORD in secret_2.metadata.labels

    await update_cratedb_resource(
        namespace=namespace.metadata.name,
        name=f"user-{name}",
        spec={
            "cluster": {
                "imageRegistry": "crate",
                "name": "my-crate-cluster",
                "version": "4.1.5",
            },
            "nodes": {
                "data": [
                    {
                        "name": "data",
                        "replicas": 1,
                        "resources": {
                            "cpus": 0.5,
                            "disk": {
                                "count": 1,
                                "size": "16GiB",
                                "storageClass": "default",
                            },
                            "heapRatio": 0.25,
                            "memory": "1Gi",
                        },
                    }
                ]
            },
            "users": [
                {
                    "name": name,
                    "password": {
                        "secretKeyRef": {
                            "key": b64encode(password1),
                            "name": f"user-{name}-1",
                        }
                    },
                },
                {
                    "name": name,
                    "password": {
                        "secretKeyRef": {
                            "key": b64encode(password1),
                            "name": f"user-{name}-2",
                        }
                    },
                },
            ],
        },
    )

    secret_1_after_resume = await core.read_namespaced_secret(
        name=f"user-{name}-1", namespace=namespace.metadata.name
    )
    secret_2_after_resume = await core.read_namespaced_secret(
        name=f"user-{name}-2", namespace=namespace.metadata.name
    )

    assert LABEL_USER_PASSWORD in secret_1_after_resume.metadata.labels
    assert LABEL_USER_PASSWORD in secret_2_after_resume.metadata.labels
