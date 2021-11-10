# CrateDB Kubernetes Operator
# Copyright (C) 2021 Crate.IO GmbH
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

import kopf
from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.api_client import ApiClient

from crate.operator.constants import LABEL_USER_PASSWORD
from crate.operator.utils.kubeapi import ensure_user_password_label


async def migrate_user_password_label(
    namespace: str,
    spec: kopf.Spec,
):
    if "users" in spec:
        async with ApiClient() as api_client:
            for user_spec in spec["users"]:
                core = CoreV1Api(api_client)

                secret_name = user_spec["password"]["secretKeyRef"]["name"]
                secret = await core.read_namespaced_secret(
                    namespace=namespace, name=secret_name
                )
                if (
                    secret.metadata.labels is None
                    or LABEL_USER_PASSWORD not in secret.metadata.labels
                ):
                    await ensure_user_password_label(
                        core, namespace, user_spec["password"]["secretKeyRef"]["name"]
                    )
