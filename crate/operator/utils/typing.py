# CrateDB Kubernetes Operator
# Copyright (C) 2020 Crate.io AT GmbH
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

from typing import Dict, TypedDict, TypeVar

from kubernetes_asyncio.client.models import (
    V1beta1CronJob,
    V1ConfigMap,
    V1Deployment,
    V1PersistentVolume,
    V1PersistentVolumeClaim,
    V1Secret,
    V1Service,
    V1StatefulSet,
)

LabelType = Dict[str, str]
K8sModel = TypeVar(
    "K8sModel",
    V1beta1CronJob,
    V1ConfigMap,
    V1Deployment,
    V1PersistentVolume,
    V1PersistentVolumeClaim,
    V1Secret,
    V1Service,
    V1StatefulSet,
)


class SecretKeyRef(TypedDict):
    key: str
    name: str


class SecretKeyRefContainer(TypedDict):
    secretKeyRef: SecretKeyRef
