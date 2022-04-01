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

import enum

API_GROUP = "cloud.crate.io"
RESOURCE_CRATEDB = "cratedbs"

LABEL_COMPONENT = "app.kubernetes.io/component"
LABEL_MANAGED_BY = "app.kubernetes.io/managed-by"
LABEL_NAME = "app.kubernetes.io/name"
LABEL_PART_OF = "app.kubernetes.io/part-of"
LABEL_NODE_NAME = f"{API_GROUP}/node-name"
LABEL_USER_PASSWORD = f"operator.{API_GROUP}/user-password"

SYSTEM_USERNAME = "system"

CONNECT_TIMEOUT = 10.0

KOPF_STATE_STORE_PREFIX = f"operator.{API_GROUP}"

CLUSTER_UPDATE_ID = "cluster_update"
CLUSTER_CREATE_ID = "cluster_create"

DATA_NODE_NAME = "hot"
DATA_PVC_NAME_PREFIX = "data"

SHARED_NODE_SELECTOR_KEY = "cratedb"
SHARED_NODE_SELECTOR_VALUE = "shared"
SHARED_NODE_TOLERATION_EFFECT = "NoSchedule"
SHARED_NODE_TOLERATION_KEY = "cratedb"
SHARED_NODE_TOLERATION_VALUE = "shared"


class CloudProvider(str, enum.Enum):
    AWS = "aws"
    AZURE = "azure"


class Port(enum.Enum):
    HTTP = 4200
    JMX = 6666
    PROMETHEUS = 7071
    POSTGRES = 5432
    TRANSPORT = 4300
