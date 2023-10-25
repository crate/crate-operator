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

import enum

API_GROUP = "cloud.crate.io"
RESOURCE_CONFIGMAP = "configmaps"
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

BACKUP_METRICS_DEPLOYMENT_NAME = "backup-metrics-{name}"
DATA_NODE_NAME = "hot"
DATA_PVC_NAME_PREFIX = "data"
SQL_EXPORTER_CONFIGMAP_PREFIX = "crate-sql-exporter"

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


class SnapshotRestoreType(enum.Enum):
    ALL = "all"
    METADATA = "metadata"
    TABLES = "tables"
    SECTIONS = "sections"
    PARTITIONS = "partitions"


class Nodepool(str, enum.Enum):
    SHARED = "shared"
    DEDICATED = "dedicated"
