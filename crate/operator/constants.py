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

SYSTEM_USERNAME = "system"

BACKOFF_TIME = 60.0


class CloudProvider(str, enum.Enum):
    AWS = "aws"
    AZURE = "azure"
