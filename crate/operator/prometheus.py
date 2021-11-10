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

import enum
import time
from datetime import datetime
from typing import Optional

from prometheus_client import REGISTRY, Info
from prometheus_client.core import GaugeMetricFamily

from crate.operator import __version__

i = Info("svc", "Service Info")
i.info(
    {
        "name": "crate-operator",
        "version": __version__,
        "started": datetime.utcnow().isoformat(),
    }
)

CLUSTER_METRICS = {}
LAST_SEEN_THRESHOLD = 300


class PrometheusClusterStatus(enum.Enum):
    GREEN = 0
    YELLOW = 1
    RED = 2
    UNREACHABLE = 3


def report_cluster_status(
    cluster_id: str,
    status: PrometheusClusterStatus,
    last_reported: Optional[int] = None,
):
    CLUSTER_METRICS[cluster_id] = {
        "status": status,
        "last_reported": last_reported if last_reported else int(time.time()),
    }


class ClusterCollector:
    def collect(self):
        now = time.time()
        cloud_clusters_health = GaugeMetricFamily(
            "cloud_clusters_health",
            "0->GREEN, 1->YELLOW, 2->RED, 3->UNREACHABLE",
            labels=["cluster_id"],
        )
        cloud_clusters_last_seen = GaugeMetricFamily(
            "cloud_clusters_last_seen",
            "Unix timestamp of when a cluster was last seen (not unreachable).",
            labels=["cluster_id"],
        )
        for cluster_id, metrics in CLUSTER_METRICS.items():
            if now - metrics["last_reported"] < LAST_SEEN_THRESHOLD:
                cloud_clusters_health.add_metric([cluster_id], metrics["status"].value)
                if metrics["status"] != PrometheusClusterStatus.UNREACHABLE:
                    cloud_clusters_last_seen.add_metric(
                        [cluster_id], metrics["last_reported"]
                    )

        yield cloud_clusters_health
        yield cloud_clusters_last_seen


REGISTRY.register(ClusterCollector())
