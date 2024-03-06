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
import time
from datetime import datetime
from typing import Optional

from prometheus_client import REGISTRY, Info
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.metrics_core import CounterMetricFamily

from crate.operator import __version__
from crate.operator.utils.k8s_api_client import GlobalApiClient

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
    cluster_name: str,
    namespace: str,
    status: PrometheusClusterStatus,
    last_reported: Optional[int] = None,
):
    CLUSTER_METRICS[cluster_id] = {
        "namespace": namespace,
        "cluster_name": cluster_name,
        "status": status,
        "last_reported": last_reported if last_reported else int(time.time()),
    }


class ClusterCollector:
    def collect(self):
        now = time.time()
        cloud_clusters_health = GaugeMetricFamily(
            "cloud_clusters_health",
            "0->GREEN, 1->YELLOW, 2->RED, 3->UNREACHABLE",
            labels=["cluster_id", "cluster_name", "exported_namespace"],
        )
        cloud_clusters_last_seen = GaugeMetricFamily(
            "cloud_clusters_last_seen",
            "Unix timestamp of when a cluster was last seen (not unreachable).",
            labels=["cluster_id", "cluster_name", "exported_namespace"],
        )
        for cluster_id, metrics in CLUSTER_METRICS.items():
            if now - metrics["last_reported"] < LAST_SEEN_THRESHOLD:
                cloud_clusters_health.add_metric(
                    [cluster_id, metrics["cluster_name"], metrics["namespace"]],
                    metrics["status"].value,
                )
                if metrics["status"] != PrometheusClusterStatus.UNREACHABLE:
                    cloud_clusters_last_seen.add_metric(
                        [cluster_id, metrics["cluster_name"], metrics["namespace"]],
                        metrics["last_reported"],
                    )

        yield cloud_clusters_health
        yield cloud_clusters_last_seen

        k8s_api_sessions = GaugeMetricFamily(
            "crate_operator_open_k8s_api_sessions",
            "Number of sessions open to the k8s api",
        )
        k8s_api_sessions_created = CounterMetricFamily(
            "crate_operator_k8s_api_sessions_created",
            "Number of sessions open to the k8s api",
        )
        k8s_api_sessions_removed = CounterMetricFamily(
            "crate_operator_k8s_api_sessions_removed",
            "Number of sessions open to the k8s api",
        )
        k8s_api_sessions.add_metric([], GlobalApiClient.get_instance_count())
        k8s_api_sessions_created.add_metric([], GlobalApiClient.get_created_instances())
        k8s_api_sessions_removed.add_metric([], GlobalApiClient.get_removed_instances())
        yield k8s_api_sessions
        yield k8s_api_sessions_created
        yield k8s_api_sessions_removed


REGISTRY.register(ClusterCollector())
