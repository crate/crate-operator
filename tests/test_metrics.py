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

import time

from prometheus_client import REGISTRY

from crate.operator.prometheus import PrometheusClusterStatus, report_cluster_status


def test_will_report_metrics_for_clusters():
    last_reported = int(time.time())
    report_cluster_status("id", PrometheusClusterStatus.GREEN, last_reported)
    metrics = list(REGISTRY.collect())
    cloud_clusters_health = next(
        filter(lambda metric: metric.name == "cloud_clusters_health", metrics), None
    )
    cloud_clusters_last_seen = next(
        filter(lambda metric: metric.name == "cloud_clusters_last_seen", metrics), None
    )
    assert cloud_clusters_health.samples[0].value == 0
    assert cloud_clusters_last_seen.samples[0].value == last_reported


def test_will_expire_clusters_that_have_not_reported_for_a_while():
    last_reported = int(time.time()) - 100000
    report_cluster_status("id", PrometheusClusterStatus.GREEN, last_reported)
    metrics = list(REGISTRY.collect())
    cloud_clusters_health = next(
        filter(lambda metric: metric.name == "cloud_clusters_health", metrics), None
    )
    cloud_clusters_last_seen = next(
        filter(lambda metric: metric.name == "cloud_clusters_last_seen", metrics), None
    )
    assert cloud_clusters_health.samples == []
    assert cloud_clusters_last_seen.samples == []


def test_will_not_report_last_seen_for_unreachable_clusters():
    report_cluster_status("id", PrometheusClusterStatus.UNREACHABLE)
    metrics = list(REGISTRY.collect())
    cloud_clusters_health = next(
        filter(lambda metric: metric.name == "cloud_clusters_health", metrics), None
    )
    cloud_clusters_last_seen = next(
        filter(lambda metric: metric.name == "cloud_clusters_last_seen", metrics), None
    )
    assert (
        cloud_clusters_health.samples[0].value
        == PrometheusClusterStatus.UNREACHABLE.value
    )
    assert cloud_clusters_last_seen.samples == []
