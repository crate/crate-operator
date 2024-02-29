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

import logging
import time

from prometheus_client import REGISTRY

from crate.operator.prometheus import PrometheusClusterStatus, report_cluster_status

logger = logging.getLogger(__name__)


def test_will_report_metrics_for_clusters():
    last_reported = int(time.time())
    report_cluster_status(
        "id", "cluster-name", "ns1", PrometheusClusterStatus.GREEN, last_reported
    )
    metrics = list(REGISTRY.collect())
    cloud_clusters_health = next(
        filter(lambda metric: metric.name == "cloud_clusters_health", metrics), None
    )
    cloud_clusters_last_seen = next(
        filter(lambda metric: metric.name == "cloud_clusters_last_seen", metrics), None
    )
    cloud_clusters_health_sample = next(
        filter(lambda s: s.labels["cluster_id"] == "id", cloud_clusters_health.samples),
        None,
    )
    cloud_clusters_last_seen = next(
        filter(
            lambda s: s.labels["cluster_id"] == "id", cloud_clusters_last_seen.samples
        ),
        None,
    )
    assert cloud_clusters_health_sample.value == 0
    assert cloud_clusters_last_seen.value == last_reported
    assert cloud_clusters_health_sample.labels["exported_namespace"] == "ns1"
    assert cloud_clusters_last_seen.labels["exported_namespace"] == "ns1"
    assert cloud_clusters_last_seen.labels["cluster_name"] == "cluster-name"


def test_will_expire_clusters_that_have_not_reported_for_a_while():
    last_reported = int(time.time()) - 100000
    report_cluster_status(
        "id", "cluster-name", "ns1", PrometheusClusterStatus.GREEN, last_reported
    )
    metrics = list(REGISTRY.collect())
    cloud_clusters_health = next(
        filter(lambda metric: metric.name == "cloud_clusters_health", metrics), None
    )
    cloud_clusters_last_seen = next(
        filter(lambda metric: metric.name == "cloud_clusters_last_seen", metrics), None
    )
    cloud_clusters_health_sample = next(
        filter(lambda s: s.labels["cluster_id"] == "id", cloud_clusters_health.samples),
        None,
    )
    cloud_clusters_last_seen_sample = next(
        filter(
            lambda s: s.labels["cluster_id"] == "id", cloud_clusters_last_seen.samples
        ),
        None,
    )
    assert cloud_clusters_health_sample is None
    assert cloud_clusters_last_seen_sample is None


def test_will_not_report_last_seen_for_unreachable_clusters():
    report_cluster_status(
        "id", "cluster-name", "ns1", PrometheusClusterStatus.UNREACHABLE
    )
    metrics = list(REGISTRY.collect())
    cloud_clusters_health = next(
        filter(lambda metric: metric.name == "cloud_clusters_health", metrics), None
    )
    cloud_clusters_last_seen = next(
        filter(lambda metric: metric.name == "cloud_clusters_last_seen", metrics), None
    )
    cloud_clusters_health_sample = next(
        filter(lambda s: s.labels["cluster_id"] == "id", cloud_clusters_health.samples),
        None,
    )
    cloud_clusters_last_seen_sample = next(
        filter(
            lambda s: s.labels["cluster_id"] == "id", cloud_clusters_last_seen.samples
        ),
        None,
    )
    assert (
        cloud_clusters_health_sample.value == PrometheusClusterStatus.UNREACHABLE.value
    )
    assert cloud_clusters_health_sample.labels["exported_namespace"] == "ns1"
    assert cloud_clusters_health_sample.labels["cluster_name"] == "cluster-name"
    assert cloud_clusters_last_seen_sample is None
