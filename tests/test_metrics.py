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
from typing import Optional

from prometheus_client import REGISTRY
from prometheus_client.core import Metric

from crate.operator.prometheus import PrometheusClusterStatus, report_cluster_status

logger = logging.getLogger(__name__)


def test_will_report_metrics_for_clusters():
    last_reported = int(time.time())
    report_cluster_status(
        "id", "cluster-name", "ns1", PrometheusClusterStatus.GREEN, last_reported
    )

    metrics = list(REGISTRY.collect())

    cloud_clusters_health: Optional[Metric] = next(
        (m for m in metrics if m.name == "cloud_clusters_health"), None
    )
    cloud_clusters_last_seen: Optional[Metric] = next(
        (m for m in metrics if m.name == "cloud_clusters_last_seen"), None
    )

    if cloud_clusters_health is None or cloud_clusters_last_seen is None:
        raise AssertionError("Expected metrics were not found")

    cloud_clusters_health_sample = next(
        (s for s in cloud_clusters_health.samples if s.labels["cluster_id"] == "id"),
        None,
    )
    cloud_clusters_last_seen_sample = next(
        (s for s in cloud_clusters_last_seen.samples if s.labels["cluster_id"] == "id"),
        None,
    )

    assert cloud_clusters_health_sample is not None, "Health sample missing"
    assert cloud_clusters_last_seen_sample is not None, "Last seen sample missing"

    assert cloud_clusters_health_sample.value == 0
    assert cloud_clusters_last_seen_sample.value == last_reported
    assert cloud_clusters_health_sample.labels["exported_namespace"] == "ns1"
    assert cloud_clusters_last_seen_sample.labels["exported_namespace"] == "ns1"
    assert cloud_clusters_last_seen_sample.labels["cluster_name"] == "cluster-name"


def test_will_expire_clusters_that_have_not_reported_for_a_while():
    last_reported = int(time.time()) - 100000
    report_cluster_status(
        "id", "cluster-name", "ns1", PrometheusClusterStatus.GREEN, last_reported
    )

    metrics = list(REGISTRY.collect())

    cloud_clusters_health: Optional[Metric] = next(
        (m for m in metrics if m.name == "cloud_clusters_health"), None
    )
    cloud_clusters_last_seen: Optional[Metric] = next(
        (m for m in metrics if m.name == "cloud_clusters_last_seen"), None
    )

    cloud_clusters_health_sample = (
        next(
            (
                s
                for s in cloud_clusters_health.samples
                if s.labels["cluster_id"] == "id"
            ),
            None,
        )
        if cloud_clusters_health
        else None
    )

    cloud_clusters_last_seen_sample = (
        next(
            (
                s
                for s in cloud_clusters_last_seen.samples
                if s.labels["cluster_id"] == "id"
            ),
            None,
        )
        if cloud_clusters_last_seen
        else None
    )

    assert cloud_clusters_health_sample is None
    assert cloud_clusters_last_seen_sample is None


def test_will_not_report_last_seen_for_unreachable_clusters():
    report_cluster_status(
        "id", "cluster-name", "ns1", PrometheusClusterStatus.UNREACHABLE
    )

    metrics = list(REGISTRY.collect())

    cloud_clusters_health: Optional[Metric] = next(
        (m for m in metrics if m.name == "cloud_clusters_health"), None
    )
    cloud_clusters_last_seen: Optional[Metric] = next(
        (m for m in metrics if m.name == "cloud_clusters_last_seen"), None
    )

    if cloud_clusters_health is None:
        raise AssertionError("Health metric missing")

    cloud_clusters_health_sample = next(
        (s for s in cloud_clusters_health.samples if s.labels["cluster_id"] == "id"),
        None,
    )

    cloud_clusters_last_seen_sample = (
        next(
            (
                s
                for s in cloud_clusters_last_seen.samples
                if s.labels["cluster_id"] == "id"
            ),
            None,
        )
        if cloud_clusters_last_seen
        else None
    )

    assert cloud_clusters_health_sample is not None, "Health sample missing"
    assert (
        cloud_clusters_health_sample.value == PrometheusClusterStatus.UNREACHABLE.value
    )
    assert cloud_clusters_health_sample.labels["exported_namespace"] == "ns1"
    assert cloud_clusters_health_sample.labels["cluster_name"] == "cluster-name"
    assert cloud_clusters_last_seen_sample is None
