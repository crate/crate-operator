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

import pytest

from crate.operator.grand_central import (
    _grand_central_hostname,
    get_grand_central_exposure,
    grand_central_uses_traefik,
)


@pytest.mark.parametrize(
    "spec, expected_use_traefik",
    [
        ({"cluster": {"exposure": "traefik"}}, True),
        ({"cluster": {"exposure": "loadbalancer"}}, False),
        ({"cluster": {}}, False),
        ({}, False),
        ({"cluster": {"exposure": "traefik"}, "grandCentral": {}}, True),
        ({"cluster": {"exposure": "traefik"}, "grandCentral": None}, True),
        # Decoupled: cluster on LoadBalancer, grand-central explicitly on Traefik.
        (
            {
                "cluster": {"exposure": "loadbalancer"},
                "grandCentral": {"exposure": "traefik"},
            },
            True,
        ),
        # Decoupled (opposite): cluster on Traefik, grand-central explicitly on
        # nginx -> resolves to False so the nginx Ingress path is taken.
        (
            {
                "cluster": {"exposure": "traefik"},
                "grandCentral": {"exposure": "nginx"},
            },
            False,
        ),
    ],
)
def test_grand_central_uses_traefik(spec, expected_use_traefik):
    assert grand_central_uses_traefik(spec) is expected_use_traefik


def test_explicit_grand_central_exposure_wins_over_cluster():
    spec = {
        "cluster": {"exposure": "loadbalancer"},
        "grandCentral": {"exposure": "traefik"},
    }
    assert get_grand_central_exposure(spec) == "traefik"


def test_grand_central_exposure_falls_back_to_cluster():
    spec = {"cluster": {"exposure": "traefik"}}
    assert get_grand_central_exposure(spec) == "traefik"


@pytest.mark.parametrize(
    "cluster_name, external_dns, expected",
    [
        # Non-colliding: cluster name appears only as the leading label.
        (
            "mycluster",
            "mycluster.eks1.us-west-2.aws.cratedb.net.",
            "mycluster.gc.eks1.us-west-2.aws.cratedb.net",
        ),
        # Collision with the region: 'us-west' also occurs in 'us-west-2'.
        (
            "us-west",
            "us-west.eks1.us-west-2.aws.cratedb.net.",
            "us-west.gc.eks1.us-west-2.aws.cratedb.net",
        ),
        # Collision with the cloud suffix: 'aws' also occurs in '.aws.'.
        (
            "aws",
            "aws.eks1.us-west-2.aws.cratedb.net.",
            "aws.gc.eks1.us-west-2.aws.cratedb.net",
        ),
        # Cluster literally named 'gc'.
        (
            "gc",
            "gc.eks1.eu-central-1.azure.cratedb.net.",
            "gc.gc.eks1.eu-central-1.azure.cratedb.net",
        ),
    ],
)
def test_grand_central_hostname_only_replaces_leading_label(
    cluster_name, external_dns, expected
):
    assert _grand_central_hostname(cluster_name, external_dns) == expected
