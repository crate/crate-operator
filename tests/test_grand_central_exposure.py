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
        (
            {
                "cluster": {"exposure": "loadbalancer"},
                "grandCentral": {"exposure": "traefik"},
            },
            True,
        ),
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
