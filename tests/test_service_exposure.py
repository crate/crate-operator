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

from unittest import mock

import pytest

from crate.operator.constants import (
    ANNOTATION_EXTERNAL_DNS_HOSTNAME,
    ANNOTATION_EXTERNAL_DNS_HOSTNAME_ALPHA,
)
from crate.operator.exposure import patch_service_exposure

from .utils import run_change_exposure

AWS_LB_TYPE = "service.beta.kubernetes.io/aws-load-balancer-type"
DNS_ANNOTATIONS = (
    ANNOTATION_EXTERNAL_DNS_HOSTNAME_ALPHA,
    ANNOTATION_EXTERNAL_DNS_HOSTNAME,
)


async def _patch_body(new_exposure, dns_record, source_ranges=None) -> dict:
    core = mock.AsyncMock()
    with mock.patch("crate.operator.create.config.CLOUD_PROVIDER", "aws"):
        await patch_service_exposure(
            core,
            "ns",
            "c",
            new_exposure,
            source_ranges,
            dns_record,
            None,
            mock.MagicMock(),
        )
    return core.patch_namespaced_service.await_args.kwargs["body"]


@pytest.mark.parametrize("dns_record", [None, "c.example.com"])
async def test_patch_service_to_clusterip(dns_record):
    body = await _patch_body("clusterip", dns_record, ["10.0.0.0/8"])

    assert body["spec"] == {
        "type": "ClusterIP",
        "externalTrafficPolicy": None,
        "loadBalancerSourceRanges": None,
    }
    annotations = body["metadata"]["annotations"]
    # The merge patch must not remove this annotation. The service controller
    # needs it to delete the NLB (#851).
    assert AWS_LB_TYPE not in annotations
    for key in DNS_ANNOTATIONS:
        assert annotations[key] is None


async def test_patch_service_clusterip_to_loadbalancer_without_dns():
    body = await _patch_body("loadbalancer", None)

    assert body["spec"] == {
        "type": "LoadBalancer",
        "externalTrafficPolicy": "Local",
        "loadBalancerSourceRanges": None,
    }
    annotations = body["metadata"]["annotations"]
    assert annotations[AWS_LB_TYPE] == "nlb"
    for key in DNS_ANNOTATIONS:
        assert key not in annotations


def _body(exposure: str, **cluster) -> dict:
    return {
        "spec": {"cluster": {"name": "c", "exposure": exposure, **cluster}},
        "metadata": {"name": "c"},
    }


async def test_change_exposure_clusterip_to_loadbalancer_without_dns():
    mocks = await run_change_exposure(_body("loadbalancer"), _body("clusterip"))

    patch_svc = mocks["patch_service_exposure"]
    patch_svc.assert_awaited_once()
    args = patch_svc.await_args.args
    # (core, namespace, name, new_exposure, source_ranges, dns_record, ...)
    assert args[3] == "loadbalancer"
    assert args[5] is None
    mocks["create_traefik_resources"].assert_not_awaited()
    mocks["delete_traefik_resources"].assert_not_awaited()


async def test_change_exposure_traefik_to_clusterip_deletes_traefik_resources():
    mocks = await run_change_exposure(
        _body("clusterip", externalDNS="c.example.com"),
        _body("traefik", externalDNS="c.example.com"),
    )

    mocks["delete_traefik_resources"].assert_awaited_once_with("ns", "c")
    mocks["create_traefik_resources"].assert_not_awaited()
