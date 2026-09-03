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

from typing import Any, Optional
from unittest import mock

import pytest

from crate.operator.exposure import (
    ChangeExposureSubHandler,
    ChangeGrandCentralExposureSubHandler,
    migrate_grand_central_exposure,
)
from crate.operator.grand_central import (
    _grand_central_hostname,
    get_grand_central_exposure,
    grand_central_uses_traefik,
)


def _await_kwargs(m: Any) -> dict:
    assert m.await_args is not None
    return m.await_args.kwargs


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


_MIGRATE_PATCHES = (
    "crate.operator.exposure.read_grand_central_deployment",
    "crate.operator.exposure.create_grand_central_exposure",
    "crate.operator.exposure.delete_grand_central_ingress",
    "crate.operator.exposure.delete_grand_central_traefik_resources",
)


async def test_migrate_to_traefik_creates_httproute_and_deletes_stale_ingress():
    spec = {"cluster": {"name": "c", "externalDNS": "c.example.com"}}
    with (
        mock.patch(_MIGRATE_PATCHES[0]) as read_deploy,
        mock.patch(_MIGRATE_PATCHES[1]) as create_exp,
        mock.patch(_MIGRATE_PATCHES[2]) as del_ingress,
        mock.patch(_MIGRATE_PATCHES[3]) as del_traefik,
    ):
        read_deploy.return_value = object()
        await migrate_grand_central_exposure(
            "ns", "c", spec, {}, use_traefik=True, logger=mock.MagicMock()
        )

    assert _await_kwargs(create_exp)["use_traefik"] is True
    # Converge to Traefik: the stale nginx Ingress must be removed...
    del_ingress.assert_awaited_once()
    # ...and the Traefik resources must NOT be deleted.
    del_traefik.assert_not_called()


async def test_migrate_to_nginx_creates_ingress_and_deletes_traefik():
    spec = {"cluster": {"name": "c", "externalDNS": "c.example.com"}}
    with (
        mock.patch(_MIGRATE_PATCHES[0]) as read_deploy,
        mock.patch(_MIGRATE_PATCHES[1]) as create_exp,
        mock.patch(_MIGRATE_PATCHES[2]) as del_ingress,
        mock.patch(_MIGRATE_PATCHES[3]) as del_traefik,
    ):
        read_deploy.return_value = object()
        await migrate_grand_central_exposure(
            "ns", "c", spec, {}, use_traefik=False, logger=mock.MagicMock()
        )

    assert _await_kwargs(create_exp)["use_traefik"] is False
    del_traefik.assert_awaited_once()
    del_ingress.assert_not_called()


async def test_migrate_noop_when_grand_central_not_deployed():
    with (
        mock.patch(_MIGRATE_PATCHES[0]) as read_deploy,
        mock.patch(_MIGRATE_PATCHES[1]) as create_exp,
        mock.patch(_MIGRATE_PATCHES[2]) as del_ingress,
        mock.patch(_MIGRATE_PATCHES[3]) as del_traefik,
    ):
        read_deploy.return_value = None
        await migrate_grand_central_exposure(
            "ns",
            "c",
            {"cluster": {"name": "c"}},
            {},
            use_traefik=True,
            logger=mock.MagicMock(),
        )

    create_exp.assert_not_called()
    del_ingress.assert_not_called()
    del_traefik.assert_not_called()


async def test_change_gc_exposure_handler_reconciles_even_when_effective_unchanged(
    faker,
):
    handler = ChangeGrandCentralExposureSubHandler(
        faker.uuid4(), faker.domain_word(), faker.md5(), {}
    )
    old = {"spec": {"cluster": {"exposure": "traefik"}}, "metadata": {}}
    body = {
        "spec": {
            "cluster": {"exposure": "traefik"},
            "grandCentral": {"exposure": "traefik"},
        },
        "metadata": {},
    }
    assert grand_central_uses_traefik(old["spec"]) is True
    assert grand_central_uses_traefik(body["spec"]) is True

    with mock.patch(
        "crate.operator.exposure.migrate_grand_central_exposure"
    ) as migrate:
        await handler.handle(
            namespace="ns",
            name="c",
            body=body,
            old=old,
            logger=mock.MagicMock(),
        )

    migrate.assert_awaited_once()
    assert _await_kwargs(migrate)["use_traefik"] is True


class _AsyncCM:
    async def __aenter__(self):
        return mock.MagicMock()

    async def __aexit__(self, *args):
        return False


def _cluster_exposure_body(exposure: str, grand_central: Optional[dict] = None) -> dict:
    spec = {
        "cluster": {
            "name": "c",
            "exposure": exposure,
            "externalDNS": "example.aks1.eastus2.azure.cratedb-dev.net.",
        },
    }
    if grand_central is not None:
        spec["grandCentral"] = grand_central
    return {"spec": spec, "metadata": {"name": "c"}}


async def _run_change_exposure(body: dict, old: dict):
    handler = ChangeExposureSubHandler("ns", "c", "hash", {})
    with (
        mock.patch("crate.operator.exposure.GlobalApiClient", return_value=_AsyncCM()),
        mock.patch("crate.operator.exposure.CoreV1Api"),
        mock.patch("crate.operator.exposure.patch_service_exposure"),
        mock.patch("crate.operator.exposure.create_traefik_resources"),
        mock.patch("crate.operator.exposure.delete_traefik_resources"),
        mock.patch("crate.operator.exposure.get_owner_references", return_value=[]),
        mock.patch("crate.operator.exposure.migrate_grand_central_exposure") as migrate,
    ):
        await handler.handle(
            namespace="ns",
            name="c",
            body=body,
            old=old,
            logger=mock.MagicMock(),
        )
    return migrate


async def test_change_cluster_exposure_converges_gc_when_gc_exposure_unset():
    body = _cluster_exposure_body("traefik")
    old = _cluster_exposure_body("loadbalancer")

    migrate = await _run_change_exposure(body, old)

    migrate.assert_awaited_once()
    assert _await_kwargs(migrate)["use_traefik"] is True


async def test_change_cluster_exposure_skips_gc_when_gc_exposure_explicit():
    body = _cluster_exposure_body("traefik", grand_central={"exposure": "nginx"})
    old = _cluster_exposure_body("loadbalancer", grand_central={"exposure": "nginx"})

    migrate = await _run_change_exposure(body, old)

    migrate.assert_not_awaited()
