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

import datetime
from typing import Any, Dict, List, Tuple
from unittest import mock

import kopf
import pytest

from crate.operator.constants import CLUSTER_UPDATE_ID
from crate.operator.handlers.handle_update_cratedb import update_cratedb
from crate.operator.operations import DELAY_CRONJOB


def _group(name: str, replicas: int, cpu: int = 2) -> Dict[str, Any]:
    return {
        "name": name,
        "replicas": replicas,
        "resources": {
            "limits": {"cpu": cpu, "memory": "4Gi"},
            "disk": {"size": "16GiB", "count": 1},
        },
    }


def _data_diff(old_groups, new_groups) -> kopf.Diff:
    """
    A diff over ``spec.nodes.data``. kopf does not recurse into lists, so the
    whole list arrives as one item -- which is what makes iterating the groups
    correctly this handler's job.
    """
    return kopf.Diff(
        [
            kopf.DiffItem(
                kopf.DiffOperation.CHANGE,
                ("spec", "nodes", "data"),
                old_groups,
                new_groups,
            )
        ]
    )


async def _run(diff: kopf.Diff, new_groups) -> Tuple[List[str], kopf.Patch]:
    """
    Run ``update_cratedb`` with ``kopf.register`` stubbed out and return the ids
    of the sub-handlers it registered, plus the patch it produced.
    """
    registered: List[str] = []
    patch = kopf.Patch()
    body = kopf.Body(
        {
            "metadata": {"annotations": {}},
            "spec": {"cluster": {"version": "5.9.0"}, "nodes": {"data": new_groups}},
        }
    )

    with mock.patch(
        "crate.operator.handlers.handle_update_cratedb.kopf.register",
        side_effect=lambda fn, id, **kwargs: registered.append(id),
    ):
        await update_cratedb(
            namespace="ns",
            name="c",
            patch=patch,
            body=body,
            status={},
            diff=diff,
            started=datetime.datetime.now(datetime.timezone.utc),
            logger=mock.Mock(),
        )
    return registered, patch


class TestSeveralDataGroups:
    """
    ``spec.nodes.data`` is a list, and the dispatch used to rebind its loop
    variable to the first element -- so a cluster with two or more data groups
    raised ``KeyError`` before any sub-handler was registered, and no update of
    any kind could complete (crate/cloud#3038).
    """

    @pytest.mark.asyncio
    async def test_scaling_one_of_two_groups_is_dispatched(self):
        old = [_group("hot", 3), _group("cold", 2)]
        new = [_group("hot", 3), _group("cold", 4)]

        registered, _ = await _run(_data_diff(old, new), new)

        assert "scale" in registered

    @pytest.mark.asyncio
    async def test_compute_change_on_the_second_group_is_detected(self):
        # The old dispatch never looked past the first group, so a change that
        # only touches a later group went unnoticed.
        old = [_group("hot", 3), _group("cold", 2, cpu=2)]
        new = [_group("hot", 3), _group("cold", 2, cpu=8)]

        registered, _ = await _run(_data_diff(old, new), new)

        assert "change_compute" in registered
        assert "restart" in registered

    @pytest.mark.asyncio
    async def test_disk_change_on_the_second_group_is_detected(self):
        old = [_group("hot", 3), _group("cold", 2)]
        new = [_group("hot", 3), _group("cold", 2)]
        new[1]["resources"]["disk"]["size"] = "32GiB"

        registered, _ = await _run(_data_diff(old, new), new)

        assert "expand_volume" in registered

    @pytest.mark.asyncio
    async def test_no_change_registers_nothing(self):
        groups = [_group("hot", 3), _group("cold", 2)]

        registered, _ = await _run(_data_diff(groups, groups), groups)

        assert registered == []


class TestSuspendResumeIsClusterWide:
    """
    Suspend and resume take *every* data group across zero. The scale handler
    decides the same way (``scale.classify_data_scaling``); if this dispatch
    disagreed it would skip ``after_cluster_update`` on what is then carried out
    as an ordinary scale, leaving ``cluster.routing.allocation.enable`` pinned to
    ``new_primaries``.
    """

    @pytest.mark.asyncio
    async def test_all_groups_to_zero_is_a_suspend(self):
        old = [_group("hot", 3), _group("cold", 2)]
        new = [_group("hot", 0), _group("cold", 0)]

        registered, patch = await _run(_data_diff(old, new), new)

        assert "scale" in registered
        # A suspend skips after_cluster_update and keeps the cronjobs disabled.
        assert "after_cluster_update" not in registered
        assert patch.status[DELAY_CRONJOB] is False

    @pytest.mark.asyncio
    async def test_all_groups_from_zero_is_a_resume(self):
        old = [_group("hot", 0), _group("cold", 0)]
        new = [_group("hot", 3), _group("cold", 2)]

        registered, patch = await _run(_data_diff(old, new), new)

        assert "scale" in registered
        # A resume skips before_cluster_update and delays the cronjobs.
        assert "before_cluster_update" not in registered
        assert patch.status[DELAY_CRONJOB] is True

    @pytest.mark.asyncio
    async def test_one_group_to_zero_is_an_ordinary_scale(self):
        # ``cold`` going to zero while ``hot`` keeps serving is not a suspend, so
        # after_cluster_update must still run and reset the allocation setting.
        old = [_group("hot", 3), _group("cold", 2)]
        new = [_group("hot", 3), _group("cold", 0)]

        registered, patch = await _run(_data_diff(old, new), new)

        assert "scale" in registered
        assert "before_cluster_update" in registered
        assert "after_cluster_update" in registered
        assert DELAY_CRONJOB not in patch.status

    @pytest.mark.asyncio
    async def test_single_group_cluster_still_suspends(self):
        # The shape every cluster in use has today: one group, so "all groups"
        # and "this group" are the same thing.
        registered, patch = await _run(
            _data_diff([_group("hot", 3)], [_group("hot", 0)]), [_group("hot", 0)]
        )

        assert "scale" in registered
        assert "after_cluster_update" not in registered
        assert patch.status[DELAY_CRONJOB] is False

    @pytest.mark.asyncio
    async def test_master_to_zero_is_rejected_unless_every_group_suspends(self):
        # The dangerous case: `cold` alone goes to zero, so this is an ordinary
        # scale. scale_cluster would honour the master change literally and take
        # every master down while `hot` is still serving, so the dispatch has to
        # reject it rather than wave it through.
        old = [_group("hot", 3), _group("cold", 2)]
        new = [_group("hot", 3), _group("cold", 0)]
        diff = kopf.Diff(
            [
                kopf.DiffItem(
                    kopf.DiffOperation.CHANGE,
                    ("spec", "nodes", "master", "replicas"),
                    3,
                    0,
                ),
                _data_diff(old, new)[0],
            ]
        )

        with pytest.raises(kopf.PermanentError, match="full cluster"):
            await _run(diff, new)

    @pytest.mark.asyncio
    async def test_master_to_zero_is_allowed_when_the_whole_cluster_suspends(self):
        old = [_group("hot", 3), _group("cold", 2)]
        new = [_group("hot", 0), _group("cold", 0)]
        diff = kopf.Diff(
            [
                kopf.DiffItem(
                    kopf.DiffOperation.CHANGE,
                    ("spec", "nodes", "master", "replicas"),
                    3,
                    0,
                ),
                _data_diff(old, new)[0],
            ]
        )

        registered, _ = await _run(diff, new)

        assert "scale" in registered

    @pytest.mark.asyncio
    async def test_adding_a_data_group_is_rejected_rather_than_ignored(self):
        # Without this the update is a silent no-op: nothing is registered, the
        # handler returns early, and the CRD and the cluster diverge quietly.
        old = [_group("hot", 3)]
        new = [_group("hot", 3), _group("cold", 2)]

        with pytest.raises(kopf.PermanentError, match="Adding and removing"):
            await _run(_data_diff(old, new), new)

    @pytest.mark.asyncio
    async def test_removing_a_data_group_is_rejected(self):
        old = [_group("hot", 3), _group("cold", 2)]
        new = [_group("hot", 3)]

        with pytest.raises(kopf.PermanentError, match="Adding and removing"):
            await _run(_data_diff(old, new), new)

    @pytest.mark.asyncio
    async def test_update_context_is_stored(self):
        old = [_group("hot", 3), _group("cold", 2)]
        new = [_group("hot", 3), _group("cold", 4)]

        _, patch = await _run(_data_diff(old, new), new)

        assert "ref" in patch.status[CLUSTER_UPDATE_ID]
