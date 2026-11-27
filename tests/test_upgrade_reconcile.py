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

from dataclasses import replace
from itertools import product

import pytest

from crate.operator.reconcile.upgrade import (
    EDGES,
    TERMINAL,
    DesiredUpgrade,
    Effect,
    EffectKind,
    ObservedUpgradeState,
    Phase,
    Pod,
    UpgradeState,
    transition,
)

OLD_IMAGE = "crate:5.10.1"
NEW_IMAGE = "crate:6.0.6"
DESIRED = DesiredUpgrade(image=NEW_IMAGE)

POD_0 = Pod("crate-data-hot-my-cluster-0", "uid-0")
POD_1 = Pod("crate-data-hot-my-cluster-1", "uid-1")
NEW_POD_0 = Pod(POD_0.name, "uid-0-new")
NEW_POD_1 = Pod(POD_1.name, "uid-1-new")

PATCHED = ObservedUpgradeState(
    prechecks_passed=True,
    statefulset_images=(NEW_IMAGE,),
    pods=(POD_0, POD_1),
    cluster_healthy=True,
)


def run(state, *observations):
    effects = []
    for observed in observations:
        state, effect = transition(state, DESIRED, observed)
        effects.append(effect.kind)
    return state, effects


def test_successful_upgrade():
    observations = [
        ObservedUpgradeState(statefulset_images=(OLD_IMAGE,), pods=(POD_0, POD_1)),
        replace(PATCHED, statefulset_images=(OLD_IMAGE,)),
        replace(PATCHED, statefulset_images=(OLD_IMAGE,)),
        PATCHED,
        PATCHED,
        replace(PATCHED, pods=(POD_1,), cluster_healthy=False),
        replace(PATCHED, pods=(NEW_POD_0, POD_1), cluster_healthy=True),
        replace(PATCHED, pods=(NEW_POD_0, POD_1)),
        replace(PATCHED, pods=(NEW_POD_0,), cluster_healthy=False),
        replace(PATCHED, pods=(NEW_POD_0, NEW_POD_1)),
        replace(PATCHED, pods=(NEW_POD_0, NEW_POD_1)),
        replace(PATCHED, pods=(NEW_POD_0, NEW_POD_1), finalized=True),
    ]
    state, effects = run(UpgradeState(), *observations)

    assert state == UpgradeState(Phase.DONE)
    assert effects == [
        EffectKind.RUN_PRECHECKS,
        EffectKind.WAIT,  # -> APPLYING
        EffectKind.PATCH_STATEFULSETS,
        EffectKind.WAIT,  # -> TERMINATING_POD
        EffectKind.DELETE_POD,
        EffectKind.WAIT,  # -> WAITING_HEALTHY
        EffectKind.WAIT,  # -> TERMINATING_POD for pod 1
        EffectKind.DELETE_POD,
        EffectKind.WAIT,  # -> WAITING_HEALTHY
        EffectKind.WAIT,  # -> FINALIZING
        EffectKind.FINALIZE,
        EffectKind.NONE,  # -> DONE
    ]


def test_no_pods_goes_straight_to_finalizing():
    state, effect = transition(
        UpgradeState(Phase.APPLYING), DESIRED, replace(PATCHED, pods=())
    )
    assert state == UpgradeState(Phase.FINALIZING)


# Each row: state, observation, expected (state, effect kind). All rows are
# "retry" situations: the effect is requested again and the phase stays.
@pytest.mark.parametrize(
    "state,observed,effect",
    [
        (UpgradeState(), ObservedUpgradeState(), EffectKind.RUN_PRECHECKS),
        (
            UpgradeState(Phase.APPLYING),
            replace(PATCHED, statefulset_images=(NEW_IMAGE, OLD_IMAGE)),
            EffectKind.PATCH_STATEFULSETS,
        ),
        (
            UpgradeState(Phase.TERMINATING_POD, (POD_0, POD_1)),
            PATCHED,
            EffectKind.DELETE_POD,
        ),
        (
            UpgradeState(Phase.FINALIZING),
            replace(PATCHED, pods=(NEW_POD_0, NEW_POD_1)),
            EffectKind.FINALIZE,
        ),
    ],
)
def test_failed_effect_is_requested_again(state, observed, effect):
    # The effect failed, so the next observation is the same. Nothing advances.
    for _ in range(3):
        next_state, next_effect = transition(state, DESIRED, observed)
        assert next_state == state
        assert next_effect.kind is effect


@pytest.mark.parametrize(
    "observed,reason",
    [
        (replace(PATCHED, pods=(POD_1,)), "pod not back"),
        (replace(PATCHED, pods=(NEW_POD_0, POD_1), cluster_healthy=False), "red"),
        (replace(PATCHED, pods=(NEW_POD_0, POD_1), cluster_healthy=None), "unknown"),
        # GREEN from before the node left, the new pod is not there yet
        (replace(PATCHED, pods=(POD_1,), cluster_healthy=True), "stale green"),
        # old uid seen again after it was gone: stale cache read
        (PATCHED, "old pod again"),
    ],
)
def test_waiting_healthy_does_not_advance(observed, reason):
    state = UpgradeState(Phase.WAITING_HEALTHY, (POD_0, POD_1))
    new_state, effect = transition(state, DESIRED, observed)
    assert new_state == state
    assert effect.kind is EffectKind.WAIT


def test_temporary_unhealthy_cluster_keeps_waiting():
    state = UpgradeState(Phase.WAITING_HEALTHY, (POD_1,))
    flapping = [False, None, False, True]
    for healthy in flapping:
        observed = replace(
            PATCHED, pods=(NEW_POD_0, NEW_POD_1), cluster_healthy=healthy
        )
        state, _ = transition(state, DESIRED, observed)
        if not healthy:
            assert state.phase is Phase.WAITING_HEALTHY
    assert state == UpgradeState(Phase.FINALIZING)


def test_later_observations_do_not_skip_phases():
    # Everything looks done already, but prechecks have not been observed.
    observed = ObservedUpgradeState(
        statefulset_images=(NEW_IMAGE,),
        pods=(NEW_POD_0, NEW_POD_1),
        cluster_healthy=True,
        finalized=True,
    )
    state, effect = transition(UpgradeState(), DESIRED, observed)
    assert state == UpgradeState()
    assert effect.kind is EffectKind.RUN_PRECHECKS


def test_no_statefulsets_observed_waits():
    state = UpgradeState(Phase.APPLYING)
    observed = replace(PATCHED, statefulset_images=())
    assert transition(state, DESIRED, observed)[0] == state


def test_finalizing_only_after_last_pod_healthy():
    state = UpgradeState(Phase.WAITING_HEALTHY, (POD_1,))
    unhealthy = replace(PATCHED, pods=(NEW_POD_0, NEW_POD_1), cluster_healthy=False)
    assert transition(state, DESIRED, unhealthy)[0] == state
    # healthy, but pod 1 is still the old one
    old = replace(PATCHED, pods=(NEW_POD_0, POD_1))
    assert transition(state, DESIRED, old)[0] == state
    healthy = replace(unhealthy, cluster_healthy=True)
    assert transition(state, DESIRED, healthy)[0] == UpgradeState(Phase.FINALIZING)


def test_prechecks_blocked_fails():
    observed = ObservedUpgradeState(prechecks_passed=False)
    state, effect = transition(UpgradeState(), DESIRED, observed)
    assert state == UpgradeState(Phase.FAILED)
    assert effect.kind is EffectKind.NOTIFY_FAILED


@pytest.mark.parametrize("phase", sorted(set(Phase) - TERMINAL, key=str))
def test_timeout_fails_from_any_active_phase(phase):
    state = UpgradeState(phase, (POD_0,))
    observed = replace(PATCHED, timed_out=True)
    new_state, effect = transition(state, DESIRED, observed)
    assert new_state == UpgradeState(Phase.FAILED)
    assert effect.kind is EffectKind.NOTIFY_FAILED


OBSERVATIONS = [
    ObservedUpgradeState(
        prechecks_passed=prechecks,
        statefulset_images=images,
        pods=pods,
        cluster_healthy=healthy,
        finalized=finalized,
        timed_out=timed_out,
    )
    for prechecks, images, pods, healthy, finalized, timed_out in product(
        [None, True, False],
        [(), (OLD_IMAGE,), (NEW_IMAGE,)],
        [(), (POD_0, POD_1), (POD_1,), (NEW_POD_0, POD_1), (NEW_POD_0, NEW_POD_1)],
        [None, False, True],
        [False, True],
        [False, True],
    )
]
STATES = [
    UpgradeState(Phase.PREPARING),
    UpgradeState(Phase.APPLYING),
    UpgradeState(Phase.TERMINATING_POD, (POD_0, POD_1)),
    UpgradeState(Phase.TERMINATING_POD, (POD_1,)),
    UpgradeState(Phase.WAITING_HEALTHY, (POD_0, POD_1)),
    UpgradeState(Phase.WAITING_HEALTHY, (POD_1,)),
    UpgradeState(Phase.FINALIZING),
    UpgradeState(Phase.DONE),
    UpgradeState(Phase.FAILED),
]


@pytest.mark.parametrize("state", STATES, ids=lambda s: s.phase.value)
def test_only_declared_edges(state):
    for observed in OBSERVATIONS:
        new_state, _ = transition(state, DESIRED, observed)
        assert new_state.phase in EDGES[state.phase] | {state.phase}
        # deterministic
        assert transition(state, DESIRED, observed)[0] == new_state


@pytest.mark.parametrize("phase", sorted(TERMINAL, key=str))
def test_terminal_is_stable(phase):
    state = UpgradeState(phase)
    for observed in OBSERVATIONS:
        assert transition(state, DESIRED, observed) == (state, Effect(EffectKind.NONE))
