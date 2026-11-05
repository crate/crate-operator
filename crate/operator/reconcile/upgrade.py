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

"""
Experimental, pure model of the upgrade flow.

Nothing calls this yet. The legacy flow is the chain of kopf sub-handlers
``before_upgrade`` -> ``upgrade`` -> ``restart`` -> ``restore_user_jwt_auth``
-> ``after_upgrade``. The next step is to run ``transition()`` in shadow mode
beside that chain and compare, before it owns any effects. The same machine is
model-checked in ``spec/UpgradeReconcile.tla``.

The state advances only on what is observed, never on an effect having been
issued. A failed or lost effect is therefore requested again on the next call.
"""

import enum
from dataclasses import dataclass, replace
from typing import Dict, FrozenSet, NamedTuple, Optional, Tuple


class Phase(enum.Enum):
    PREPARING = "PREPARING"
    APPLYING = "APPLYING"
    TERMINATING_POD = "TERMINATING_POD"
    WAITING_HEALTHY = "WAITING_HEALTHY"
    FINALIZING = "FINALIZING"
    DONE = "DONE"
    FAILED = "FAILED"


TERMINAL: FrozenSet[Phase] = frozenset({Phase.DONE, Phase.FAILED})

# Every edge transition() can take, apart from staying put. Mirrors ``Edges``
# in the TLA+ spec.
EDGES: Dict[Phase, FrozenSet[Phase]] = {
    Phase.PREPARING: frozenset({Phase.APPLYING, Phase.FAILED}),
    Phase.APPLYING: frozenset({Phase.TERMINATING_POD, Phase.FINALIZING, Phase.FAILED}),
    Phase.TERMINATING_POD: frozenset({Phase.WAITING_HEALTHY, Phase.FAILED}),
    Phase.WAITING_HEALTHY: frozenset(
        {Phase.TERMINATING_POD, Phase.FINALIZING, Phase.FAILED}
    ),
    Phase.FINALIZING: frozenset({Phase.DONE, Phase.FAILED}),
    Phase.DONE: frozenset(),
    Phase.FAILED: frozenset(),
}


class Pod(NamedTuple):
    name: str
    uid: str


@dataclass(frozen=True)
class UpgradeState:
    phase: Phase = Phase.PREPARING
    # Pods still to restart, head first. Same as ``status.pendingPods``.
    pending: Tuple[Pod, ...] = ()


@dataclass(frozen=True)
class DesiredUpgrade:
    image: str


@dataclass(frozen=True)
class ObservedUpgradeState:
    # None until the re-index and unsafe-path checks have run.
    prechecks_passed: Optional[bool] = None
    statefulset_images: Tuple[str, ...] = ()
    # Pods of the cluster in restart order (masters first).
    pods: Tuple[Pod, ...] = ()
    # None when the health could not be read.
    cluster_healthy: Optional[bool] = None
    finalized: bool = False
    timed_out: bool = False


class EffectKind(enum.Enum):
    RUN_PRECHECKS = "RUN_PRECHECKS"
    PATCH_STATEFULSETS = "PATCH_STATEFULSETS"
    DELETE_POD = "DELETE_POD"
    FINALIZE = "FINALIZE"
    NOTIFY_FAILED = "NOTIFY_FAILED"
    WAIT = "WAIT"
    NONE = "NONE"


@dataclass(frozen=True)
class Effect:
    kind: EffectKind
    pod: Optional[str] = None
    reason: str = ""


def _wait(reason: str) -> Effect:
    return Effect(EffectKind.WAIT, reason=reason)


def transition(
    state: UpgradeState, desired: DesiredUpgrade, observed: ObservedUpgradeState
) -> Tuple[UpgradeState, Effect]:
    """
    Compute one reconcile step. Moves at most one phase per call.

    Effects are requests to the caller and must be idempotent: the same
    effect is returned again until the observation shows it took place.
    """
    phase = state.phase
    if phase in TERMINAL:
        return state, Effect(EffectKind.NONE)

    if observed.timed_out:
        return UpgradeState(Phase.FAILED), Effect(
            EffectKind.NOTIFY_FAILED, reason=f"timed out in {phase.value}"
        )

    if phase is Phase.PREPARING:
        if observed.prechecks_passed is None:
            return state, Effect(EffectKind.RUN_PRECHECKS)
        if not observed.prechecks_passed:
            return UpgradeState(Phase.FAILED), Effect(
                EffectKind.NOTIFY_FAILED, reason="prechecks blocked the upgrade"
            )
        return replace(state, phase=Phase.APPLYING), _wait("prechecks passed")

    if phase is Phase.APPLYING:
        images = observed.statefulset_images
        if not images:
            return state, _wait("no statefulsets observed")
        if any(image != desired.image for image in images):
            return state, Effect(EffectKind.PATCH_STATEFULSETS)
        # Snapshot once, like restart_cluster() does into status.pendingPods.
        if not observed.pods:
            return UpgradeState(Phase.FINALIZING), _wait("no pods to restart")
        return UpgradeState(Phase.TERMINATING_POD, observed.pods), _wait(
            "statefulsets patched"
        )

    if phase is Phase.FINALIZING:
        # Restore JWT auth, re-create internal tables, send the success webhook.
        if not observed.finalized:
            return state, Effect(EffectKind.FINALIZE)
        return UpgradeState(Phase.DONE), Effect(EffectKind.NONE)

    # TERMINATING_POD and WAITING_HEALTHY: one pod at a time, head first.
    head = state.pending[0]
    uid = {pod.name: pod.uid for pod in observed.pods}.get(head.name)

    if phase is Phase.TERMINATING_POD:
        if uid == head.uid:
            # Repeated while the pod terminates. Legacy does the same.
            return state, Effect(EffectKind.DELETE_POD, pod=head.name)
        return replace(state, phase=Phase.WAITING_HEALTHY), _wait(
            f"pod {head.name} terminated"
        )

    # WAITING_HEALTHY
    if uid is None:
        return state, _wait(f"pod {head.name} not back yet")
    if uid == head.uid:
        # Can only be a stale read, the old pod was seen gone before.
        return state, _wait(f"old pod {head.name} observed again")
    if not observed.cluster_healthy:
        return state, _wait("cluster not healthy")
    rest = state.pending[1:]
    if rest:
        return UpgradeState(Phase.TERMINATING_POD, rest), _wait(
            f"pod {head.name} restarted"
        )
    return UpgradeState(Phase.FINALIZING), _wait("all pods restarted")
