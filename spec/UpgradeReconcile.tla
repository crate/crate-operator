-------------------------- MODULE UpgradeReconcile --------------------------
(* Model of crate/operator/reconcile/upgrade.py. Experiment, see README.md.  *)
(*                                                                          *)
(* The controller (Reconcile) only reads obs, a snapshot of the real world  *)
(* that can lag behind. The environment runs requested effects, can drop    *)
(* them (API failure), flaps cluster health and can time out.               *)
EXTENDS Naturals, Sequences

CONSTANTS N, MaxFailures, MaxFlaps

ASSUME N \in Nat \ {0}

Phases == {"PREPARING", "APPLYING", "TERMINATING_POD", "WAITING_HEALTHY",
           "FINALIZING", "DONE", "FAILED"}
Terminal == {"DONE", "FAILED"}
Pods == 1..N
Order == [i \in Pods |-> i]

\* Same as EDGES in upgrade.py.
Edges(ph) ==
    CASE ph = "PREPARING"       -> {"APPLYING", "FAILED"}
      [] ph = "APPLYING"        -> {"TERMINATING_POD", "FINALIZING", "FAILED"}
      [] ph = "TERMINATING_POD" -> {"WAITING_HEALTHY", "FAILED"}
      [] ph = "WAITING_HEALTHY" -> {"TERMINATING_POD", "FINALIZING", "FAILED"}
      [] ph = "FINALIZING"      -> {"DONE", "FAILED"}
      [] OTHER                  -> {}

\* EffectKind. DELETE_POD carries the pod, the others pod 0.
E(kind) == [kind |-> kind, pod |-> 0]
Executable == {"RUN_PRECHECKS", "PATCH_STATEFULSETS", "DELETE_POD", "FINALIZE"}

VARIABLES
    phase, pending, effect,                 \* UpgradeState + last Effect
    prechecks, patched, pod, healthy,       \* the real world
    finalized, timedOut,
    obs,                                    \* ObservedUpgradeState
    failures, flaps,                        \* environment budget
    envFailed, upgradedHealthy, visited     \* history, for the properties

ctrl  == <<phase, pending, effect>>
world == <<prechecks, patched, pod, healthy, finalized, timedOut>>
vars  == <<ctrl, world, obs, failures, flaps, envFailed, upgradedHealthy, visited>>

\* pod[p]: "old" = uid equals the pending uid, "gone" = no pod with that
\* name, "new" = same name with a new uid.
Real == [prechecks |-> prechecks, patched |-> patched, pod |-> pod,
         healthy |-> healthy, finalized |-> finalized, timedOut |-> timedOut]

AllNew == \A p \in Pods : pod[p] = "new"

TypeOK ==
    /\ phase \in Phases
    /\ pending \in Seq(Pods)
    /\ effect.kind \in Executable \cup {"NOTIFY_FAILED", "WAIT", "NONE"}
    /\ prechecks \in {"unknown", "pass", "block"}
    /\ pod \in [Pods -> {"old", "gone", "new"}]
    /\ healthy \in BOOLEAN

Init ==
    /\ phase = "PREPARING"
    /\ pending = <<>>
    /\ effect = E("NONE")
    /\ prechecks = "unknown"
    /\ patched = FALSE
    /\ pod = [p \in Pods |-> "old"]
    /\ healthy = TRUE
    /\ finalized = FALSE
    /\ timedOut = FALSE
    /\ obs = Real
    /\ failures = 0
    /\ flaps = 0
    /\ envFailed = FALSE
    /\ upgradedHealthy = FALSE
    /\ visited = {"PREPARING"}

(* transition(). Returns <<phase, pending, effect>>. Only called for a      *)
(* non-terminal phase.                                                      *)
Transition(o) ==
    IF o.timedOut THEN <<"FAILED", <<>>, E("NOTIFY_FAILED")>>
    ELSE CASE phase = "PREPARING" ->
                CASE o.prechecks = "unknown" -> <<phase, pending, E("RUN_PRECHECKS")>>
                  [] o.prechecks = "block"   -> <<"FAILED", <<>>, E("NOTIFY_FAILED")>>
                  [] OTHER                   -> <<"APPLYING", pending, E("WAIT")>>
           [] phase = "APPLYING" ->
                IF o.patched THEN <<"TERMINATING_POD", Order, E("WAIT")>>
                ELSE <<phase, pending, E("PATCH_STATEFULSETS")>>
           [] phase = "FINALIZING" ->
                IF o.finalized THEN <<"DONE", <<>>, E("NONE")>>
                ELSE <<phase, pending, E("FINALIZE")>>
           [] phase = "TERMINATING_POD" ->
                IF o.pod[Head(pending)] = "old"
                THEN <<phase, pending, [kind |-> "DELETE_POD", pod |-> Head(pending)]>>
                ELSE <<"WAITING_HEALTHY", pending, E("WAIT")>>
           [] phase = "WAITING_HEALTHY" ->
                IF o.pod[Head(pending)] = "new" /\ o.healthy
                THEN IF Len(pending) > 1
                     THEN <<"TERMINATING_POD", Tail(pending), E("WAIT")>>
                     ELSE <<"FINALIZING", <<>>, E("WAIT")>>
                ELSE <<phase, pending, E("WAIT")>>

(* One reconcile. A call that changes nothing is a stutter, so it is left   *)
(* out; otherwise a waiting controller would hide dead-ends from TLC.       *)
Reconcile ==
    /\ phase \notin Terminal
    /\ LET r == Transition(obs) IN
         /\ r /= ctrl
         /\ phase' = r[1]
         /\ pending' = r[2]
         /\ effect' = r[3]
    /\ UNCHANGED <<world, obs, failures, flaps, envFailed>>

\* A fresh read of the cluster. Until then, obs can be any older state.
Observe ==
    /\ obs /= Real
    /\ obs' = Real
    /\ UNCHANGED <<ctrl, world, failures, flaps, envFailed>>

\* Effects are idempotent: running one twice changes nothing more.
RunPrechecks ==
    /\ effect.kind = "RUN_PRECHECKS"
    /\ effect' = E("NONE")
    /\ prechecks' \in IF prechecks = "unknown" THEN {"pass", "block"} ELSE {prechecks}
    /\ envFailed' = (envFailed \/ prechecks' = "block")
    /\ UNCHANGED <<phase, pending, patched, pod, healthy, finalized, timedOut,
                   obs, failures, flaps>>

Patch ==
    /\ effect.kind = "PATCH_STATEFULSETS"
    /\ effect' = E("NONE")
    /\ patched' = TRUE
    /\ UNCHANGED <<phase, pending, prechecks, pod, healthy, finalized, timedOut,
                   obs, failures, flaps, envFailed>>

DeletePod ==
    /\ effect.kind = "DELETE_POD"
    /\ effect' = E("NONE")
    \* healthy stays as it is: the master sees the node leave later (stale GREEN).
    /\ pod' = IF pod[effect.pod] = "old" THEN [pod EXCEPT ![effect.pod] = "gone"] ELSE pod
    /\ UNCHANGED <<phase, pending, prechecks, patched, healthy, finalized, timedOut,
                   obs, failures, flaps, envFailed>>

Finalize ==
    /\ effect.kind = "FINALIZE"
    /\ effect' = E("NONE")
    /\ finalized' = TRUE
    /\ UNCHANGED <<phase, pending, prechecks, patched, pod, healthy, timedOut,
                   obs, failures, flaps, envFailed>>

\* The API call failed. Nothing changed, the controller asks again.
FailEffect ==
    /\ effect.kind \in Executable
    /\ failures < MaxFailures
    /\ effect' = E("NONE")
    /\ failures' = failures + 1
    /\ UNCHANGED <<phase, pending, world, obs, flaps, envFailed>>

\* The StatefulSet controller brings the pod back with the current template.
PodRecreated ==
    /\ \E p \in Pods :
         /\ pod[p] = "gone"
         /\ pod' = [pod EXCEPT ![p] = "new"]
    /\ UNCHANGED <<ctrl, prechecks, patched, healthy, finalized, timedOut,
                   obs, failures, flaps, envFailed>>

\* GREEN needs every node back.
BecomeHealthy ==
    /\ ~healthy
    /\ \A p \in Pods : pod[p] /= "gone"
    /\ healthy' = TRUE
    /\ UNCHANGED <<ctrl, prechecks, patched, pod, finalized, timedOut,
                   obs, failures, flaps, envFailed>>

\* A missing node always turns the cluster non-GREEN, the rest are flaps.
Unhealthy ==
    /\ healthy
    /\ \E p \in Pods : pod[p] = "gone"
    /\ healthy' = FALSE
    /\ UNCHANGED <<ctrl, prechecks, patched, pod, finalized, timedOut,
                   obs, failures, flaps, envFailed>>

Flap ==
    /\ healthy
    /\ flaps < MaxFlaps
    /\ healthy' = FALSE
    /\ flaps' = flaps + 1
    /\ UNCHANGED <<ctrl, prechecks, patched, pod, finalized, timedOut,
                   obs, failures, envFailed>>

TimeOut ==
    /\ phase \notin Terminal
    /\ ~timedOut
    /\ timedOut' = TRUE
    /\ envFailed' = TRUE
    /\ UNCHANGED <<ctrl, prechecks, patched, pod, healthy, finalized,
                   obs, failures, flaps>>

\* Terminal phases stutter, so TLC does not report them as deadlocks.
Stop ==
    /\ phase \in Terminal
    /\ UNCHANGED vars

History ==
    /\ upgradedHealthy' = (upgradedHealthy \/ (healthy' /\ AllNew'))
    /\ visited' = visited \cup {phase'}

Do(A) == A /\ History

Next ==
    \/ Do(Reconcile) \/ Do(Observe)
    \/ Do(RunPrechecks) \/ Do(Patch) \/ Do(DeletePod) \/ Do(Finalize)
    \/ Do(FailEffect) \/ Do(PodRecreated) \/ Do(BecomeHealthy)
    \/ Do(Unhealthy) \/ Do(Flap)
    \/ Do(TimeOut)
    \/ Stop

\* Failures and flaps are bounded by the constants, so they stop at some point.
Fairness ==
    /\ WF_vars(Do(Reconcile))
    /\ WF_vars(Do(Observe))
    /\ WF_vars(Do(RunPrechecks))
    /\ WF_vars(Do(Patch))
    /\ WF_vars(Do(DeletePod))
    /\ WF_vars(Do(Finalize))
    /\ WF_vars(Do(PodRecreated))
    /\ WF_vars(Do(BecomeHealthy))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* 1. Finalization only after all pods run the new version and the cluster *)
(*    was GREEN with all of them.                                          *)
FinalizeAfterHealthy ==
    phase \in {"FINALIZING", "DONE"} => AllNew /\ upgradedHealthy

\* No pod comes back before the StatefulSets carry the new image.
NewPodsUpgraded == \A p \in Pods : pod[p] = "new" => patched

(* 2. DONE only through every upgrade phase, and only along Edges.         *)
CompletePath == phase = "DONE" => visited = Phases \ {"FAILED"}
EdgesRespected == [][phase' \in Edges(phase) \cup {phase}]_phase

(* 3. Terminal phases do not change.                                        *)
TerminalStable == [][(phase \in Terminal) => phase' = phase]_phase

(* 4. No dead-end: TLC's deadlock check, with only terminal phases allowed *)
(*    to stutter. TimeOut is enabled in every active phase, so a phase that *)
(*    waits forever is only caught by DoneUnlessEnvFails below.             *)

(* 5. Liveness.                                                             *)
EventuallyTerminal == <>(phase \in Terminal)
DoneUnlessEnvFails == []~envFailed => <>(phase = "DONE")
=============================================================================
