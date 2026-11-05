# Upgrade reconcile spec

TLA+ model of `crate/operator/reconcile/upgrade.py`. Experiment: nothing in the
operator uses either yet. The plan is to call `transition()` in shadow mode
beside the legacy kopf upgrade handlers, compare, and only then let it own
effects. Delete `crate/operator/reconcile/`, `tests/test_upgrade_reconcile.py`
and this directory to drop it.

## Running TLC

TLC is Java. Run it in Docker, nothing to install:

```sh
curl -sSL -o spec/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
docker run --rm -v "$PWD/spec":/spec -w /spec eclipse-temurin:21-jre \
  java -cp tla2tools.jar tlc2.TLC -workers auto -metadir /tmp/tlc UpgradeReconcile
```

With `N = 2` pods it finds ~3.3k distinct states and finishes in a second or two.

## Python to TLA+

| `upgrade.py`                        | `UpgradeReconcile.tla`                          |
|-------------------------------------|-------------------------------------------------|
| `Phase.*`                           | `Phases` (same strings)                         |
| `EDGES`                             | `Edges(ph)`, checked by `EdgesRespected`        |
| `UpgradeState.pending`              | `pending`, a sequence of pod indices            |
| `Effect` / `EffectKind`             | `effect`, `[kind, pod]`                         |
| `ObservedUpgradeState`              | `obs`, a possibly stale copy of `Real`          |
| `transition()`                      | `Transition(o)`, run by `Reconcile`             |
| pod uid equals `pending` uid        | `pod[p] = "old"`                                |
| no pod with that name               | `pod[p] = "gone"`                               |
| same name, new uid                  | `pod[p] = "new"`                                |
| `statefulset_images` all desired    | `patched`                                       |
| `timed_out`                         | `timedOut`                                      |

The environment actions (`RunPrechecks`, `Patch`, `DeletePod`, `Finalize`,
`FailEffect`, `PodRecreated`, `BecomeHealthy`, `Unhealthy`, `Flap`, `TimeOut`,
`Observe`) stand in for Kubernetes, CrateDB and the kopf retry loop.

## Checked

- `FinalizeAfterHealthy`: FINALIZING/DONE only once every pod is new and the
  cluster was GREEN with all of them.
- `NewPodsUpgraded`: no pod comes back before the StatefulSets are patched.
- `CompletePath`, `EdgesRespected`: DONE only through every phase, along `Edges`.
- `TerminalStable`: DONE and FAILED do not change.
- No dead-end: deadlock check plus `DoneUnlessEnvFails`.
- `EventuallyTerminal`, `DoneUnlessEnvFails`: with bounded failures and flaps,
  the run ends, and ends in DONE unless prechecks blocked or it timed out.

Each property was checked against a broken copy of the spec (skip the health
check, delete before patching, skip APPLYING, leave DONE, drop the fairness on
health) and TLC reported the expected violation.

Not modelled: a GREEN read right after a node rejoins, before the master has
recomputed health (`is_cluster_healthy()` polls several times against that),
rollback, and the non-upgrade handlers
(`before_cluster_update`, cronjob suspend).
