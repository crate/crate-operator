# CrateDB Operator — Red Hat OpenShift Partner Validation Report

> **Status:** 🚧 Draft / test plan — results to be filled in during execution
> **Validation type:** Red Hat **Partner Validation** (self-attested), _not_ full Operator Certification

---

## 0. How to use this document

This is both the **test plan** and the **validation report**. Red Hat Partner
Validation is _self-attested_: **we** define the test criteria, **we** run the
tests, and Red Hat reviews our product listing and our attestation — Red Hat does
**not** run tests against the operator and there is **no mandated test format**.
This document is that evidence.

Workflow:

1. Fill in the **Report metadata** and **Test environment** tables.
2. Execute each test case in section 3 against every OpenShift version in the
   support matrix.
3. Record the result (`PASS` / `FAIL` / `N/A`) and attach evidence (logs,
   `oc` output, screenshots, CI run URLs) in the evidence slot.
4. Fill in **Operational considerations** and the **Sign-off**.
5. Use the **Red Hat submission checklist** (section 6) to file the validation
   in Red Hat Partner Connect.

Legend: ⬜ not run · ✅ pass · ❌ fail · ➖ N/A

### Evidence layout

Raw command output and screenshots are **evidence**, kept separate from the
report body. Each test case's `Evidence:` slot links into
`openshift-validation/evidence/<ocp-version>/`:

```
openshift-validation/
├── partner-validation-report.md      # this report (source of truth)
├── capture.sh                        # captures read-only verification snapshots
└── evidence/
    └── ocp-4.22/
        ├── 00-environment.txt        # versions + cluster info (from capture.sh)
        ├── tc-01-install.txt
        ├── tc-02-deploy-health.txt
        ├── tc-03-scaling.txt
        ├── tc-08-adminui.png
        └── ...
```

Run `./capture.sh <ocp-version> <cratedb-namespace> <operator-namespace>` after
each test action to snapshot the read-only verification commands; capture the
interactive actions (scaling, pod-delete, drain) by `tee`-ing them into the same
folder as you run them. Link each file from the matching `Evidence:` slot, e.g.
`**Evidence:** [tc-02-deploy-health.txt](evidence/ocp-4.22/tc-02-deploy-health.txt)`.

The Markdown report is the source of truth; render a PDF/HTML copy from it for
the Partner Connect listing and for sharing with customers/sales.

---

## 1. Report metadata

| Field                           | Value                                           |
| ------------------------------- | ----------------------------------------------- |
| Operator version under test     | `2.64.1`                                        |
| `crate-control` sidecar version | `<must match operator version>`                 |
| CrateDB version(s) tested       | `6.4.4`                                         |
| OpenShift version(s) tested     | `4.22.11` (EUS)                                 |
| Installation method             | Helm (`crate-operator-crds` + `crate-operator`) |
| Report author                   | Thomas Achatz                                   |
| Date of execution               | `YYYY-MM-DD`                                    |
| Overall result                  | ⬜                                              |

### Support matrix

The Partner Validation badge is pinned to the versions listed here. Tests are
**not** rerun automatically — we re-run only when we choose to add support for a
new CrateDB or OpenShift version.

| OpenShift version | CrateDB version | Result | Notes                                        |
| ----------------- | --------------- | ------ | -------------------------------------------- |
| 4.22.11 (EUS)     | 6.4.4           | ⬜     | Single committed version for this validation |

> We commit to **one** OpenShift version for this validation: **4.22.11**, an
> even-numbered Extended Update Support (EUS) release — the lifecycle enterprise
> OpenShift customers standardize on. Minimum supported OpenShift is **4.12**
> (per `docs/source/openshift.rst`), so 4.22 is well within range. Additional
> versions can be added later with a re-run. Confirm the exact EUS window/EOL
> against Red Hat's OpenShift lifecycle policy before locking in the badge.

---

## 1b. Validation scope — in / out

Partner Validation is self-attested, so **we choose the scope**. We validate the
OpenShift-specific install path and the core cluster lifecycle on the version(s)
we commit to — **not** the operator's entire feature surface.

**In scope** (the 9 lifecycle scenarios TC-01…TC-09):
operator install, cluster deploy, scaling, pod recovery/rescheduling, persistent
storage, operator upgrade, OpenShift version compatibility, monitoring/
observability, removal & cleanup.

**Out of scope** (operator features not exercised for this validation — they may
work, but we are not attesting them here):

- Backups / snapshots & restore
- SSL/TLS via Let's Encrypt
- Custom cluster/node settings beyond the tested baseline
- Hot/cold storage tiers
- Users & secrets management flows
- Cloud-provider zone awareness (auto-detection is _disabled_ in `openshift`
  mode by design)
- Master + data (non-"all-equal") topologies

> Rationale: keeps the test matrix and the required OpenShift cluster small,
> and keeps the evidence focused on what a platform engineer actually evaluates.
> Additional scenarios or versions can be added later with a re-run.

---

## 1c. How the operator works on OpenShift (adaptations)

OpenShift enforces a stricter security posture than vanilla Kubernetes — the
restricted Security Context Constraint (SCC) and Pod Security Admission (PSA).
The operator detects this and adapts its behavior when
`CRATEDB_OPERATOR_CLOUD_PROVIDER=openshift` is set. These are deliberate design
choices, not workarounds, and each is exercised by a test case below.

| Adaptation                                              | Why OpenShift needs it                                                                                                                                           | How the operator handles it                                                                                                                                                                                                                                                                               | Validated by                                 |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| **`crate-control` sidecar for SQL execution**           | The restricted SCC does not permit `pod_exec`, which the operator normally uses to run SQL inside a pod                                                          | Deploys a lightweight HTTP sidecar (port 5050) with an authenticated `/exec` endpoint; auth token stored in a per-cluster Secret (`crate-control-<name>`). This sidecar is the SQL data-plane for **all** operator→cluster SQL on OpenShift — system-user bootstrap and user/password management included | TC-02 (present & healthy), TC-08             |
| **Per-cluster custom SCC** (`crate-anyuid-<ns>-<name>`) | The CrateDB entrypoint uses `chroot` to set up its runtime, which requires starting as UID 0; restricted SCC forbids this                                        | Creates one SCC per cluster granting `SYS_CHROOT` + `RunAsAny`, dropping `KILL`/`MKNOD`, **not** privileged; bound only to a dedicated per-cluster ServiceAccount (`crate-<name>`) — no cluster-wide grant                                                                                                | TC-02 (`openshift.io/scc` annotation on pod) |
| **Root → drop-privilege startup**                       | CrateDB must briefly run as root for `chroot`                                                                                                                    | Pod security context is `runAsUser: 0, fsGroup: 0`; after `chroot` the process drops to UID 1000 (`crate`). The root phase is limited to the entrypoint                                                                                                                                                   | TC-02                                        |
| **No privileged init container**                        | The restricted SCC forbids the privileged `sysctl` init container used to set `vm.max_map_count`                                                                 | The init container is skipped; kernel tuning is delegated to the cluster admin via Node Tuning Operator / MachineConfig                                                                                                                                                                                   | Environment prerequisite; TC-01/TC-02        |
| **PVC `blockOwnerDeletion` disabled**                   | The StatefulSet controller lacks permission to set finalizers on PVCs in OpenShift                                                                               | Owner references on PVCs are created with `blockOwnerDeletion: false`                                                                                                                                                                                                                                     | TC-05, TC-09                                 |
| **Self-contained lifecycle**                            | On OpenShift the operator avoids the external-fileserver lifecycle hooks and relies on CrateDB's built-in shard replication for availability during pod turnover | Runs without the `postStart`/`preStop` hooks; availability during rolling updates is maintained through shard replicas                                                                                                                                                                                    | TC-03/TC-04                                  |

**Operational note:** on OpenShift the operator relies on CrateDB's shard
replicas (rather than a graceful-decommission hook) to maintain availability
during rolling updates and node maintenance. Deploy tables with
`number_of_replicas >= 1` and perform node maintenance in planned windows — this
is standard practice for stateful workloads on Kubernetes. Validated in TC-03/TC-04.

**Cleanup:** the **namespaced** resources (ServiceAccount `crate-<name>`, sidecar
Secret, Service, StatefulSet) carry owner references on the `CrateDB` CR and are
garbage-collected on delete. The per-cluster SCC is cluster-scoped, so the
operator's delete handler removes it explicitly (`crate-anyuid-<ns>-<name>`).
Validated in TC-09.

> **Authoritative source:** the full, maintained description of these adaptations
> — including the complete SCC spec, kernel-tuning setup, PSA labelling, storage
> and troubleshooting — lives in [`docs/source/openshift.rst`](../docs/source/openshift.rst).
> This section is a summary for reviewers; if the two ever diverge, the docs are
> canonical.

---

## 2. Test environment details

| Field                | Value                                                                             |
| -------------------- | --------------------------------------------------------------------------------- |
| Cluster type         | OpenShift Container Platform on GCP (installer-provisioned)                       |
| Cluster topology     | 3 control-plane + 3 workers, one worker per zone (`us-central1-a` / `-b` / `-c`)  |
| Worker node size     | 4 vCPU / ~16 GiB each                                                             |
| StorageClass used    | `ssd-csi` (`pd.csi.storage.gke.io`, `allowVolumeExpansion: true`) — **zonal RWO** |
| Kernel tuning method | None required — nodes provide `vm.max_map_count=262144` (CrateDB's minimum)       |
| Namespace PSA level  | `pod-security.kubernetes.io/enforce=privileged`                                   |
| Operator image       | `crate/crate-operator:2.64.1`                                                     |
| Sidecar image        | `crate/crate-control:2.64.1` (matches operator version)                           |
| Registry access      | Docker Hub (public)                                                               |

> **Topology note:** workers are spread one-per-zone (mirrors CrateDB Cloud's
> multi-AZ layout) and GCP persistent disks are **zonal** in RWO mode. This is
> intentional and realistic, but it shapes TC-04: a pod's PVC can only re-attach
> in its own zone, and each zone has exactly one worker. See TC-04 for how node
> drain is validated as a _resilience_ scenario rather than a simple reschedule.

> **Sizing note:** at 4 vCPU/node, a CrateDB pod cannot request 4 CPU (node
> allocatable is ~3.5 CPU). The test cluster uses a reduced spec
> (`cpu: 1, memory: 3Gi` per node — see `manifests/02-cratedb.yaml`) so that
> scale-up (TC-03) can fit two pods on one worker. This is a functional
> validation, not a sizing/performance benchmark.

### Environment prerequisites (must be satisfied before test cases)

These are documented in `docs/source/openshift.rst` and are **operator
requirements on OpenShift**, not test steps:

- [ ] OpenShift 4.12+ with cluster-admin available for install
- [ ] Kernel param `vm.max_map_count` is at least `262144` (CrateDB's minimum),
      verified with
      `oc debug node/<node> -- chroot /host sysctl vm.max_map_count`. OpenShift
      nodes provide `262144` by default, so no tuning is required here.
- [ ] `crate-control` sidecar image reachable (Docker Hub or mirrored)
- [ ] Target namespace labelled with `privileged` Pod Security Admission
- [ ] A suitable RWO StorageClass (SSD/NVMe-backed) exists

---

## 3. Test cases

Each test case maps to one acceptance criteria:

- Operator installation
- CrateDB cluster deployment
- Scaling operations
- Pod recovery and rescheduling
- Persistent storage validation
- Operator upgrade validation
- OpenShift version compatibility validation
- Monitoring and observability validation
- Operator removal and cleanup validation

---

### TC-01 — Operator installation

**Objective:** The operator (CRDs + controller) installs cleanly on OpenShift in
`openshift` cloud-provider mode.

**Steps:**

```console
# 1. Install CRDs
$ helm repo add crate-operator https://crate.github.io/crate-operator
$ helm install crate-operator-crds crate-operator/crate-operator-crds

# 2. Install the operator in openshift mode
$ helm install crate-operator crate-operator/crate-operator \
    --set env.CRATEDB_OPERATOR_CLOUD_PROVIDER=openshift \
    --set env.CRATEDB_OPERATOR_CRATE_CONTROL_IMAGE=crate/crate-control:<tag> \
    --namespace crate-operator --create-namespace
```

**Expected result:**

- `cratedbs.cloud.crate.io` CRD is registered (`oc get crd | grep cratedb`).
- Operator Deployment reaches `1/1 Ready`; pod logs show no RBAC/SCC errors.
- Operator ClusterRole includes `securitycontextconstraints` verbs.

**Verification:**

```console
$ oc get crd cratedbs.cloud.crate.io
$ oc get deploy crate-operator -n crate-operator
$ oc logs deploy/crate-operator -n crate-operator | tail -50
```

**Result:** ⬜ **Evidence:** _(logs / `oc get` output)_

---

### TC-02 — CrateDB cluster deployment

**Objective:** A `CrateDB` custom resource produces a healthy cluster with all
OpenShift-specific resources created by the operator.

**Steps:**

```console
$ oc new-project cratedb
$ oc label namespace cratedb \
    pod-security.kubernetes.io/enforce=privileged \
    pod-security.kubernetes.io/warn=privileged \
    pod-security.kubernetes.io/audit=privileged
$ oc apply -f dev-cluster.yaml   # 3 data nodes, storageClass set to the tested SC
```

**Expected result:** the operator automatically creates:

- SCC `crate-anyuid-cratedb-<name>`
- ServiceAccount `crate-<name>`
- sidecar auth Secret `crate-control-<name>` and headless Service
- StatefulSet with the `crate-control` sidecar; pods `runAsUser: 0, fsGroup: 0`
- all pods `Ready`, CrateDB cluster health **GREEN**, correct node count.

**Verification:**

```console
$ oc get pods -n cratedb -l app.kubernetes.io/component=cratedb
$ oc get scc | grep crate-anyuid
$ oc get pod <pod> -n cratedb -o jsonpath='{.metadata.annotations.openshift\.io/scc}'
# Cluster health (via Admin UI Route or SQL):
#   SELECT health FROM sys.cluster;  -> GREEN
```

**Result:** ⬜ **Evidence:** _(pod list, SCC, health query)_

---

### TC-03 — Scaling operations

**Objective:** Data nodes scale up and down safely without data loss.

**Steps:**

1. Load a test table with a known row count and replicas configured.
2. Scale **up** a data node definition (`replicas: 3 → 5`) by editing the CR.
3. Wait for the operator to complete; verify GREEN + new pods joined.
4. Scale **down** (`5 → 3`); verify the operator moves shards before
   removing nodes and the cluster returns to GREEN.

> **Capacity note (this environment):** scale-up to 5 lands two CrateDB pods on
> some workers (3 workers, one per zone). At `cpu: 1, memory: 3Gi` per node the
> two-pods-per-worker case fits within the 4 vCPU / ~16 GiB workers. New pods'
> zonal PVCs are provisioned in whichever zone they schedule into. If a pod is
> `Pending` on scale-up, check node capacity before suspecting the operator.

**Expected result:**

- Scaling follows the documented process (master defs first, scale-ups, then
  scale-downs; see `docs/source/concepts.rst`).
- No lost rows; `sys.cluster` health returns to GREEN within `SCALING_TIMEOUT`.
- Operator status/notifications report `event: scale, status: success`.

**Verification:**

```console
$ oc get sts -n cratedb
# SELECT count(*) FROM <test_table>;   (unchanged before/after)
# SELECT health FROM sys.cluster;      (GREEN)
```

**Result:** ⬜ **Evidence:** _(row counts, health, operator logs)_

> **Note:** on OpenShift, data safety during scale-down is maintained through
> CrateDB's shard relocation and replication. Confirm scale-down preserves data
> and that the cluster returns to GREEN; `sys.shards` can be recorded as evidence.

---

### TC-04 — Pod recovery and rescheduling

**Objective:** The cluster self-heals when a pod is deleted, and stays available
when a whole node (zone) goes down.

This cluster runs **one worker per zone** (`us-central1-a/b/c`) with **zonal RWO**
GCP disks — the same multi-AZ shape as CrateDB Cloud. Recovery is validated in two
complementary scenarios:

**Sub-test A — Pod recovery:**

1. `oc delete pod <data-pod> -n cratedb`.
2. Confirm the StatefulSet recreates the pod, it re-attaches its persistent
   volume, the correct `crate-anyuid` SCC is re-applied, the node rejoins, and the
   cluster returns to GREEN.

_Expected:_ full recovery to GREEN, no data loss.

**Sub-test B — Zone-loss resilience:**

1. Ensure the test table has `number_of_replicas >= 1` so shards are replicated
   across zones.
2. `oc adm cordon <node>` then
   `oc adm drain <node> --delete-emptydir-data --ignore-daemonsets` to simulate a
   zone/node outage.
3. Confirm the cluster **remains available** and continues serving reads and
   writes through the shard replicas in the surviving zones.
4. `oc adm uncordon <node>`; the pod returns to its zone, re-attaches its volume,
   and the cluster returns to **GREEN**.

_Expected:_ continuous availability during the outage; full recovery to GREEN
once the node returns. (With zonal storage, a volume re-attaches in its own zone,
which is the standard behavior for cloud block storage.)

**Verification:**

```console
$ oc get pods -n cratedb -o wide -w        # note node/zone placement
# SELECT health FROM sys.cluster;          # available throughout; GREEN after recovery
# SELECT * FROM sys.shards WHERE primary=false;   # replicas present in other zones
```

**Result:** ⬜ **Evidence:** _(pod placement, health transitions, shard state, recovery timing)_

> **Note:** zone-loss resilience relies on shard replication, so Sub-test B
> requires `number_of_replicas >= 1`. Record `sys.shards` before, during, and
> after to evidence continuous availability.

---

### TC-05 — Persistent storage validation

**Objective:** Data persists across pod restarts; PVCs behave correctly on
OpenShift.

**Steps:**

1. Insert a known dataset. Note PVC names (`oc get pvc -n cratedb`).
2. Delete a pod; confirm the same PVC is re-bound and data is intact.
3. **Volume expansion (if `allowVolumeExpansion: true`):** increase
   `disk.size` in the CR; confirm PVCs expand and pods stay healthy.
4. Confirm the debug/heap-dump volume is created
   (`DEBUG_VOLUME_STORAGE_CLASS`).

**Expected result:** one RWO PVC per pod via `volumeClaimTemplates`; data
survives restart; expansion succeeds if supported.

**Verification:**

```console
$ oc get pvc -n cratedb
# row count intact after pod restart
```

**Result:** ⬜ **Evidence:** _(PVC list, before/after row counts)_

> **OpenShift note:** `blockOwnerDeletion` is disabled on PVC owner references
> in openshift mode — confirm no PVC finalizer errors in operator logs.

---

### TC-06 — Operator upgrade validation

**Objective:** The operator upgrades in place without disrupting running clusters.

**Steps:**

1. With a healthy cluster from TC-02 running, upgrade the operator:
   ```console
   $ helm upgrade crate-operator crate-operator/crate-operator \
       --reuse-values --version <new-chart-version>
   $ helm upgrade crate-operator-crds crate-operator/crate-operator-crds \
       --version <new-chart-version>
   ```
2. Confirm CRD schema changes (if any) apply cleanly and existing CRs remain
   valid.
3. Confirm the running CrateDB cluster is undisturbed (still GREEN, no
   unexpected pod restarts) and remains manageable (e.g. a subsequent scale
   still works).

**Expected result:** operator rolls to the new version; existing clusters stay
healthy; reconciliation resumes normally.

**Verification:**

```console
$ helm list -n crate-operator
$ oc get deploy crate-operator -n crate-operator -o jsonpath='{..image}'
# SELECT health FROM sys.cluster;  (still GREEN)
```

**Result:** ⬜ **Evidence:** _(helm versions, cluster health across upgrade)_

---

### TC-07 — OpenShift version compatibility validation

**Objective:** The full suite (TC-01…TC-06, TC-08, TC-09) passes on the single
OpenShift version we commit to support.

**Steps:** Record the exact tested version (`oc version`) and confirm all core
test cases below pass on it. If additional versions are added later, extend this
table with one row per version and re-run.

**Expected result:** all core test cases pass on the committed version; any
deviations are recorded.

**Result summary:**

| OCP version   | TC-01 | TC-02 | TC-03 | TC-04 | TC-05 | TC-06 | TC-08 | TC-09 |
| ------------- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| 4.22.11 (EUS) | ⬜    | ⬜    | ⬜    | ⬜    | ⬜    | ⬜    | ⬜    | ⬜    |

**Evidence:** _(`oc version` output + per-TC evidence files under `evidence/ocp-4.22/`)_

---

### TC-08 — Monitoring and observability validation

**Objective:** CrateDB metrics are observable on OpenShift.

**Steps:**

1. Confirm the SQL Exporter / metrics endpoint is scrapeable (the operator ships
   with SQL Exporter — verify the version deployed).
2. If using OpenShift user-workload monitoring, create a `ServiceMonitor` and
   confirm targets are `Up` in the OpenShift monitoring stack.
3. Access the CrateDB Admin UI via a passthrough Route and confirm cluster
   status is visible.

**Expected result:** metrics are exposed and scrapeable; Admin UI reachable via
Route; key health/shard metrics visible.

**Verification:**

```console
$ oc create route passthrough cratedb-http --service=crate-<name> --port=4200 -n cratedb
# Prometheus targets Up; metrics endpoint returns data
```

**Result:** ⬜ **Evidence:** _(target status, metrics sample, Admin UI screenshot)_

---

### TC-09 — Operator removal and cleanup validation

**Objective:** Deleting a cluster and uninstalling the operator leaves no
orphaned resources.

**Steps:**

1. Delete the `CrateDB` CR: `oc delete cratedb <name> -n cratedb`.
   - Confirm the **namespaced** resources (StatefulSet, sidecar Service, auth
     Secret, ServiceAccount) are garbage-collected via owner references.
   - Confirm the **cluster-scoped** SCC `crate-anyuid-<ns>-<name>` is removed by
     the operator's delete handler (deleted _explicitly_, not GC'd). Check the
     operator logs for `Deleted SCC ...` and the absence of any "may need to be
     removed manually" warning.
   - Confirm PVC deletion behavior matches expectation (document whether PVCs
     are retained or removed).
2. Uninstall the operator: `helm uninstall crate-operator -n crate-operator`.
3. Optionally remove CRDs: `helm uninstall crate-operator-crds`.

**Expected result:** no orphaned SCC/SA/Secret/Service; namespace is clean;
uninstall does not error.

**Verification:**

```console
$ oc get scc | grep crate-anyuid          # gone
$ oc get sa,secret,svc -n cratedb | grep crate   # gone
$ oc get pvc -n cratedb                    # document retention behavior
```

**Result:** ⬜ **Evidence:** _(before/after resource listings)_

---

## 4. Operational considerations (OpenShift)

The following are the OpenShift-specific operational characteristics of the
deployment. They are consistent with the product documentation
(`docs/source/openshift.rst`) and represent standard configuration for stateful
workloads on OpenShift.

- **Availability during rolling updates and node maintenance** relies on CrateDB
  shard replicas rather than a decommission lifecycle hook. Deploy tables with
  `number_of_replicas >= 1` and schedule node maintenance in planned windows;
  monitor `sys.shards`.
- **Zone-aware node attributes** are set explicitly on OpenShift via
  `.spec.nodes.data.*.settings` (the `openshift` cloud-provider mode manages
  OpenShift-native placement rather than public-cloud zone auto-detection).
- **Sidecar image configuration is validated at deploy time:** the operator
  requires `CRATEDB_OPERATOR_CRATE_CONTROL_IMAGE` and fails fast with a clear
  error if it is unset, so misconfiguration is caught immediately. Keep the
  sidecar image tag aligned with the operator version.
- **Pod placement across zones/nodes** is configured with standard Kubernetes
  scheduling primitives (`topologySpreadConstraints` / node placement) in the
  cluster spec when required. On this cluster the `ssd-csi` StorageClass uses
  `WaitForFirstConsumer`, so pods distribute naturally across the three zones.
- **Kernel parameters:** CrateDB requires `vm.max_map_count >= 262144`. OpenShift
  nodes provide `262144` by default, which satisfies this — no node tuning was
  required. If a cluster's nodes report a lower value, raise it via the cluster
  administrator (Node Tuning Operator / MachineConfig) before deploying clusters.
- **`privileged` PSA required:** CrateDB pods start as UID 0 with `SYS_CHROOT`;
  the namespace must allow privileged PSA (or an equivalent policy exception).

---

## 5. Sign-off / attestation

By signing, CrateDB attests that it has tested the interoperability of the
CrateDB Operator with the listed Red Hat OpenShift version(s) and supports the
product when used with those versions.

|                          |     |
| ------------------------ | --- |
| Tested by                |     |
| Reviewed by (Eng)        |     |
| Approved by (Product/CE) |     |
| Date                     |     |

---

## 6. Red Hat submission checklist

Partner Validation is filed in **Red Hat Partner Connect** (not from this repo).
Ownership is shared — most of the _listing_ is Product/Marketing/Legal, not
Engineering.

**Prerequisite (blocking — resolve first with CE/Product):**

- [ ] Red Hat Partner Connect **corporate account** exists and program terms are
      accepted. _(Who administers it? If none, who has authority to create it?)_

**Portal listing (Product / Marketing / Legal):**

- [ ] Create Product → category (Standalone Application / Operator)
- [ ] Product info: name, logo, description, features
- [ ] ≥ 3 linked resources (docs URLs — e.g. this report, OpenShift docs page)
- [ ] Support details (website mandatory), marketing + technical contacts
- [ ] Legal: license agreement URL, privacy policy URL
- [ ] SEO: category + ≥ 1 search alias

**Validation attestation (Engineering supplies the evidence):**

- [ ] Attest interoperability tested on the selected OpenShift version(s)
- [ ] Attest CrateDB supports the product on those versions
- [ ] Publish/link this Validation Report as supporting documentation

**After submission:** Red Hat's ecosystem team reviews the listing details; on
approval the product is eligible for publication to the Red Hat Ecosystem
Catalog.

> **Scope note:** This is Partner Validation (self-attested), **not** full
> Operator Certification. Certification would additionally require packaging the
> operator as an OLM bundle on a certified registry and passing Red Hat's
> automated checks (preflight / scorecard) — out of scope for this validation.

---

## 7. References

- Red Hat Partner Validation Guide (2026): https://docs.redhat.com/en/documentation/red_hat_partner_certification/2026/html-single/red_hat_partner_validation_guide/index
- CrateDB Operator OpenShift docs: `docs/source/openshift.rst`
- CrateDB Operator concepts (scaling/restart): `docs/source/concepts.rst`
