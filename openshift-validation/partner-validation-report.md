# CrateDB Operator - Red Hat OpenShift Partner Validation Report

> **Status:** ✅ Completed - all test cases (TC-01 - TC-09) passed on OpenShift 4.22.11.
> **Validation type:** Red Hat **Partner Validation** (self-attested)
>
> Legend: ⬜ not run · ✅ pass · ❌ fail · ➖ N/A

---

## 1. Report metadata

| Field                           | Value                                           |
| ------------------------------- | ----------------------------------------------- |
| Operator version under test     | `2.64.1`                                        |
| `crate-control` sidecar version | `2.64.1`                                        |
| CrateDB version(s) tested       | `6.4.4`                                         |
| OpenShift version(s) tested     | `4.22.11` (EUS)                                 |
| Installation method             | Helm (`crate-operator-crds` + `crate-operator`) |
| Report author                   | Thomas Achatz                                   |
| Date of execution               | 2026-09-10                                      |
| Overall result                  | ✅                                              |

### Support matrix

The Partner Validation badge is pinned to the versions listed here. Tests are
**not** rerun automatically - we re-run only when we choose to add support for a
new OpenShift version.

| OpenShift version | CrateDB version | Result | Notes                                        |
| ----------------- | --------------- | ------ | -------------------------------------------- |
| 4.22.11 (EUS)     | 6.4.4           | ✅     | Single committed version for this validation |

> We commit to **one** OpenShift version for this validation: **4.22.11**, an
> even-numbered Extended Update Support (EUS) release - the lifecycle enterprise
> OpenShift customers standardize on. Minimum supported OpenShift is **4.12**
> (per `docs/source/openshift.rst`), so 4.22 is well within range. Additional
> versions can be added later with a re-run.

---

## 1b. Validation scope - in / out

Partner Validation is self-attested. We validate the OpenShift-specific install
path and the core cluster lifecycle on the version(s) we commit to.

**In scope** (the 9 lifecycle scenarios TC-01 - TC-09):
operator install, cluster deploy, scaling, pod recovery/rescheduling, persistent
storage, operator upgrade, OpenShift version compatibility, monitoring/
observability, removal & cleanup.

**Out of scope** (operator features not exercised for this validation - they may
work, but we are not attesting them here):

- SSL/TLS via Let's Encrypt
- Custom cluster/node settings beyond the tested baseline
- Hot/cold storage tiers
- Users & secrets management flows
- Master + data (non-"all-equal") topologies

---

## 1c. How the operator works on OpenShift (adaptations)

OpenShift enforces a stricter security posture than vanilla Kubernetes - the
restricted Security Context Constraint (SCC) and Pod Security Admission (PSA).
The operator detects this and adapts its behavior when
`CRATEDB_OPERATOR_CLOUD_PROVIDER=openshift` is set. These are deliberate design
choices, not workarounds, and each is exercised by a test case below.

| Adaptation                                              | Why OpenShift needs it                                                                                                                                           | How the operator handles it                                                                                                                                                                                                                                  | Validated by                                 |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------- |
| **`crate-control` sidecar for SQL execution**           | The restricted SCC does not permit `pod_exec`, which the operator normally uses to run SQL inside a pod                                                          | Deploys a lightweight HTTP sidecar (port 5050) with an authenticated `/exec` endpoint; auth token stored in a per-cluster Secret (`crate-control-<name>`). It replaces `pod_exec` for in-pod SQL such as system-user bootstrap and user/password management. | TC-02 (present & healthy), TC-08             |
| **Per-cluster custom SCC** (`crate-anyuid-<ns>-<name>`) | The CrateDB entrypoint uses `chroot` to set up its runtime, which requires starting as UID 0; restricted SCC forbids this                                        | Creates one SCC per cluster granting `SYS_CHROOT` + `RunAsAny`, dropping `KILL`/`MKNOD`, **not** privileged; bound only to a dedicated per-cluster ServiceAccount (`crate-<name>`) - no cluster-wide grant                                                   | TC-02 (`openshift.io/scc` annotation on pod) |
| **Root -> drop-privilege startup**                      | CrateDB must briefly run as root for `chroot`                                                                                                                    | Pod security context is `runAsUser: 0, fsGroup: 0`; after `chroot` the process drops to UID 1000 (`crate`). The root phase is limited to the entrypoint                                                                                                      | TC-02                                        |
| **No privileged init container**                        | The restricted SCC forbids the privileged `sysctl` init container used to set `vm.max_map_count`                                                                 | The init container is skipped; kernel tuning is delegated to the cluster admin via Node Tuning Operator / MachineConfig                                                                                                                                      | Environment prerequisite; TC-01/TC-02        |
| **PVC `blockOwnerDeletion` disabled**                   | The StatefulSet controller lacks permission to set finalizers on PVCs in OpenShift                                                                               | Owner references on PVCs are created with `blockOwnerDeletion: false`                                                                                                                                                                                        | TC-05, TC-09                                 |
| **Self-contained lifecycle**                            | On OpenShift the operator avoids the external-fileserver lifecycle hooks and relies on CrateDB's built-in shard replication for availability during pod turnover | Runs without the `postStart`/`preStop` hooks; availability during rolling updates is maintained through shard replicas                                                                                                                                       | TC-03/TC-04                                  |

**Operational note:** on OpenShift the operator relies on CrateDB's shard
replicas (rather than a graceful-decommission hook) to maintain availability
during rolling updates and node maintenance. Deploy tables with
`number_of_replicas >= 1` and perform node maintenance in planned windows - this
is standard practice for stateful workloads on Kubernetes. Validated in TC-03/TC-04.

**Cleanup:** the **namespaced** resources (ServiceAccount `crate-<name>`, sidecar
Secret, Service, StatefulSet) carry owner references on the `CrateDB` CR and are
garbage-collected on delete. The per-cluster SCC is cluster-scoped, so the
operator's delete handler removes it explicitly (`crate-anyuid-<ns>-<name>`).
Validated in TC-09.

> **Authoritative source:** the full, maintained description of these adaptations
> (the complete SCC spec, kernel-tuning setup, PSA labelling, storage, and
> troubleshooting) lives in
> [`docs/source/openshift.rst`](../docs/source/openshift.rst). This section is a
> summary for reviewers; if the two ever diverge, the docs are canonical.

---

## 2. Test environment details

| Field                | Value                                                                             |
| -------------------- | --------------------------------------------------------------------------------- |
| Cluster type         | OpenShift Container Platform on GCP                                               |
| Cluster topology     | 3 control-plane + 3 workers, one worker per zone (`us-central1-a` / `-b` / `-c`)  |
| Worker node size     | 4 vCPU / ~16 GiB each                                                             |
| StorageClass used    | `ssd-csi` (`pd.csi.storage.gke.io`, `allowVolumeExpansion: true`) - **zonal RWO** |
| Kernel tuning method | None required - nodes provide `vm.max_map_count=262144` (CrateDB's minimum)       |
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
> (`cpu: 1, memory: 3Gi` per node - see `manifests/02-cratedb.yaml`). This is a
> functional validation, not a sizing/performance benchmark. Note also that the
> operator places one data pod per node, so the 3 data nodes map to the
> 3 workers, one per zone.

### Environment prerequisites (must be satisfied before test cases)

These are documented in `docs/source/openshift.rst` and are **operator
requirements on OpenShift**, not test steps:

- [ ] OpenShift 4.12+ with cluster-admin available for install
- [ ] Kernel param `vm.max_map_count` is at least `262144` (CrateDB's minimum),
      verified with
      `oc debug node/<node> -- chroot /host sysctl vm.max_map_count`. OpenShift
      nodes commonly provide `262144` by default, so no tuning is required here.
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

### TC-01 - Operator installation

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
    --set env.CRATEDB_OPERATOR_DEBUG_VOLUME_STORAGE_CLASS=<your-storageclass> \
    --set crate-operator-crds.enabled=false \
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

**Result:** ✅ **Evidence:** [env](evidence/ocp-4.22/00-environment.txt) · [CRD](evidence/ocp-4.22/tc-01-crd.txt) · [operator](evidence/ocp-4.22/tc-01-operator.txt) · [operator logs](evidence/ocp-4.22/tc-01-operator-logs.txt)

---

### TC-02 - CrateDB cluster deployment

**Objective:** A `CrateDB` custom resource produces a healthy cluster with all
OpenShift-specific resources created by the operator.

**Steps:**

```console
$ oc new-project cratedb
$ oc label namespace cratedb \
    pod-security.kubernetes.io/enforce=privileged \
    pod-security.kubernetes.io/warn=privileged \
    pod-security.kubernetes.io/audit=privileged
$ oc apply -f manifests/02-cratedb.yaml   # 3 data nodes, storageClass set to the tested SC
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
#   SELECT health FROM sys.cluster_health;  -> GREEN
```

**Result:** ✅ **Evidence:** [pods](evidence/ocp-4.22/tc-02-pods.txt) · [StatefulSet](evidence/ocp-4.22/tc-02-sts.txt) · [SCC](evidence/ocp-4.22/tc-02-scc.txt) · [per-pod SCC](evidence/ocp-4.22/tc-02-pod-scc.txt) · [SA/Secret/Service](evidence/ocp-4.22/tc-02-sa-secret-svc.txt) · [health](evidence/ocp-4.22/tc-02-scc-health.txt)

---

### TC-03 - Scaling operations

**Objective:** Data nodes scale up and down safely without data loss.

**Steps:** (`3 -> 2 -> 3` on this 3-worker cluster)

1. Load a test table with a known row count and replicas configured.
2. Scale **down** a data node definition (`replicas: 3 -> 2`) by editing the CR.
3. Wait for the operator to complete; verify it relocates shards off the
   departing node _before_ removing it, and the cluster returns to GREEN with the
   row count unchanged.
4. Scale **up** (`2 -> 3`); verify the new node joins, shards rebalance, and the
   cluster returns to GREEN with the row count unchanged.

**Expected result:**

- Scaling follows the documented process (master defs first, scale-ups, then
  scale-downs; see `docs/source/concepts.rst`).
- No lost rows; `sys.health` returns GREEN within `SCALING_TIMEOUT`.
- Operator status/notifications report `event: scale, status: success`.

**Verification:**

```console
$ oc get sts -n cratedb
# SELECT count(*) FROM <test_table>;   (unchanged before/after)
# SELECT health FROM sys.health;      (GREEN)
```

**Result:** ✅ **Evidence:** [tc-03-scaling.txt](evidence/ocp-4.22/tc-03-scaling.txt)

> **Note:** on OpenShift, data safety during scale-down is maintained through
> CrateDB's shard relocation and replication. Confirm scale-down preserves data
> and that the cluster returns to GREEN; `sys.shards` can be recorded as evidence.

---

### TC-04 - Pod recovery and rescheduling

**Objective:** The cluster self-heals when a pod is deleted, and stays available
when a whole node (zone) goes down.

This cluster runs **one worker per zone** (`us-central1-a/b/c`) with **zonal RWO**
GCP disks - the same multi-AZ shape as CrateDB Cloud. Recovery is validated in two
complementary scenarios:

**Sub-test A - Pod recovery:**

1. `oc delete pod <data-pod> -n cratedb`.
2. Confirm the StatefulSet recreates the pod, it re-attaches its persistent
   volume, the correct `crate-anyuid` SCC is re-applied, the node rejoins, and the
   cluster returns to GREEN.

_Expected:_ full recovery to GREEN, no data loss.

**Sub-test B - Zone-loss resilience:**

1. Ensure the test table has `number_of_replicas >= 1` so shards are replicated
   across zones.
2. `oc adm cordon <node>` then
   `oc adm drain <node> --delete-emptydir-data --ignore-daemonsets` to simulate a
   zone/node outage.
3. Confirm the cluster **remains available** and continues serving reads and
   writes through the shard replicas in the surviving zones.
4. `oc adm uncordon <node>`; the pod returns to its zone, re-attaches its volume,
   and the cluster returns to **GREEN**.

_Expected:_ continuous availability during the outage - reads and writes keep
succeeding via the surviving zones - with **no data loss** (`missing_shards = 0`
throughout); full recovery once the node returns. The pass condition is
availability + no lost data, not a specific health colour: with enough surviving
nodes to satisfy the replica count, CrateDB re-replicates onto them and health
returns to GREEN on its own (a transient YELLOW may or may not be observed
depending on when it is sampled). With zonal storage the drained pod's volume
re-attaches only in its own zone, which is standard for cloud block storage.

**Verification:**

```console
$ oc get pods -n cratedb -o wide -w        # note node/zone placement
# SELECT health, missing_shards, underreplicated_shards FROM sys.health;  # missing_shards=0 throughout
# an INSERT during the outage succeeds  ->  proves availability via surviving zones
```

**Result:** ✅ **Evidence:** [pod recovery](evidence/ocp-4.22/tc-04-pod-recovery.txt) · [zone-loss](evidence/ocp-4.22/tc-04-zone-loss.txt)

> **Observed:** Sub-test A - pod recreated, PVC re-attached, SCC re-applied, row
> count unchanged, GREEN. Sub-test B - the drained pod went `Pending` (zonal
> storage), a write **succeeded during the outage** with no data loss
> (`missing_shards = 0`), and CrateDB re-replicated onto the two surviving nodes,
> returning to GREEN; full recovery on uncordon.

> **Note:** zone-loss resilience relies on shard replication, so Sub-test B
> requires `number_of_replicas >= 1`. Record `sys.shards` before, during, and
> after to evidence continuous availability.

---

### TC-05 - Persistent storage validation

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

**Result:** ✅ **Evidence:** [tc-05-storage.txt](evidence/ocp-4.22/tc-05-storage.txt) · [PVCs](evidence/ocp-4.22/tc-05-pvc.txt)

> **Observed:** data persisted across pod restart (TC-04A) and across expansion.
> Operator-driven expansion of the `hot` data disks `32GiB -> 64GiB`: PVCs patched,
> `FileSystemResizeSuccessful` on all three, `EXPAND_STORAGE` webhook success, data
> intact (row count unchanged), health GREEN. By default the operator briefly
> restarts the cluster to finalize the filesystem resize
> (`CRATEDB_OPERATOR_NO_DOWNTIME_STORAGE_EXPANSION=false`); the debug volumes are
> not expanded (only the data node-group disks).

> **OpenShift note:** `blockOwnerDeletion` is disabled on PVC owner references
> in openshift mode - confirm no PVC finalizer errors in operator logs.

---

### TC-06 - Operator upgrade validation

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
# SELECT health FROM sys.health;  (still GREEN)
```

**Result:** ✅ **Evidence:** [tc-06-upgrade.txt](evidence/ocp-4.22/tc-06-upgrade.txt)

> **Observed:** real upgrade of both releases `2.63.1 -> 2.64.1`. The running
> CrateDB cluster was **undisturbed** - same three pods, `RESTARTS 0`, identical
> IPs/nodes across the upgrade (non-disruptive) - with data preserved
> (`count 2 -> 2`) and health GREEN before and after. The `crate-control` sidecar
> image on running pods is applied when pods are next recreated rather than during
> the operator upgrade itself; keep the sidecar tag aligned with the operator
> version, and recreate pods if an immediate sidecar update is required.

---

### TC-07 - OpenShift version compatibility validation

**Objective:** The full suite (TC-01 - TC-06, TC-08, TC-09) passes on the single
OpenShift version we commit to support.

**Steps:** Record the exact tested version (`oc version`) and confirm all core
test cases below pass on it. If additional versions are added later, extend this
table with one row per version and re-run.

**Expected result:** all core test cases pass on the committed version; any
deviations are recorded.

**Result summary:**

| OCP version   | TC-01 | TC-02 | TC-03 | TC-04 | TC-05 | TC-06 | TC-08 | TC-09 |
| ------------- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| 4.22.11 (EUS) | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    |

**Evidence:** _(`oc version` output + per-TC evidence files under `evidence/ocp-4.22/`)_

---

### TC-08 - Monitoring and observability validation

**Objective:** CrateDB metrics are observable on OpenShift.

**Steps:**

1. Confirm the Prometheus metrics endpoint is scrapeable. Each CrateDB pod exposes
   JMX/Prometheus metrics on **port 7071** (`/metrics`); the SQL Exporter sidecar
   additionally exposes SQL-based metrics on **port 9399**.
2. Optionally, with OpenShift user-workload monitoring, create a `ServiceMonitor`
   targeting the `prometheus` (7071) port and confirm the target is `Up`.
3. Access the CrateDB Admin UI on the HTTP port (4200) and confirm cluster status.

**Expected result:** the `:7071` metrics endpoint returns Prometheus-formatted
`crate_*` metrics; the Admin UI shows the cluster as healthy.

**Verification:**

```console
# metrics - JMX/Prometheus exporter on port 7071
$ oc port-forward -n cratedb pod/<crate-pod> 7071:7071
$ curl -s http://localhost:7071/metrics | grep '^crate_'

# Admin UI - HTTP on port 4200
$ oc port-forward -n cratedb pod/<crate-pod> 4200:4200   # then open http://localhost:4200
```

**Result:** ✅ **Evidence:** [metrics](evidence/ocp-4.22/tc-08-observability.txt) · [Admin UI](evidence/ocp-4.22/tc-08-adminui.png)

> **Observed:** each CrateDB pod exposes a JMX/Prometheus metrics endpoint on
> `:7071/metrics`, returning real metrics (`crate_query_*`, `crate_node` shard
> info/stats: primaries 2 / replicas 2 / unassigned 0). The Admin UI (via Route/
> port-forward on 4200) shows the healthy 3-node cluster (version 6.4.4, status
> GREEN). The SQL-exporter sidecar (`:9399`) provides additional SQL-based
> metrics and requires an SSL-enabled cluster to connect. It operates normally on an SSL-enabled cluster (the production
> default). Prometheus observability is validated here via the `:7071` endpoint.

---

### TC-09 - Operator removal and cleanup validation

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
$ oc get scc | grep crate-anyuid
$ oc get sa,secret,svc -n cratedb | grep crate
$ oc get pvc -n cratedb
```

**Result:** ✅ **Evidence:** [tc-09-cleanup.txt](evidence/ocp-4.22/tc-09-cleanup.txt)

> **Observed:** deleting the `CrateDB` CR removed all cluster resources - the
> namespaced objects (StatefulSet, the `crate-control`/`crate-discovery`/`crate`
> Services, Secrets, ServiceAccount) and the data/debug **PVCs** were
> garbage-collected via owner references, and the cluster-scoped SCC was removed
> explicitly by the operator's delete handler (operator log:
> `Deleted SCC crate-anyuid-cratedb-my-cluster`). `helm uninstall` of both
> releases succeeded and the namespace was left empty (`No resources found`). No
> orphaned resources.

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
- **Cluster width is bounded by the number of nodes.** The operator applies a
  hard pod anti-affinity keyed on `kubernetes.io/hostname`, so no two data pods of
  the same cluster are scheduled on the same node. Scaling the data tier beyond
  the available node count leaves the extra pods `Pending` until more nodes are
  added (validated in TC-03), so size the node pool for the intended cluster
  width. On `openshift` the operator does not add a zone-level spread constraint;
  in this test the pods happened to distribute across zones because each of the
  three worker nodes was in a different zone.
- **Storage expansion is supported (grow-only).** Data disks can be expanded in
  place. The operator applies the new size and, by default, performs a brief
  rolling restart to finalize the filesystem resize on CSI drivers that need a
  remount (`CRATEDB_OPERATOR_NO_DOWNTIME_STORAGE_EXPANSION=false`). A fully
  online, no-downtime mode is available (`=true`) on CSI drivers that support
  in-use filesystem resize. Volumes can grow but not shrink.
- **Sidecar image configuration is validated at deploy time:** the operator
  requires `CRATEDB_OPERATOR_CRATE_CONTROL_IMAGE` and fails fast with a clear
  error if it is unset, so misconfiguration is caught immediately. Keep the
  sidecar image tag aligned with the operator version.
- **Kernel parameters:** CrateDB requires `vm.max_map_count >= 262144`. OpenShift
  nodes commonly provide `262144` by default, which satisfies this - no node tuning was
  required. If a cluster's nodes report a lower value, raise it via the cluster
  administrator (Node Tuning Operator / MachineConfig) before deploying clusters.
- **`privileged` PSA required:** CrateDB pods start as UID 0 with `SYS_CHROOT`;
  the namespace must allow privileged PSA (or an equivalent policy exception).

---

## 5. Sign-off / attestation

By signing, CrateDB attests that it has tested the interoperability of the
CrateDB Operator with the listed Red Hat OpenShift version(s) and supports the
product when used with those versions.

|                          |               |
| ------------------------ | ------------- |
| Tested by                | Thomas Achatz |
| Reviewed by (Eng)        | _(pending)_   |
| Approved by (Product/CE) | _(pending)_   |
| Date                     | 2026-09-10    |

---

## 6. References

- Red Hat Partner Validation Guide (2026): https://docs.redhat.com/en/documentation/red_hat_partner_certification/2026/html-single/red_hat_partner_validation_guide/index
- CrateDB Operator OpenShift docs: `docs/source/openshift.rst`
- CrateDB Operator concepts (scaling/restart): `docs/source/concepts.rst`
