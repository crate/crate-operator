# OpenShift Partner Validation — Runbook (this cluster)

Ordered steps to stand up the operator + a CrateDB test cluster on the validation
environment, then run the test cases. Tailored to the provisioned cluster.

---

## Step 0 — Preflight

Cluster-admin, version 4.22.11, healthy cluster-operators, NTO present, `ssd-csi`
expandable class confirmed. Capture the evidence:

```bash
./capture.sh 4.22 cratedb crate-operator
```

Optional sanity checks specific to this cluster:

```bash
oc get nodes -L topology.kubernetes.io/zone
oc get sc ssd-csi -o jsonpath='{.volumeBindingMode}{"\n"}'
```

`WaitForFirstConsumer` matters here: it lets each StatefulSet pod's zonal PVC be
provisioned in the zone the pod is scheduled into, so the 3 CrateDB pods spread
across the 3 zones instead of all landing in one.

---

## Step 1 — Verify kernel parameter (no tuning needed on this cluster)

CrateDB requires `vm.max_map_count >= 262144`. OpenShift nodes provide `262144`
by default, so no node tuning is required here — just confirm and capture it as
evidence:

```bash
for n in $(oc get nodes -l node-role.kubernetes.io/worker -o name); do
  echo "== $n =="
  oc debug "$n" -- chroot /host sysctl vm.max_map_count
done | tee evidence/ocp-4.22/00-sysctl.txt
```

Expect `vm.max_map_count = 262144` on each worker. (If a future cluster's nodes
report a lower value, raise it via the Node Tuning Operator / MachineConfig
before deploying CrateDB.)

---

## Step 2 — Install the operator (TC-01)

`helm upgrade --install` is idempotent, so this step is safe to re-run.

```bash
helm repo add crate-operator https://crate.github.io/crate-operator
helm repo update

helm upgrade --install crate-operator-crds crate-operator/crate-operator-crds

helm upgrade --install crate-operator crate-operator/crate-operator \
  --set env.CRATEDB_OPERATOR_CLOUD_PROVIDER=openshift \
  --set env.CRATEDB_OPERATOR_CRATE_CONTROL_IMAGE=crate/crate-control:2.64.1 \
  --namespace crate-operator --create-namespace
```

> Pin the chart if you want an exact operator version: add `--version <chart>`.
> Keep the `crate-control` tag equal to the operator version.

Verify + capture:

```bash
oc get crd cratedbs.cloud.crate.io
oc rollout status deploy/crate-operator -n crate-operator
./capture.sh 4.22 cratedb crate-operator
```

---

## Step 3 — Prepare the namespace (PSA privileged)

```bash
oc apply -f manifests/01-namespace.yaml
```

---

## Step 4 — Deploy the CrateDB test cluster (TC-02)

```bash
oc apply -f manifests/02-cratedb.yaml
oc get pods -n cratedb -o wide -w    # wait for 3/3 pods Ready, spread across zones
```

Verify the OpenShift adaptations landed, and capture evidence:

```bash
./capture.sh 4.22 cratedb crate-operator
oc get scc | grep crate-anyuid
oc get pod <pod> -n cratedb -o jsonpath='{.metadata.annotations.openshift\.io/scc}{"\n"}'
```

Cluster health (via a Route or `psql`) — record into the evidence folder:

```bash
oc create route passthrough cratedb-http --service=crate-my-cluster --port=4200 -n cratedb
# then: SELECT health FROM sys.cluster;   -> expect GREEN
```

**Prepare for TC-04:** create a test table with a replica so shards exist in
other zones (needed for the zone-loss resilience sub-test):

```sql
CREATE TABLE validation.t (id INT, v TEXT)
  CLUSTERED INTO 6 SHARDS WITH (number_of_replicas = 1);
INSERT INTO validation.t VALUES (1, 'a'), (2, 'b');
```

---

## Step 5 — Run the remaining test cases

Follow `partner-validation-report.md` §3 in order and record results + evidence:

| TC    | What to do                                                                                            | Capture                                          |
| ----- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| TC-03 | Scale `hot` `3 → 5`, then `5 → 3` (fits at cpu:1)                                                     | `... \| tee evidence/ocp-4.22/tc-03-scaling.txt` |
| TC-04 | A: `oc delete pod`; B: `oc adm cordon`/`drain` a worker, watch health stay available, then `uncordon` | `... \| tee evidence/ocp-4.22/tc-04-*.txt`       |
| TC-05 | Restart a pod, confirm data intact; expand `disk.size`                                                | `./capture.sh 4.22 ...` (PVCs)                   |
| TC-06 | `helm upgrade` the operator, confirm cluster undisturbed                                              | `... \| tee evidence/ocp-4.22/tc-06-upgrade.txt` |
| TC-08 | Metrics endpoint / ServiceMonitor + Admin UI screenshot                                               | screenshot → `evidence/ocp-4.22/`                |
| TC-09 | `oc delete cratedb my-cluster`; confirm SCC/SA/Secret/Service gone; `helm uninstall`                  | `./capture.sh 4.22 ...` before/after             |

After each interactive action, re-run `./capture.sh 4.22 cratedb crate-operator`
to refresh the read-only snapshots, and `tee` the action's own output as noted.

---

## Teardown (when done)

```bash
oc delete cratedb my-cluster -n cratedb
helm uninstall crate-operator -n crate-operator
helm uninstall crate-operator-crds
oc delete -f manifests/01-namespace.yaml
```
