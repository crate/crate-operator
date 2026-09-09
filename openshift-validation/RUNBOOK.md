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

# CRDs as their own release
helm upgrade --install crate-operator-crds crate-operator/crate-operator-crds \
  --version 2.64.1 \
  --namespace crate-operator --create-namespace

# operator, with the bundled CRD subchart disabled
helm upgrade --install crate-operator crate-operator/crate-operator \
  --version 2.64.1 \
  --set env.CRATEDB_OPERATOR_CLOUD_PROVIDER=openshift \
  --set env.CRATEDB_OPERATOR_CRATE_CONTROL_IMAGE=crate/crate-control:2.64.1 \
  --set env.CRATEDB_OPERATOR_DEBUG_VOLUME_STORAGE_CLASS=standard-csi \
  --set crate-operator-crds.enabled=false \
  --namespace crate-operator --create-namespace
```

> `CRATEDB_OPERATOR_DEBUG_VOLUME_STORAGE_CLASS` is required here: the operator
> provisions a debug (heap-dump) volume, and its default StorageClass name does
> not exist on this cluster (the default class is `standard-csi`). Without it the
> CrateDB pods stay `Pending` on an unbound debug PVC. Optionally add
> `--set env.CRATEDB_OPERATOR_DEBUG_VOLUME_SIZE=16GiB` to shrink the default
> 64 GiB/pod debug volume.

> Both charts are pinned to `2.64.1` so a re-run reproduces the validated
> version. Keep the `crate-control` tag equal to the operator version.

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

Cluster health — record into the evidence folder. This cluster runs without
SSL, so reach the HTTP endpoint via port-forward:

```bash
oc port-forward -n cratedb pod/crate-data-hot-my-cluster-0 4200:4200 &
# then: SELECT health FROM sys.cluster_health;   -> expect GREEN
```

> If you prefer a Route: use `oc create route edge` when CrateDB runs without
> SSL (the router terminates TLS and forwards plain HTTP to :4200).
> `oc create route passthrough` only works when CrateDB itself terminates TLS,
> i.e. when SSL is configured on the cluster.

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
| TC-03 | Scale `hot` `3 → 2`, then `2 → 3` (operator places 1 data pod/node → max = worker count = 3)           | `... \| tee evidence/ocp-4.22/tc-03-scaling.txt` |
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
helm uninstall crate-operator-crds -n crate-operator
oc delete -f manifests/01-namespace.yaml
```

---

## After the run — evidence, report, submission

**Evidence layout.** `capture.sh` writes read-only snapshots into
`evidence/<ocp-version>/`; capture interactive actions (scaling, pod-delete,
drain, upgrade) by `tee`-ing them into the same folder, and save the Admin UI
screenshot there too. Link each file from the matching `Evidence:` slot in
`partner-validation-report.md`. The `evidence/` folder is gitignored (it contains
infra details — node names, IPs, hostnames) and is kept out of the public repo.

**Report — two outputs.** `partner-validation-report.md` is the **canonical
source**: clean, relative links, committed to the repo, and edited after reviews.
The thing submitted to Red Hat is a **separate derived copy** (kept out of the
public repo) — do not embed evidence or environment specifics into the canonical
file.

**Building the submission copy** (generate from the canonical file into a
gitignored location, e.g. `submission/`; then render to PDF):

- [ ] **Embed the evidence inline** — external readers can't follow the relative
      `evidence/...` links, so paste the relevant command output and the Admin UI
      screenshot into the report body.
- [ ] **Sanitize infra details** while embedding — node FQDNs → `worker-a/-b/-c`,
      pod/internal IPs and the LoadBalancer IP → redacted, and crop/blur the IP in
      the Admin UI screenshot.
- [ ] **Swap internal repo paths for public URLs or drop them** — e.g.
      `docs/source/openshift.rst` → the public docs page on
      `crate-operator.readthedocs.io`; `manifests/*.yaml` / `concepts.rst` →
      describe inline or link the public equivalent. No dead relative paths in the
      submitted PDF.
- [ ] Render to PDF (e.g. `pandoc <submission>.md -o <submission>.pdf`) and link
      that PDF as a resource in Partner Connect.

After reviews, edit the canonical `partner-validation-report.md` and regenerate
the submission copy — the canonical file never carries embedded evidence, so it
stays clean and diffable.

**Submission — Red Hat Partner Connect** (filed in the portal, not from this repo;
most of the *listing* is Product/Marketing/Legal, not Engineering):

- [ ] **Prerequisite (blocking):** a Red Hat Partner Connect **corporate account**
      exists and program terms are accepted. *(Who administers it? If none, who has
      authority to create it?)*
- [ ] Portal listing (Product/Marketing/Legal): create Product → category; product
      info (name, logo, description); ≥ 3 linked resources (e.g. the rendered
      report, the OpenShift docs page); support details (website mandatory) +
      marketing/technical contacts; legal (license + privacy URLs); SEO alias.
- [ ] Validation attestation (Engineering supplies the evidence): attest
      interoperability tested on the selected OpenShift version(s); attest CrateDB
      supports the product on those versions; link the rendered report.
- [ ] After submission, Red Hat's ecosystem team reviews the listing; on approval
      the product is published to the Red Hat Ecosystem Catalog.

> Scope: this is Partner Validation (self-attested), **not** full Operator
> Certification. Certification would additionally require packaging the operator
> as an OLM bundle on a certified registry and passing Red Hat's automated checks
> (preflight / scorecard) — out of scope here.
