#!/usr/bin/env bash
#
# capture.sh — snapshot read-only verification evidence for the CrateDB Operator
#              OpenShift Partner Validation.
#
# This captures the *read-only* verification commands referenced in
# partner-validation-report.md into evidence/<ocp-version>/ with timestamped
# headers.
#
#
# Usage:
#   ./capture.sh <ocp-version> [cratedb-namespace] [operator-namespace]
#
# Example:
#   ./capture.sh 4.15 cratedb crate-operator
#
set -u

OCP_VERSION="${1:-}"
CRATEDB_NS="${2:-cratedb}"
OPERATOR_NS="${3:-crate-operator}"

if [[ -z "$OCP_VERSION" ]]; then
  echo "usage: $0 <ocp-version> [cratedb-namespace] [operator-namespace]" >&2
  echo "example: $0 4.15 cratedb crate-operator" >&2
  exit 2
fi

CLI="$(command -v oc || command -v kubectl || true)"
if [[ -z "$CLI" ]]; then
  echo "error: neither 'oc' nor 'kubectl' found on PATH" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EVIDENCE_DIR="$SCRIPT_DIR/evidence/ocp-$OCP_VERSION"
mkdir -p "$EVIDENCE_DIR"

snap() {
  local file="$EVIDENCE_DIR/$1"; shift
  local title="$1"; shift
  [[ "$1" == "--" ]] && shift
  {
    echo "===================================================================="
    echo "# $title"
    echo "# \$ $*"
    echo "# captured: $(date -u '+%Y-%m-%dT%H:%M:%SZ') (UTC)"
    echo "===================================================================="
    "$@" 2>&1
    echo
    echo "# exit code: $?"
  } >"$file"
  echo "  -> $1  ($file)"
}

echo "Capturing evidence for OpenShift $OCP_VERSION"
echo "  cratedb namespace:  $CRATEDB_NS"
echo "  operator namespace: $OPERATOR_NS"
echo "  using CLI:          $CLI"
echo "  writing to:         $EVIDENCE_DIR"
echo

# ---- 00: environment ------------------------------------------------------
{
  echo "===================================================================="
  echo "# Environment snapshot"
  echo "# captured: $(date -u '+%Y-%m-%dT%H:%M:%SZ') (UTC)"
  echo "===================================================================="
  echo
  echo "### CLI / cluster version"
  "$CLI" version 2>&1
  echo
  echo "### Nodes"
  "$CLI" get nodes -o wide 2>&1
  echo
  echo "### StorageClasses"
  "$CLI" get storageclass 2>&1
  echo
  echo "### Helm releases (operator namespace)"
  if command -v helm >/dev/null 2>&1; then
    helm list -n "$OPERATOR_NS" 2>&1
  else
    echo "(helm not found on PATH)"
  fi
  echo
  echo "### Operator image"
  "$CLI" get deploy crate-operator -n "$OPERATOR_NS" \
    -o jsonpath='{range .spec.template.spec.containers[*]}{.name}={.image}{"\n"}{end}' 2>&1
} >"$EVIDENCE_DIR/00-environment.txt"
echo "  -> environment  ($EVIDENCE_DIR/00-environment.txt)"

# ---- TC-01: operator installation ----------------------------------------
snap "tc-01-crd.txt"          "TC-01 CRD registered"          -- \
  "$CLI" get crd cratedbs.cloud.crate.io
snap "tc-01-operator.txt"     "TC-01 operator deployment"     -- \
  "$CLI" get deploy crate-operator -n "$OPERATOR_NS" -o wide
snap "tc-01-operator-logs.txt" "TC-01 operator logs (tail)"   -- \
  "$CLI" logs deploy/crate-operator -n "$OPERATOR_NS" --tail=100

# ---- TC-02: cluster deployment (also used by TC-03/04/05) -----------------
snap "tc-02-pods.txt"         "TC-02 CrateDB pods"            -- \
  "$CLI" get pods -n "$CRATEDB_NS" -l app.kubernetes.io/component=cratedb -o wide
snap "tc-02-sts.txt"          "TC-02 StatefulSets"            -- \
  "$CLI" get sts -n "$CRATEDB_NS" -o wide
snap "tc-02-scc.txt"          "TC-02 per-cluster SCC exists"  -- \
  "$CLI" get scc
snap "tc-02-sa-secret-svc.txt" "TC-02 SA / Secret / Service"  -- \
  "$CLI" get sa,secret,svc -n "$CRATEDB_NS"
snap "tc-02-events.txt"       "TC-02 namespace events"        -- \
  "$CLI" get events -n "$CRATEDB_NS" --sort-by=.lastTimestamp

# Per-pod SCC annotation (proves the crate-anyuid SCC is in effect).
{
  echo "===================================================================="
  echo "# TC-02 per-pod SCC annotation (openshift.io/scc)"
  echo "# captured: $(date -u '+%Y-%m-%dT%H:%M:%SZ') (UTC)"
  echo "===================================================================="
  for pod in $("$CLI" get pods -n "$CRATEDB_NS" \
        -l app.kubernetes.io/component=cratedb \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    scc="$("$CLI" get pod "$pod" -n "$CRATEDB_NS" \
        -o jsonpath='{.metadata.annotations.openshift\.io/scc}' 2>&1)"
    echo "$pod -> ${scc:-<none>}"
  done
} >"$EVIDENCE_DIR/tc-02-pod-scc.txt"
echo "  -> pod SCC annotations  ($EVIDENCE_DIR/tc-02-pod-scc.txt)"

# ---- TC-05: persistent storage --------------------------------------------
snap "tc-05-pvc.txt"          "TC-05 PersistentVolumeClaims"  -- \
  "$CLI" get pvc -n "$CRATEDB_NS" -o wide

# ---- TC-06: operator upgrade (version snapshot) ---------------------------
snap "tc-06-operator-image.txt" "TC-06 operator image after upgrade" -- \
  "$CLI" get deploy crate-operator -n "$OPERATOR_NS" -o jsonpath='{..image}'

echo
echo "Done. Review the files in $EVIDENCE_DIR and link them from the"
echo "matching Evidence: slots in partner-validation-report.md."
echo
echo "Reminders — capture these manually (not read-only):"
echo "  * Cluster health:  SELECT health FROM sys.cluster;  -> paste into tc-*.txt"
echo "  * TC-03 scaling, TC-04 pod-delete/drain, TC-06 helm upgrade output:"
echo "      run the command with '| tee $EVIDENCE_DIR/<tc>.txt'"
echo "  * TC-08 Admin UI / metrics: save a screenshot into $EVIDENCE_DIR/"
