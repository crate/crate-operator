OpenShift Support
=================

The CrateDB Kubernetes Operator supports deployment on Red Hat OpenShift
Container Platform 4.x.

Overview
--------

OpenShift enforces stricter security policies than standard Kubernetes.
The operator adapts to these constraints when ``CLOUD_PROVIDER`` is set to
``openshift``:

- **No ``pod_exec``**: OpenShift's restricted SCC does not permit
  ``pod_exec`` by default. The operator deploys a lightweight HTTP sidecar
  (``crate-control``) for SQL execution instead.
- **No privileged init containers**: The ``sysctl`` init container is
  skipped. Kernel tuning is handled via TuneD or MachineConfig.
- **Custom SCC**: A per-cluster SecurityContextConstraint is created to
  allow the ``SYS_CHROOT`` capability and ``RunAsAny`` UID policy.
- **PVC owner references**: ``blockOwnerDeletion`` is disabled on PVC
  owner references because the StatefulSet controller lacks permission
  to set finalizers on PVCs in OpenShift.
- **Lifecycle hooks disabled**: The ``postStart`` and ``preStop`` hooks
  (used for ``dc_util`` routing and graceful decommission) are disabled
  because they download binaries from an external server, which may not
  be reachable from OpenShift pods.

.. warning::

   Disabling lifecycle hooks means that the ``preStop`` graceful
   decommission (``dc_util --min-availability PRIMARIES``) is **not
   performed** on OpenShift. During rolling updates or pod evictions,
   shard availability may be temporarily reduced. Plan maintenance
   windows accordingly and monitor shard replication status.


Architecture Changes
--------------------

**Sidecar-based SQL Execution**
   A ``crate-control`` sidecar container runs alongside each CrateDB pod.
   It exposes port 5050 with a ``/exec`` endpoint for SQL execution and a
   ``/health`` endpoint for readiness probes. Communication is
   authenticated via a randomly generated token stored in a Kubernetes
   Secret (``crate-control-<name>``).

**Custom SecurityContextConstraints (SCC)**
   The operator creates one SCC per CrateDB cluster, scoped to a dedicated
   ServiceAccount. The SCC spec is as follows:

   .. code-block:: yaml

      allowPrivilegedContainer: false
      allowedCapabilities:
        - SYS_CHROOT
      requiredDropCapabilities:
        - KILL
        - MKNOD
      runAsUser:
        type: RunAsAny          # Required: CrateDB entrypoint starts as root
      seLinuxContext:
        type: MustRunAs
      fsGroup:
        type: RunAsAny
      supplementalGroups:
        type: RunAsAny
      volumes:
        - configMap
        - downwardAPI
        - emptyDir
        - persistentVolumeClaim
        - projected
        - secret
      users:
        - "system:serviceaccount:<namespace>:crate-<name>"

   **Why ``RunAsAny`` / root?** The upstream CrateDB container image uses
   ``chroot`` in its entrypoint to set up the runtime environment, which
   requires starting as UID 0. After ``chroot``, the process drops
   privileges to UID 1000 (the ``crate`` user). The pod-level
   ``securityContext`` is set to ``runAsUser: 0, fsGroup: 0``.

   **Why ``SYS_CHROOT``?** Required for the ``chroot`` syscall in the
   CrateDB entrypoint.

   **Why drop ``KILL`` and ``MKNOD``?** These capabilities are not needed
   by CrateDB and are dropped to reduce the attack surface.

**Per-Cluster ServiceAccount**
   A ServiceAccount ``crate-<name>`` is created in the cluster's namespace
   and bound to the SCC via the ``users`` field. This ServiceAccount has
   owner references and will be garbage collected when the CrateDB CR is
   deleted.


Prerequisites
-------------

1. OpenShift Container Platform 4.12 or later
2. Cluster-admin privileges for installation (see
   :ref:`operator-rbac-requirements` below)
3. Kernel parameters configured via MachineConfig or TuneD (see
   :ref:`kernel-tuning` below)
4. ``crate-control`` sidecar image built and available in your registry
5. CrateDB container image available in your registry


.. _kernel-tuning:

Configuring Kernel Parameters
-----------------------------

CrateDB requires specific kernel parameters, most critically
``vm.max_map_count=262144``. On standard Kubernetes the operator sets
these via a privileged init container. On OpenShift, **you** must
configure them before deploying any CrateDB cluster and verify host defaults.

Node Tuning Operator with machineConfigLabels (Recommended)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Node Tuning Operator (NTO) can generate a ``MachineConfig``
automatically when you use ``machineConfigLabels`` in the recommend
section. This applies kernel parameters at the node level via the
Machine Config Operator, guaranteeing the settings are in place before
any pod starts — with no race condition.

Create the following ``Tuned`` resource:

.. code-block:: yaml

   apiVersion: tuned.openshift.io/v1
   kind: Tuned
   metadata:
     name: cratedb-tuned
     namespace: openshift-cluster-node-tuning-operator
   spec:
     profile:
     - name: openshift-cratedb
       data: |
         [main]
         summary=Optimize systems running CrateDB
         include=openshift-node
         [sysctl]
         vm.max_map_count=262144
     recommend:
     - machineConfigLabels:
         machineconfiguration.openshift.io/role: worker
       priority: 10
       profile: openshift-cratedb

.. code-block:: console

   $ oc apply -f cratedb-tuned.yaml

This targets all nodes in the ``worker`` MachineConfigPool. If you run
CrateDB on a dedicated pool (recommended for production), replace the
label value accordingly (e.g., ``machineconfiguration.openshift.io/role:
cratedb``).

.. note::

   You need access to the ``openshift-cluster-node-tuning-operator``
   namespace to create the Tuned resource.

.. warning::

   The generated MachineConfig triggers a **rolling reboot** of all
   nodes in the matching pool. Apply this during a maintenance window.

After the rollout completes, verify on a node:

.. code-block:: console

   $ oc debug node/<node-name> -- chroot /host sysctl vm.max_map_count


Building the Sidecar Image
--------------------------

Build and push the ``crate-control`` sidecar to your container registry:

.. code-block:: console

   $ cd sidecars/cratecontrol
   $ podman build -t <registry>/crate-control:<tag> .
   $ podman push <registry>/crate-control:<tag>

For **disconnected / air-gapped clusters**, mirror the image to your
internal registry:

.. code-block:: console

   $ oc image mirror \
       <source-registry>/crate-control:<tag> \
       <internal-registry>/crate-control:<tag>

If your cluster uses an ``ImageDigestMirrorSet`` or
``ImageContentSourcePolicy``, add appropriate entries for the
``crate-control`` and ``crate`` images.

If pulling from a private registry, create a pull secret and link it to
the CrateDB ServiceAccount:

.. code-block:: console

   $ oc create secret docker-registry crate-pull-secret \
       --docker-server=<registry> \
       --docker-username=<user> \
       --docker-password=<password> \
       -n <cratedb-namespace>
   $ oc secrets link crate-<cluster-name> crate-pull-secret --for=pull \
       -n <cratedb-namespace>


.. _operator-rbac-requirements:

Operator RBAC Requirements
--------------------------

The operator's ClusterRole requires permissions to manage SCCs:

.. code-block:: yaml

   - apiGroups: ["security.openshift.io"]
     resources: ["securitycontextconstraints"]
     verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]

It also requires ``serviceaccounts`` in core API resources.

These permissions are included in the Helm chart's RBAC templates and
in ``deploy/rbac.yaml``.

**If your organization restricts SCC creation**, the SCC can be
pre-created by a cluster admin before the operator runs. The operator
will detect the existing SCC (409 Conflict) and skip creation. The SCC
name follows the pattern ``crate-anyuid-<namespace>-<cluster-name>``.

.. note::

   The person installing the operator needs sufficient privileges to
   create the ClusterRole and ClusterRoleBinding. After installation,
   the operator's ServiceAccount performs all SCC operations.


Installation
------------

Step 1: Install CRDs
^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

   $ helm repo add crate-operator https://crate.github.io/crate-operator
   $ helm install crate-operator-crds crate-operator/crate-operator-crds

Step 2: Install the Operator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

   $ helm install crate-operator crate-operator/crate-operator \
       --set env.CRATEDB_OPERATOR_CLOUD_PROVIDER=openshift \
       --set env.CRATEDB_OPERATOR_CRATE_CONTROL_IMAGE=<registry>/crate-control:<tag> \
       --namespace crate-operator \
       --create-namespace

Step 3: Prepare the Target Namespace
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create the namespace where CrateDB clusters will run and configure Pod
Security Admission:

.. code-block:: console

   $ oc new-project cratedb
   $ oc label namespace cratedb \
       pod-security.kubernetes.io/enforce=privileged \
       pod-security.kubernetes.io/warn=privileged \
       pod-security.kubernetes.io/audit=privileged

.. warning::

   The ``privileged`` Pod Security Admission level is required because
   CrateDB pods start as root (UID 0) and use ``SYS_CHROOT``. Without
   this label, pods will be rejected by the admission controller on
   OpenShift 4.12+.

   If your organization prohibits namespace-wide ``privileged`` PSA, you
   will need to work with your security team to create a suitable
   exception or use a policy engine (e.g., Kyverno, Gatekeeper) with
   more granular rules.


Storage Classes
---------------

CrateDB uses ``ReadWriteOnce`` (RWO) PersistentVolumeClaims, one per pod
(via StatefulSet volumeClaimTemplates).

Before deploying, verify available StorageClasses:

.. code-block:: console

   $ oc get storageclass

Key considerations:

- **Performance**: CrateDB is I/O intensive. Use SSD/NVMe-backed storage
  (e.g., ``gp3-csi`` on ROSA, ``ocs-storagecluster-ceph-rbd`` on ODF,
  or the ``local-storage`` operator for bare-metal).
- **Volume expansion**: If you plan to resize data volumes, ensure the
  StorageClass has ``allowVolumeExpansion: true``.
- **Debug volume**: The operator also creates a debug volume for Java
  heap dumps. This uses the ``DEBUG_VOLUME_STORAGE_CLASS`` config
  variable (defaults to the cluster's default StorageClass).

Use the chosen StorageClass name in the ``storageClass`` field of your
CrateDB resource spec.


Deploying CrateDB Clusters
---------------------------

.. code-block:: yaml

   apiVersion: cloud.crate.io/v1
   kind: CrateDB
   metadata:
     name: my-cluster
     namespace: cratedb
   spec:
     cluster:
       imageRegistry: crate
       name: crate-dev
       version: 6.2.1
     nodes:
       data:
       - name: hot
         replicas: 3
         resources:
           limits:
             cpu: 4
             memory: 4Gi
           disk:
             count: 1
             size: 128GiB
             storageClass: gp3-csi
           heapRatio: 0.5

The operator will automatically:

1. Create a SecurityContextConstraint (``crate-anyuid-cratedb-my-cluster``)
2. Create a ServiceAccount (``crate-my-cluster``)
3. Create a Secret with the sidecar auth token (``crate-control-my-cluster``)
4. Create a headless Service for the sidecar (``crate-control-my-cluster``)
5. Deploy the StatefulSet with the ``crate-control`` sidecar
6. Configure pod security context (``runAsUser: 0, fsGroup: 0``)

Verify the deployment:

.. code-block:: console

   $ oc get pods -n cratedb -l app.kubernetes.io/component=cratedb
   $ oc get scc | grep crate-anyuid

Confirm the correct SCC is being used by a pod:

.. code-block:: console

   $ oc get pod <pod-name> -n cratedb -o jsonpath='{.metadata.annotations.openshift\.io/scc}'


Exposing CrateDB
-----------------

CrateDB exposes two protocols:

- **HTTP API** (port 4200): Used for the Admin UI and REST SQL endpoint.
- **PostgreSQL wire protocol** (port 5432): Used by psql, JDBC, and
  other PostgreSQL-compatible clients.

**HTTP via Route (Admin UI / REST)**

.. code-block:: console

   $ oc create route passthrough cratedb-http \
       --service=crate-my-cluster \
       --port=4200 \
       -n cratedb

**PostgreSQL protocol**

OpenShift Routes are HTTP-only. For PostgreSQL wire protocol access, use
a ``LoadBalancer`` Service (if your platform supports it) or a
``NodePort``:

.. code-block:: console

   $ oc expose service crate-my-cluster \
       --type=LoadBalancer \
       --name=cratedb-pg-lb \
       --port=5432 \
       --target-port=5432 \
       -n cratedb


Security Considerations
-----------------------

**Root Access**
   CrateDB pods start as UID 0 (root). The CrateDB entrypoint
   immediately uses ``chroot`` to set up the runtime environment, then
   drops to UID 1000 (``crate`` user). The root phase is brief and
   limited to the entrypoint script.

**SCC Scope**
   Each CrateDB cluster gets its own SCC, bound only to
   ``system:serviceaccount:<namespace>:crate-<name>``. No cluster-wide
   grants are made. See the full SCC spec in the `Architecture Changes`_
   section above.

**Sidecar Authentication**
   The ``crate-control`` sidecar requires a token (from the
   ``crate-control-<name>`` Secret) in the ``Token`` HTTP header for all
   ``/exec`` requests. The ``/health`` endpoint is unauthenticated.

**Sidecar Network Scope**
   The sidecar is exposed via a headless ClusterIP Service and is only
   reachable from within the cluster network.

**Dropped Capabilities**
   ``KILL`` and ``MKNOD`` are explicitly dropped. Only ``SYS_CHROOT`` is
   added. ``allowPrivilegedContainer`` is ``false``.


Troubleshooting
---------------

**Pods rejected by admission controller**
   If pods fail with a ``Forbidden`` error mentioning Pod Security:

   .. code-block:: console

      $ oc get events -n cratedb | grep Forbidden

   Verify namespace PSA labels:

   .. code-block:: console

      $ oc get namespace cratedb -o jsonpath='{.metadata.labels}' | python3 -m json.tool

   Ensure ``pod-security.kubernetes.io/enforce: privileged`` is set.

**Pod fails with "cannot set blockOwnerDeletion" error**
   Verify ``CRATEDB_OPERATOR_CLOUD_PROVIDER=openshift`` is set:

   .. code-block:: console

      $ oc get deploy crate-operator -n crate-operator \
          -o jsonpath='{.spec.template.spec.containers[0].env}' | python3 -m json.tool

**Pod fails with "Permission denied" on /crate/bin/crate**
   The SCC may not be created or bound correctly:

   .. code-block:: console

      $ oc get scc | grep crate-anyuid
      $ oc describe scc crate-anyuid-<namespace>-<cluster-name>
      $ oc get pod <pod-name> -n <namespace> \
          -o jsonpath='{.metadata.annotations.openshift\.io/scc}'

**CrateDB fails with "vm.max_map_count too low"**
   Kernel parameters are not applied on the node. Check:

   .. code-block:: console

      $ oc debug node/<node-name> -- chroot /host sysctl vm.max_map_count

   If using TuneD, this may be a timing issue on the first pod per node.
   The pod will succeed on automatic retry. For a permanent fix, use
   MachineConfig (see :ref:`kernel-tuning`).

**Sidecar container fails to start**
   Check the image is pullable and the config is set:

   .. code-block:: console

      $ oc get events -n <namespace> | grep crate-control
      $ oc logs <pod-name> -c crate-control -n <namespace>

   Verify the operator has the image configured:

   .. code-block:: console

      $ oc get deploy crate-operator -n crate-operator \
          -o jsonpath='{.spec.template.spec.containers[0].env}' | python3 -m json.tool

**System user bootstrap fails**
   Check sidecar logs and verify the auth token Secret exists:

   .. code-block:: console

      $ oc logs <pod-name> -c crate-control -n <namespace>
      $ oc get secret crate-control-<cluster-name> -n <namespace>


Limitations
-----------

- **No graceful decommission on pod eviction**: The ``preStop`` lifecycle
  hook (``dc_util --min-availability PRIMARIES``) is disabled on
  OpenShift. Rolling updates and node drains may temporarily reduce shard
  availability. Monitor ``sys.shards`` during maintenance.
- **``CLOUD_PROVIDER`` is exclusive**: Setting ``openshift`` means
  cloud-specific features for ``aws``, ``azure``, or ``gcp`` (e.g.,
  zone awareness attribute auto-detection) are not active. You can still
  set zone attributes manually via ``.spec.nodes.data.*.settings``.
- **``CRATE_CONTROL_IMAGE`` is required**: The operator does not validate
  that this variable is set. If it is missing, StatefulSets will be
  created with an empty image reference and pods will fail to pull.
- **Lifecycle hooks disabled**: ``postStart`` (routing reset) and
  ``preStop`` (graceful decommission) hooks are skipped entirely.
