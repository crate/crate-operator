.. _kubernetes-maintenance:

Kubernetes Node Maintenance
===========================

This page describes how to upgrade or replace the Kubernetes worker nodes that
run CrateDB pods without the cluster turning red, and without needless shard
recovery while nodes restart.

It applies to maintenance that you start from outside the operator, for
example a node image upgrade, a Kubernetes version upgrade or a node pool
replacement. Restarts that the operator performs itself (version upgrades,
scaling, compute changes) already follow a safe sequence and need none of this.

.. warning::

   Do not start node maintenance while the operator is running an operation on
   the cluster, such as an upgrade, a scale or a restart. The operator manages
   ``cluster.routing.allocation.enable`` during those operations, and changing
   it by hand at the same time makes the two interfere.


Why a ``PodDisruptionBudget`` is not enough
-------------------------------------------

The operator creates a ``PodDisruptionBudget`` with ``maxUnavailable: 1``, but
it does not make a node drain safe for CrateDB. A pod counts as available as
soon as it is Ready, while CrateDB may still be recovering shards, so the next
drain can take down a primary and its only replica together.

If the cluster has several node definitions in ``spec.nodes`` (for example
dedicated master nodes), each has its own budget, so one drain can evict a pod
from each of them at once.

The operator also adds a ``preStop`` hook that stops CrateDB gracefully. It
does nothing on OpenShift, or when the pod cannot download its helper binary,
and it reports no error. The procedure below does the same steps by hand.


Before you start
----------------

The examples use these shell variables:

.. code-block:: console

   $ NAMESPACE=<namespace of the cluster>
   $ NAME=<name of the CrateDB resource>
   $ SELECTOR="app.kubernetes.io/component=cratedb,app.kubernetes.io/name=$NAME"

``NAME`` is the ``NAME`` column of ``kubectl get cratedbs``, not
``CLUSTERNAME``. SQL statements are run with the client of your choice, for
example ``crash``, connected to the cluster.

All settings and statements on this page apply to the whole cluster, so they
can be sent to any node that is up. Do not send them to the pod you are about
to decommission, or to one that is still restarting. A connection through the
cluster's Kubernetes service can end up on such a pod. When in doubt, connect
to a specific healthy pod, for example with ``kubectl port-forward``.

Check the following first:

- The cluster is green, and no shard is unassigned or relocating. Query the
  cluster itself, using :ref:`sys.health <cratedb:sys-health>` and
  :ref:`sys.shards <cratedb:sys-shards>`:

  .. code-block:: sql

     SELECT health FROM sys.health ORDER BY severity DESC LIMIT 1;
     SELECT count(*) FROM sys.shards WHERE state <> 'STARTED';

  The health must be ``GREEN`` and the count ``0``. A cluster without tables
  returns no row for the first query, which also counts as green. The health
  that the operator reports on the ``CrateDB`` resource is refreshed only
  periodically, by default every 60 seconds, and can be out of date. Use it
  as a hint, not as the gate.

- Every table and partition has at least one replica, also tables that allow
  a range such as ``0-1``, which can have no replica at all. Without replicas
  the data is lost when its node goes away, unless the volume follows the pod
  (see below). The query returns the tables and partitions that can have none:

  .. code-block:: sql

     SELECT table_schema, table_name, NULL AS partition_ident, number_of_replicas
     FROM information_schema.tables
     WHERE table_schema NOT IN ('sys', 'information_schema', 'pg_catalog')
       AND (number_of_replicas = '0' OR number_of_replicas LIKE '0-%')
     UNION ALL
     SELECT table_schema, table_name, partition_ident, number_of_replicas
     FROM information_schema.table_partitions
     WHERE number_of_replicas = '0' OR number_of_replicas LIKE '0-%';

- You know the current graceful stop settings, because the procedure
  overrides them and you remove the override at the end:

  .. code-block:: sql

     SELECT settings['cluster']['graceful_stop'] FROM sys.cluster;

- You have a recent snapshot.
- The cluster has more than one node. A single-node cluster cannot be
  decommissioned and is unavailable while its pod restarts.


Choose the stop guarantee
-------------------------

:ref:`cluster.graceful_stop.min_availability
<cratedb:cluster.graceful_stop.min_availability>` controls what CrateDB makes
sure of before a node may stop (see also :ref:`Graceful stop
<cratedb:conf_graceful_stop>`). The right value depends on where the data lives:

``PRIMARIES``
   Use it when the data volume follows the pod to its new node, such as EBS,
   Azure Disk or Persistent Disk. The data is not lost with the node, CrateDB
   only has to make sure that every primary has another copy available while
   the pod restarts.

``FULL``
   Use it when the data is on disks local to the node. The data is gone once
   the node is replaced, so every shard must be moved elsewhere first. This
   takes much longer and needs enough free disk space on the remaining nodes.
   Do not restrict shard allocation for it (step 2 and step 5 are skipped):
   with allocation limited to ``primaries``, CrateDB stops the node without
   moving the replicas away, and the guarantee does not hold. The volume of a
   pod on a local disk is also tied to its node, which step 4 takes care of.


Procedure
---------

1. **Cordon all nodes that you are going to replace**, before draining any of
   them. Otherwise a pod that is evicted from one node can be scheduled onto
   another node that is about to be drained, and is restarted a second time.

   .. code-block:: console

      $ kubectl cordon <node> [<node> ...]

   Then handle one Kubernetes node at a time, and within it one CrateDB pod at
   a time. The steps 2 to 6 are repeated for each CrateDB pod on the node. Do
   not decommission pods of different node definitions in parallel. List the
   pods of this cluster that run on a node with:

   .. code-block:: console

      $ kubectl -n $NAMESPACE get pods -l $SELECTOR -o wide \
          --field-selector spec.nodeName=<node>

   If other CrateDB clusters also have pods on the node, repeat the steps 2 to
   6 for each of them before you drain it. Check for them with:

   .. code-block:: console

      $ kubectl get pods -A -l app.kubernetes.io/component=cratedb -o wide \
          --field-selector spec.nodeName=<node>

2. **For** ``PRIMARIES`` **only: allow only primaries to be allocated** (see
   :ref:`cluster.routing.allocation.enable
   <cratedb:cluster.routing.allocation.enable>`) while a node is down. This avoids
   building replicas on the remaining nodes for a node that is about to
   return:

   .. code-block:: sql

      SET GLOBAL TRANSIENT "cluster.routing.allocation.enable" = 'primaries';

   Use ``primaries`` and not ``new_primaries``, which the `CrateDB rolling
   upgrade guide`_ uses. ``new_primaries`` also stops the primaries of the
   returning node from being recovered. With ``FULL``, skip this step.

3. **Decommission the pod.** The CrateDB node name is the node definition
   name followed by the pod's ordinal, for example ``data-hot-0`` for the
   first pod of a ``hot`` data node definition, or ``master-1``. It is not the
   pod name. List the names from :ref:`sys.nodes <cratedb:sys-nodes>` with:

   .. code-block:: sql

      SELECT name FROM sys.nodes ORDER BY name;

   Then run :ref:`ALTER CLUSTER DECOMMISSION
   <cratedb:alter_cluster_decommission>`:

   .. code-block:: sql

      SET GLOBAL TRANSIENT "cluster.graceful_stop.min_availability" = 'PRIMARIES';
      SET GLOBAL TRANSIENT "cluster.graceful_stop.timeout" = '720s';
      SET GLOBAL TRANSIENT "cluster.graceful_stop.force" = false;
      ALTER CLUSTER DECOMMISSION 'data-hot-0';

   Use ``FULL`` instead of ``PRIMARIES`` for node-local disks. The statement
   returns immediately. When it has finished, the CrateDB process exits. Watch
   it with:

   .. code-block:: console

      $ kubectl -n $NAMESPACE logs -f <pod> -c crate

   Set :ref:`cluster.graceful_stop.timeout
   <cratedb:cluster.graceful_stop.timeout>` explicitly, because its default is
   2 hours. ``720s`` is enough for ``PRIMARIES``. For ``FULL``, size it to the
   time that moving the data takes, which can be hours. The pod's termination
   grace period (20 minutes, or 15 on clusters created by older operator
   versions) does not limit a decommission that you start with SQL, but it does
   limit the graceful stop that runs when you delete the pod in step 4.

   With ``force`` set to ``false``, a decommission that runs into the timeout
   does not stop the node: the process keeps running and the shards stay where
   they are. If that happens, check the free disk space on the other nodes and
   the shards on the node, and run the decommission again with a longer
   timeout. Do not delete the pod while the process is still running.

4. **Move the pod.** After the process has exited, the kubelet restarts the
   container on the same node, even though the node is cordoned, and the node
   joins the cluster again. Delete the pod right away, so that the StatefulSet
   creates it on another node, and wait for it to become Ready:

   .. code-block:: console

      $ kubectl -n $NAMESPACE delete pod <pod>
      $ kubectl -n $NAMESPACE wait --for=condition=Ready pod/<pod> --timeout=20m

   If the node cannot be left running until the pod is deleted, use
   ``kubectl drain`` instead, which honours the ``PodDisruptionBudget``.

   Where the operator's lifecycle hooks work (not on OpenShift, and only if the
   pod can download ``dc_util``), every container start also resets
   ``cluster.routing.allocation.enable`` to ``all``, so the restart in this
   step resets it before you can react. With ``PRIMARIES``, set ``primaries``
   again straight after the restart, before you delete the pod. The reset when
   the new pod starts does no harm, because step 5 sets ``all`` anyway:

   .. code-block:: sql

      SET GLOBAL TRANSIENT "cluster.routing.allocation.enable" = 'primaries';

   With node-local disks (``FULL``), the pod's ``PersistentVolumeClaim`` is
   bound to this node, and a new pod cannot start elsewhere with it. First make
   sure that no shard is left on the node. The node rejoins after the restart
   and can receive shards again, in which case you decommission it again:

   .. code-block:: sql

      SELECT count(*) FROM sys.shards WHERE node['name'] = 'data-hot-0';

   When the count is ``0``, delete the claim together with the pod. This
   destroys the data on this node, which ``FULL`` has moved elsewhere.
   Depending on the storage class, the ``PersistentVolume`` has to be deleted
   too:

   .. code-block:: console

      $ kubectl -n $NAMESPACE delete pvc <claim> --wait=false
      $ kubectl -n $NAMESPACE delete pod <pod>

5. **For** ``PRIMARIES`` **only: allow allocation again:**

   .. code-block:: sql

      SET GLOBAL TRANSIENT "cluster.routing.allocation.enable" = 'all';

6. **Wait for green** before the next pod, and do not rely on the pod being
   Ready. With allocation limited to ``primaries`` the cluster stays yellow
   after the node returned, until you did step 5. Run the queries from "Before
   you start" again:

   .. code-block:: sql

      SELECT health FROM sys.health ORDER BY severity DESC LIMIT 1;
      SELECT count(*) FROM sys.shards WHERE state <> 'STARTED';

   The cluster is ready for the next step when the health is ``GREEN`` and the
   count is ``0``. Recovery after a restart can take a while on large tables;
   do not shorten it.

7. **Drain the node** once no CrateDB pod of any cluster is left on it, for the
   remaining workloads:

   .. code-block:: console

      $ kubectl drain <node> --ignore-daemonsets --delete-emptydir-data \
          --pod-selector 'app.kubernetes.io/component!=cratedb'

   The selector keeps the drain from evicting a CrateDB node pod that you have
   not handled yet, for example one of another cluster, without the graceful
   stop. Other pods of the clusters, such as backup pods, are drained as usual.

   Then upgrade or remove the node, and continue with the next one.

When the last node is done, verify that
``cluster.routing.allocation.enable`` is ``all`` again, and remove the graceful
stop overrides from step 3. The reset brings back the values from before, from
the configuration or a persistent setting. If you had set them with ``SET GLOBAL
TRANSIENT`` yourself, set those values again:

.. code-block:: sql

   RESET GLOBAL "cluster.graceful_stop.min_availability",
                "cluster.graceful_stop.timeout",
                "cluster.graceful_stop.force";
   SELECT settings['cluster']['routing']['allocation']['enable'],
          settings['cluster']['graceful_stop'] FROM sys.cluster;


Managed node pool upgrades
--------------------------

The rolling upgrades of managed Kubernetes services (EKS, AKS, GKE and others)
drain nodes without regard to the state of CrateDB, and give up on the
``PodDisruptionBudget`` after a provider-specific time. Prefer to create a new
node pool, move to it node by node with the procedure above, and delete the old
pool. If you must use the provider's upgrade, use the smallest surge and
parallelism, and pause it whenever the cluster is not green.


If something goes wrong
-----------------------

- If a pod does not become Ready, leave allocation as it is, do not drain
  further nodes, and look at ``kubectl describe pod`` and the namespace events.
  Pods with a zone-bound volume can only run on nodes in that zone.
- If the cluster turned red, stop all maintenance. Do not delete pods or
  volumes. Restore the nodes that were removed if possible, since the missing
  primaries are on their volumes.


.. _CrateDB rolling upgrade guide: https://cratedb.com/docs/guide/admin/upgrade/rolling.html
