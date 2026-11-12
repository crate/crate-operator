.. _upgrading-exporters:

Upgrading Exporters
===================

Every CrateDB pod created by the operator ships two metrics exporters:

- The `Crate JMX HTTP Exporter`_, a Java agent that runs inside the CrateDB
  process. Its version is set with :envvar:`JMX_EXPORTER_VERSION`.
- The `SQL Exporter`_, a sidecar container named ``sql-exporter``. Its image is
  set with :envvar:`SQL_EXPORTER_IMAGE`.

The operator writes both settings into the StatefulSets of a cluster **only
when the cluster is created**. Changing the settings, or upgrading to an
operator release that ships newer defaults, affects new clusters only.
Existing clusters keep the versions they were created with until their
StatefulSets are changed by hand, as described on this page.

The :doc:`changelog <changelog>` lists the exporter versions each operator
release ships.

.. note::

   The StatefulSets use the ``OnDelete`` update strategy. Changing a
   StatefulSet therefore never restarts a pod by itself; pods pick up the
   change when they are recreated.

The examples below use these shell variables:

.. code-block:: console

   $ NAMESPACE=<namespace of the cluster>
   $ NAME=<name of the CrateDB resource>
   $ SELECTOR="app.kubernetes.io/component=cratedb,app.kubernetes.io/name=$NAME"

``NAME`` is the ``NAME`` column of ``kubectl get cratedbs``, not
``CLUSTERNAME``.

A cluster has one StatefulSet per node definition (``crate-master-$NAME`` and
``crate-data-<node name>-$NAME``). The selector matches all of them, and all
of their pods.


Upgrading the SQL Exporter
--------------------------

The SQL exporter runs in its own container, so it can be upgraded without
restarting CrateDB and without recreating the pods.

1. Set :envvar:`SQL_EXPORTER_IMAGE` on the operator, so that new clusters use
   the new image.

2. Change the image in the StatefulSets, so that pods created from now on use
   it. Add ``--dry-run=server`` to preview the change first:

   .. code-block:: console

      $ IMAGE=burningalchemist/sql_exporter:<new version>
      $ kubectl -n $NAMESPACE set image statefulset -l $SELECTOR sql-exporter=$IMAGE

3. Change the image of the running pods. Kubernetes restarts only the
   ``sql-exporter`` container of each pod; the ``crate`` container keeps
   running:

   .. code-block:: console

      $ kubectl -n $NAMESPACE set image pod -l $SELECTOR sql-exporter=$IMAGE

4. Check the result. The image is the new one, the restart count of the
   ``sql-exporter`` container went up by one, and the pods were not recreated:

   .. code-block:: console

      $ kubectl -n $NAMESPACE get pod -l $SELECTOR -o custom-columns='POD:.metadata.name,CREATED:.metadata.creationTimestamp,CONTAINER:.status.containerStatuses[*].name,IMAGE:.status.containerStatuses[*].image,RESTARTS:.status.containerStatuses[*].restartCount'

If step 3 is skipped, the pods switch to the new image whenever they are next
recreated.


Reloading the SQL Exporter Configuration
----------------------------------------

The SQL exporter reads its configuration, the ConfigMap
``crate-sql-exporter-$NAME``, only on start. After changing the ConfigMap,
wait about two minutes for Kubernetes to update the mounted files, then
restart the ``sql-exporter`` container in every pod:

.. code-block:: sh

   for POD in $(kubectl -n $NAMESPACE get pod -l $SELECTOR -o name); do
     kubectl -n $NAMESPACE exec $POD -c sql-exporter -- /bin/sh -c 'kill 1'
   done


Upgrading the JMX Exporter
--------------------------

The JMX exporter is loaded into the CrateDB process when it starts, and its
JAR file is downloaded by an init container. A new version therefore needs the
pods to be recreated.

The version shows up in three places of each StatefulSet:

- the ``fetch-jmx-exporter`` init container, which downloads the JAR file,
- the ``CRATE_JAVA_OPTS`` environment variable of the ``crate`` container,
  which loads the JAR file as a Java agent, and
- the volume mount of the ``crate`` container that makes the JAR file
  available.

.. warning::

   All three places must name the same version. With a mismatch, CrateDB
   cannot find the JAR file and the pod does not start.

1. Set :envvar:`JMX_EXPORTER_VERSION` on the operator, so that new clusters
   use the new version.

2. Replace the version in every StatefulSet of the cluster:

   .. code-block:: sh

      OLD=<current version>
      NEW=<new version>
      for STS in $(kubectl -n $NAMESPACE get statefulset -l $SELECTOR -o name); do
        kubectl -n $NAMESPACE get $STS -o yaml \
          | sed -e "s|crate-jmx-exporter-$OLD\.jar|crate-jmx-exporter-$NEW.jar|g" \
                -e "s|crate-jmx-exporter/$OLD/|crate-jmx-exporter/$NEW/|g" \
          | kubectl -n $NAMESPACE replace -f -
      done

3. Check that the old version is gone. The command prints nothing when it is:

   .. code-block:: console

      $ kubectl -n $NAMESPACE get statefulset -l $SELECTOR -o yaml | grep "crate-jmx-exporter[-/]$OLD"

4. Recreate the pods. There are two options:

   - Do nothing. The pods get the new version whenever they are next
     recreated, for example when the Kubernetes nodes are upgraded or when
     the operator restarts the cluster for a CrateDB version upgrade.
   - Delete the pods one at a time. Before deleting the next pod, wait until
     the deleted pod is ready again and the cluster is healthy:

     .. code-block:: console

        $ kubectl -n $NAMESPACE delete pod <pod name>
        $ kubectl -n $NAMESPACE get pod -l $SELECTOR -o wide
        $ kubectl -n $NAMESPACE get cratedb $NAME -o jsonpath='{.status.crateDBStatus.health}'
        GREEN

5. Check that the recreated pods use the new version:

   .. code-block:: console

      $ kubectl -n $NAMESPACE get pod -l $SELECTOR -o yaml | grep -o "javaagent:[^ =]*"


.. _Crate JMX HTTP Exporter: https://github.com/crate/jmx_exporter
.. _SQL Exporter: https://github.com/burningalchemist/sql_exporter
