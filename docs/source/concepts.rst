Concepts
========

.. _concept-bootstrapping:

Bootstrapping
-------------

As part of the cluster management, the *CrateDB Kubernetes Operator* requires a
CrateDB user named ``system`` with ``ALL`` privileges. The user will be created
automatically upon creation of a new cluster. For that, the *CrateDB Kubernetes
Operator* will use Kubernetes' ``exec`` feature to run the bundled ``crash``
command inside on of the eligable master nodes. The user creation will fail
gracefully if the system user already exists, for whatever reason.
Additionally, the user will receive the ``ALL`` privileges such that they can
then do *anything* in the cluster.

A user may specify a license in ``spec.cluster.license`` which should be loaded
into the cluster. Since a cluster may be started with more nodes than allowed
by the free license, the *CrateDB Kubernetes Operator* will attempt to set the
license *before* trying to create the system user. Thus, license setting works
the same way, by ``exec``\ing into a master node's crate container and running
``crash``.

Lastly, using the newly created ``system`` user, the *CrateDB Kubernetes
Operator* will create all users specified under ``spec.users`` with their
corresponding passwords and the ``ALL`` privileges by connecting to the cluster
using the PostgreSQL protocol, and *not* by ``exec``\ing into a Kubernetes
container.

The entire bootstrapping process may not take longer than 1800 seconds
(default) before it is considered failed. The timeout can be configed with the
:envvar:`BOOTSTRAP_TIMEOUT` environment variable.

Cluster Restart
---------------

Some situations, such as version upgrades, require a cluster restart. Most
times a rolling restart is sufficient. When instructed to do so, the *CrateDB
Kubernetes Operator* will perform a rolling cluster restart following this
process:

#. If there are master nodes, restart each of them in ordinal order.

#. For each data node definition, in the order they are defined in the spec,
   restart each of its nodes in ordinal order.

Restarting a CrateDB node works by deleting the corresponding Kubernetes pod.
The operator will then wait for a new pod to go down and another one to come up
and join the CrateDB cluster. Lastly, the cluster needs to `report a green
health`_ three times in a row with a 30 second wait time in between, for a pod
to be considered back in service.

Cluster Scaling
---------------

From time to time it's necessary to scale a cluster. At this time, the operator
allows for scaling *existing* node definitions up and down. That means, when
a cluster contains 3 master nodes, 4 data nodes with name "hot" and 10 data
nodes with name "cold", each of them can be scaled up or down (there's a
minimum of 3 master nodes) as one pleases.

The scaling operation will follow these four basic steps:

#. First, all master nodes will be scaled. Up or down doesn't matter.

   Since the master nodes do not have any data, scaling them is fairly quickly
   done. Getting this change out of the way first removes complexity during the
   rest of the process.

#. Next, all node definitions that are scaled up will be handled.

   This ensures that, even when there are tables with loads of replicas in the
   cluster, scaling down other nodes reduces the risk of too few nodes. It also
   means that data will be started to move to the new nodes already, such that,
   when nodes are removed in step 3, some of their data has already been
   transferred.

#. Next, the remaining node definitions that will be scaled down are taken care
   of.

   This step is a bit more complex than the previous ones: First of all, the
   operator checks that none of the table in the cluster uses more replicas
   than there are nodes available in the cluster. This is, to avoid
   underreplicated tables.

   Next, it will deallocate all shards from the nodes that will be turned off.
   Once the data has been moved, the corresponding nodes are turned off by
   decreasing the number of replicas in the Kubernetes StatefulSet.

#. The last step the operator is going to take is to update all StatefulSets
   that make a CrateDB cluster to the expected total number of nodes. This
   ensures that, if a pod dies, its restart won't screw with CrateDB's expected
   number of nodes. This will also involve acknowledging all
   ``gateway.expected_nodes`` `node checks`_ (ID 1).

The entire scaling operation may not take longer than 3600 seconds or whatever
is configured in the :envvar:`SCALING_TIMEOUT` environment variable.

.. _concept-webhooks:

Webhooks
--------

Kubernetes follows an event driven architecture. Depending on your use of the
operator, it can be beneficial to receive "notifications" when some events
occurred, such as a successful or failed cluster upgrade or scaling. By setting
the environment variables :envvar:`WEBHOOK_PASSWORD`, :envvar:`WEBHOOK_URL`,
and :envvar:`WEBHOOK_USERNAME` to non-empty values, the operator will send HTTP
POST requests to the provided URL. An exemplarily JSON payload is shown and
documented below.

.. important::

   It's worth noting, that the operator will *not* retry failed webhook
   notifications!

.. code-block:: json

   {
     "cluster": "my-new-crate-cluster",
     "event": "upgrade",
     "namespace": "my-crate-namespace",
     "scale_data": null,
     "status": "success",
     "upgrade_data": {
       "new_registry": "crate",
       "new_version": "4.1.6",
       "old_registry": "crate",
       "old_version": "4.1.5"
     },
   }

:``cluster``:
   The Kubernetes name (``.metadata.name``) of the ``cratedbs.cloud.crate.io``
   resource.

:``event``:
   Either ``'scale'`` or ``'upgrade'``.

:``namespace``:
   The Kubernetes namespace (``.metadata.namespace``) of the
   ``cratedbs.cloud.crate.io`` resource is deployed in.

:``scale_data``:
   When ``event`` is ``'scale'``, otherwise ``null``.:

   :``new_data_replicas``:
      An array of objects, where each object has a ``name`` and a ``replicas``
      key. The name corresponds to a node name (``.spec.nodes.data.*.name``),
      the replicas to the number of new replicas (
      ``.spec.nodes.data.*.replicas``)

   :``new_master_replicas``:
      Optional number of replicas of new master nodes.

   :``old_data_replicas``:
      An array of objects, where each object has a ``name`` and a ``replicas``
      key. The name corresponds to a node name (``.spec.nodes.data.*.name``),
      the replicas to the number of old replicas (
      ``.spec.nodes.data.*.replicas``)

   :``old_master_replicas``:
      Optional number of replicas of old master nodes.

:``status``:
   Either ``'failure'`` or ``'success'``.

:``upgrade_data``:
   When ``event`` is ``'upgrade'``, otherwise ``null``.:

   :``old_registry``:
      The old Docker image registry as defined in
      ``.spec.cluster.imageRegistry``.

   :``new_registry``:
      The new Docker image registry as defined in
      ``.spec.cluster.imageRegistry``.

   :``old_version``:
      The old image version (Docker tag) as defined in
      ``.spec.cluster.version``.

   :``new_version``:
      The new image version (Docker tag) as defined in
      ``.spec.cluster.version``.


.. _report a green health: https://crate.io/docs/crate/reference/en/latest/admin/system-information.html#health
.. _node checks: https://crate.io/docs/crate/reference/en/latest/admin/system-information.html#node-checks
