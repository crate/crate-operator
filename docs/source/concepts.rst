Concepts
========

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

Cluster Restart
---------------

Some situations, such as version upgrades, require a cluster restart. Most
times a rolling restart is sufficient. When instructed to do so, the *CrateDB
Kubernetes Operator* will perform a rolling cluster restart following this
process:

1. If there are master nodes, restart each of them in ordinal order.

1. For each data node definition, in the order they are defined in the spec,
   restart each of its nodes in ordinal order.

Restarting a CrateDB node works by deleting the corresponding Kubernetes pod.
The operator will then wait for a new pod to go down and another one to come up
and join the CrateDB cluster. Lastly, the cluster needs to `report a green
health`_ three times in a row with a 30 second wait time in between, for a pod
to be considered back in service.

.. _report a green health: https://crate.io/docs/crate/reference/en/latest/admin/system-information.html#health
