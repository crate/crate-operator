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
