=========
Changelog
=========

Unreleased
----------

* Fixed the notifications, which were broken for some time due to a missing 'await'

1.1.0 (2021-03-02)
------------------

* Added max-shards-per-node metric to the sql exporter

1.0.2 (2021-02-01)
__________________

* Bumped version of the JMX Exporter to ``1.0.0``

* Modified the tests to not use a custom storageclass anymore, which was causing
  issues.

1.0.1 (2021-01-26)
------------------

* Removed username validation from the custom resource definition.
  Since CrateDB accepts every string as a username, we also don't want
  to validate the username in the crate-operator.

1.0 (2020-12-03)
----------------

* Made ``CLUSTER_BACKUP_IMAGE`` configuration parameter optional to remove
  dependency on external Docker image.

* Will now pass the ``WEBHOOK_URL`` and credentials to the created backup cronjob.

* Watch on Kubernetes Secrets that have the
  ``operator.cloud.crate.io/user-password`` label assigned and update the users
  of all CrateDB resources in the same namespace if the password changed.

* Fixed an inconsistent behavior where the configuration option
  :envvar:`CLOUD_PROVIDER` would override an explicitly defined
  ``node.attr.zone`` in either ``.spec.cluster.settings``,
  ``.spec.nodes.master.settings``, or ``.spec.nodes.data.*.settings``.

* To allow CrateDB user password updates, Kubernetes Secrets referenced in the
  ``.spec.users`` section of a CrateDB custom resource, will have a
  ``operator.cloud.crate.io/user-password`` label applied.

* Changed the pod spreading on Azure to use the underlying Azure zone instead of
  the fault/failure domain.

* Fixed configuration parsing of the :envvar:`KUBECONFIG` environment variable.

* Fixed a bug in the CrateDB CustomResourceDefinition which would prevent
  annotations, labels, or settings in the node or cluster specs to be
  preserved.

* Renamed the ``kopf.zalando.org/last-handled-configuration`` annotation, which
  Kopf uses to track changes, to ``operator.cloud.crate.io/last``.

* Renamed the prefix for the progress tracking annotations from
  ``kopf.zalando.org`` to ``operator.cloud.crate.io``.

* Renamed the custom resource finalizer from
  ``kopf.zalando.org/KopfFinalizerMarker`` to
  ``operator.cloud.crate.io/finalizer``.

* Fixed parsing of replicas. Previously, in a replica settings like ``'2-5'``
  or ``'2-all'``, the upper bound was used. This effectively made scale-down
  operations impossible, at least for the ``'all'`` case. However, a table and
  with that a cluster is healthy when the minimum number of replicas is
  available, which is indicated by the lower bound.

* Fixed a bug that would prevent the version of the Docker image of the
  ``mkdir-heapdump`` init container to be updated when a cluster is upgraded.


1.0b4 (2020-11-03)
------------------

* Set timeouts for event watching in the underlying Kopf framework to prevent
  the operator from getting stuck.

* Support Pod spreading across zones on Azure using weighted Pod
  affinity on ``failure-domain.beta.kubernetes.io/zone`` topology. See also
  https://kubernetes.io/docs/reference/kubernetes-api/labels-annotations-taints/#failure-domainbetakubernetesiozone

  CrateDB nodes are also aware of this topology thought the ``zone`` node
  attribute.

* Ensured that Kubernetes API client's connections are closed properly.

1.0b3 (2020-08-11)
------------------

* Set the configured log level for all loggers. This ensures that even with
  Kopf's ``--debug`` or ``--verbose`` CLI flags, Kubernetes API responses are
  not logged anymore when the log level is ``INFO`` or higher. This is to avoid
  leaking secrets into the operator log when it e.g. reads Kubernetes secrets.

1.0b2 (2020-07-16)
------------------

* Set the idle timeout of Service loadbalancer to cloud provider specific
  maximum.

* Fixed a bug that prevented the cluster name from ``.spec.cluster.name`` to be
  used as CrateDB's cluster name.

* Fixed broken creation of StatefulSets when ``CLOUD_PROVIDER`` was set to
  ``aws`` due to missing ``topology_key`` in Pod affinity declaration.

* Added the changelog to the documentation.

1.0b1 (2020-07-07)
------------------

* Initial release of the *CrateDB Kubernetes Operator*.
