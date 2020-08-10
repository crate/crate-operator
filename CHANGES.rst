=========
Changelog
=========

Unreleased
----------

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
