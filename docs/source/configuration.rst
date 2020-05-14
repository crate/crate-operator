Configuration
=============

The configuration for the *CrateDB Kubernetes Operator* follows the `12 Factor
Principles`_ and uses environment variables. All environment variables are
expected to be used in upper-case letters and must be prefixed with
``CRATEDB_OPERATOR_``.

:``KUBECONFIG``:
   If defined, needs to point to a valid Kubernetes configuration file. Due to
   the underlying libraries, multiple paths, such as
   ``/path/to/kube.conf:/another/path.conf`` are not allowed. For compatibility
   and ease of use, if ``CRATEDB_OPERATOR_KUBECONFIG`` is not defined, the
   operator will also look for the ``KUBECONFIG`` environment variable. Default
   is ``None`` and leads to "in-cluster" configuration.

:``LOG_LEVEL``:
   The log level used for log messages emitted by the CrateDB Kubernetes
   Operator. Valid values are ``CRITICAL``, ``ERROR``, ``WARNING``, ``INFO``,
   or ``DEBUG``. The default value is ``INFO``.


.. _12 Factor Principles: https://12factor.net/
