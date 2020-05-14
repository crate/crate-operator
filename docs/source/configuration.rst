Configuration
=============

The configuration for the *CrateDB Kubernetes Operator* follows the `12 Factor
Principles`_ and uses environment variables. All environment variables are
expected to be used in upper-case letters and must be prefixed with
``CRATEDB_OPERATOR_``.

:``LOG_LEVEL``:
   The log level used for log messages emitted by the CrateDB Kubernetes
   Operator. Valid values are ``CRITICAL``, ``ERROR``, ``WARNING``, ``INFO``,
   or ``DEBUG``. The default value is ``INFO``.


.. _12 Factor Principles: https://12factor.net/
