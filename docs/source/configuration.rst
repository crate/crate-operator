Configuration
=============

The configuration for the *CrateDB Kubernetes Operator* follows the `12 Factor
Principles`_ and uses environment variables. All environment variables are
expected to be used in upper-case letters and must be prefixed with
``CRATEDB_OPERATOR_``.

:``BOOTSTRAP_TIMEOUT``:
   When deploying a cluster, the operator will perform some bootstrap tasks as
   documented in :ref:`the concept section <concept-bootstrapping>`. The
   operator will wait at most this many seconds until it considers the
   bootstrapping to have failed. Set to ``0`` to disable timeouts.

   The default value is ``1800`` seconds.

:``CLUSTER_BACKUP_IMAGE``:
   (**Required**)

   When enabling backups for a cluster, the operator deploys a Prometheus_
   exporter to be scraped for backup metrics, and a Kubernetes CronJob that
   creates backups every defined interval. This variable needs to point to a
   Docker image *and* tag to use it for both.

:``DEBUG_VOLUME_SIZE``:
   The volume size for the ``PersistentVolume`` that is used as a storage
   location for Java heap dumps.

   The default value is ``256 GiB``.

:``DEBUG_VOLUME_STORAGE_CLASS``:
   The Kubernetes storage class name for the ``PersistentVolume`` that is
   used as a storage location for Java heap dumps.

   The default value is ``crate-local``.

:``IMAGE_PULL_SECRETS``:
   A comma separated list of Kubernetes image pull secrets. Each Kubernetes
   resource created by the operator will have all these secrets attached.

   The default value is an empty list.

:``JMX_EXPORTER_VERSION``:
   (**Required**)

   CrateDB exports metrics via the JMX protocol. This is the version of the
   exporter to be used.

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
   or ``DEBUG``.

   The default value is ``INFO``.

:``ROLLING_RESTART_TIMEOUT``:
   A rolling cluster restart takes some time, heavily depending on the cluster
   size, its number of nodes, amount of data, etc. After some change
   operations, such as cluster upgrades, the operator will trigger a rolling
   cluster restart. The operator will wait at most this many seconds until it
   considers the rolling restart to have failed. Set to ``0`` to disable
   timeouts.

   The default value is ``3600`` seconds.

:``TESTING``:
   During development or testing, some contraints enforced by the operator may
   be obstructive. An example for that can be the Kubernetes pod anti-affinity
   on all CrateDB pods, that guarantees that a single Kubernetes node failure
   doesn't take down several CrateDB nodes. As a result, deploying a CrateDB
   cluster that has explicit master nodes won't be possible on a 3-node
   Kubernetes cluster, because there would be 3 master + *n* data nodes.
   Setting this to ``True`` will remove the contraint.

   .. danger::

      Do **not** set this when running the operator in production! It *will*
      impact the reliability of your CrateDB clusters!

   The default value is ``False``.


.. _12 Factor Principles: https://12factor.net/
.. _Prometheus: https://prometheus.io/
