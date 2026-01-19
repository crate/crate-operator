Configuration
=============

The configuration for the CrateDB Kubernetes Operator follows the `12 Factor
Principles`_ and uses environment variables. All environment variables are
expected to use upper case letters and must be prefixed with
``CRATEDB_OPERATOR_``.

.. envvar:: BOOTSTRAP_TIMEOUT

   When deploying a cluster, the operator will perform some bootstrap tasks as
   documented in :ref:`the Concepts section <concept-bootstrapping>`. The
   operator will wait at most this many seconds until it considers the
   bootstrapping to have failed. Set to ``0`` to disable timeouts.

   The default value is ``1800`` seconds.

.. envvar:: CLOUD_PROVIDER

   Some cloud providers require a specific setup for CrateDB nodes, due to the
   availability guarantees of the underlying infrastructure. For example, in
   AWS, a block storage used by CrateDB as a data partition is only available
   in one availability zone (AZ) of that corresponding AWS region. If all
   copies of some shard were located in the same AZ, an outage of that AZ would
   imply some data being unavailable in CrateDB.
   This variable is optional. If it is set, make sure that the underlying
   Kubernetes Nodes are configured with availability zones.
   The operator will then use this information to ensure that CrateDB nodes are
   spread across different availability zones. This is done by setting the
   ``node.attr.zone`` attribute on CrateDB nodes.
   The operator also sets annotations on the Kubernetes Loadbalancers, based on
   ``CLOUD_PROVIDER``.

   To ensure CrateDB properly replicates shards to other nodes in different
   availability zones, one can make use of CrateDB's :ref:`routing allocation
   awareness <cratedb:conf-routing-allocation-awareness>`. For example,
   deploying a cluster in AWS' ``eu-central-1`` region:

   .. code-block:: yaml

      kind: CrateDB
      spec:
        cluster:
          settings:
            cluster.routing.allocation.awareness.attributes: "zone"
            cluster.routing.allocation.awareness.force.zone.values: "eu-central-1a,eu-central-1b,eu-central-1c"

   Allowed values:

   - ``aws``
   - ``azure``
   - ``gcp``
   - ``openshift`` (Red Hat OpenShift Container Platform)

   When set to ``openshift``, the operator will:

   - Use a sidecar container (``crate-control``) for SQL execution instead of
     ``pod_exec`` to comply with OpenShift's security policies
   - Create OpenShift-specific SecurityContextConstraints and ServiceAccounts
   - Skip privileged init containers
   - Adjust pod security contexts to allow CrateDB to run with required capabilities

   Under the hood, the operator will pass a ``zone`` attribute to all CrateDB
   nodes. This attribute can also be defined explicitly or override the one set
   by the operator. To do this on a cluster level, set ``.spec.cluster.settings``:

   .. code-block:: yaml

      kind: CrateDB
      spec:
        cluster:
          settings:
            node.attr.zone: "some-value"

   To set or override the attribute on a node type level, set it in
   ``.spec.nodes.master.settings`` or ``.spec.nodes.data.*.settings``.

.. envvar:: CRATE_CONTROL_IMAGE

   When running on OpenShift (``CLOUD_PROVIDER=openshift``), this variable
   specifies the Docker image for the ``crate-control`` sidecar container.
   The sidecar provides an HTTP endpoint for SQL execution, replacing the
   ``pod_exec`` approach which is not allowed by OpenShift's security policies.

   This variable is **required** when ``CLOUD_PROVIDER`` is set to ``openshift``.

   Example value: ``your-registry.example.com/crate-control:latest``

   The crate-control image can be built from the ``sidecars/cratecontrol/``
   directory in the operator repository.

.. envvar:: CLUSTER_BACKUP_IMAGE

   When enabling backups for a cluster, the operator deploys a Prometheus_
   exporter to be scraped for backup metrics, and a Kubernetes CronJob that
   creates backups every defined interval. If :envvar:`WEBHOOK_URL` and
   related credentials are specified, the backup CronJob will post backup
   creation events back to the webhook URL. This variable needs to point to a
   Docker image *and* tag to use it for the exporter and CronJob.

.. envvar:: DEBUG_VOLUME_SIZE

   The volume size for the ``PersistentVolume`` that is used as a storage
   location for Java heap dumps.

   The default value is ``256 GiB``.

.. envvar:: DEBUG_VOLUME_STORAGE_CLASS

   The Kubernetes storage class name for the ``PersistentVolume`` that is
   used as a storage location for Java heap dumps.

   The default value is ``crate-local``.

.. envvar:: IMAGE_PULL_SECRETS

   A comma-separated list of Kubernetes image pull secrets. Each Kubernetes
   resource created by the operator will have all these secrets attached.

   The default value is an empty list.

.. envvar:: JMX_EXPORTER_VERSION

   (**Required**)

   CrateDB exports metrics via the JMX protocol. This is the version of the
   exporter to be used.

.. envvar:: KUBECONFIG

   If defined, it needs to point to a valid Kubernetes configuration file. Due
   to the underlying libraries, multiple paths, such as
   ``/path/to/kube.conf:/another/path.conf``, are not allowed. For
   compatibility and ease of use, if ``CRATEDB_OPERATOR_KUBECONFIG`` is not
   defined, the operator will also look for the ``KUBECONFIG`` environment
   variable. Default is ``None`` and leads to "in-cluster" configuration.

.. envvar:: LOG_LEVEL

   The log level used for log messages emitted by the CrateDB Kubernetes
   Operator. Valid values are ``CRITICAL``, ``ERROR``, ``WARNING``, ``INFO``,
   or ``DEBUG``.

   The default value is ``INFO``.

.. envvar:: ROLLING_RESTART_TIMEOUT

   A rolling cluster restart takes some time, depending on the cluster size,
   number of nodes, amount of data, etc. After some change operations, such as
   cluster upgrades, the operator will trigger a rolling cluster restart. The
   operator will wait at most this many seconds until it considers the rolling
   restart to have failed. Set to ``0`` to disable timeouts.

   The default value is ``3600`` seconds.

.. envvar:: SCALING_TIMEOUT

   When scaling a cluster, the operator will sometimes need to deallocate some
   CrateDB nodes before turning them off. To ensure the operator keeps
   functioning on the resource, scaling operations will be aborted after this
   many seconds and will be considered to have failed. Set to ``0`` to disable
   timeouts.

   The default value is ``3600`` seconds.

.. envvar:: TESTING

   During development or testing, some constraints enforced by the operator may
   be obstructive. One such example is the Kubernetes pod anti-affinity on all
   CrateDB pods, which guarantees that a single Kubernetes node failure doesn't
   take down several CrateDB nodes. This makes deploying a CrateDB cluster that
   has explicit master nodes impossible on a 3-node Kubernetes cluster, because
   there would be 3 master + *n* data nodes.

   Setting this to ``True`` will remove the constraint.

   .. danger::

      Do **not** set this variable when running the operator in production! It
      *will* impact the reliability of your CrateDB clusters!

   The default value is ``False``.

.. envvar:: WEBHOOK_PASSWORD

   Any webhook request submitted by the operator will include :rfc:`HTTP Basic
   Auth <7617>` credentials. This is the password.

   The default value is ``None``.

.. envvar:: WEBHOOK_URL

   The operator can optionally be configured to submit HTTP POST requests to an
   API upon certain events (see :ref:`concept-webhooks`). For that to work, the
   :envvar:`WEBHOOK_PASSWORD`, :envvar:`WEBHOOK_URL`, and
   :envvar:`WEBHOOK_USERNAME` need to be set.

   The default value is ``None``.

.. envvar:: WEBHOOK_USERNAME

   Any webhook request submitted by the operator will include :rfc:`HTTP Basic
   Auth <7617>` credentials. This is the username.

   The default value is ``None``.

.. envvar:: NO_DOWNTIME_STORAGE_EXPANSION

   Whether to perform volume expansion operations without suspending the cluster.
   For this to work, it must be supported by the underlying infrastructure. At the time
   of writing, this works on Azure AKS and AWS EKS if using the CSI drivers.

   By default, the operator will suspend the cluster while performing volume expansion,
   and resume it once the PVCs expand.

   The default value is ``False``.


.. _12 Factor Principles: https://12factor.net/
.. _Prometheus: https://prometheus.io/
