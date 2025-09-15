=========
Changelog
=========

Unreleased
----------

* Downgraded ``kopf`` to ``1.37.1`` due to compatibility issue.

2.52.0 (2025-08-26)
-------------------

* Detect tables that require re-indexing before performing a major version upgrade.

* Automatically re-create internal system tables after completing a major version
  upgrade.

* Enable AWS cross-zone load balancing

2.51.0 (2025-08-06)
-------------------

* Added ``blobs.path`` setting to the CrateDB cluster spec.

2.50.0 (2025-07-28)
-------------------

* Implemented rollback framework for CrateDB cluster operations. Introduced
  ``RollbackHandler`` and specialized subhandler ``RollbackUpgradeSubHandler``
  to support targeted rollback logic.

2.49.0 (2025-07-14)
-------------------

* Bump ``sql_exporter`` to ``0.18.0``.

* Always set ``app.kubernetes.io/managed-by`` to ``crate-operator`` to ensure the
  operator can reliably identify CrateDB pods.

2.48.0 (2025-06-10)
-------------------

* Temporarily set ``cluster.routing.allocation.enable`` to ``new_primaries`` before
  deleting each pod and reset it after the pod has restarted. This ensures more
  controlled shard allocation during rolling restarts.

* Bumped setuptools to 78.1.1 to fix CVE-2025-47273.

* Added support to restore a snapshot from a Azure storage.

* Increased rolling restart, scaling, and cluster update timeouts to 4 hours to support
  longer node shutdown durations introduced by CrateDB decommissioning logic.

2.47.1 (2025-05-14)
-------------------

* Downgraded Python version from ``3.13`` to ``3.12`` to avoid SSL certificate
  verification issues caused by stricter behavior in ``urllib3``, which is used
  indirectly by project dependencies.

2.47.0 (2025-05-12)
-------------------

* Updated Python version to ``3.13``.

* Upgraded ``kopf`` to ``1.37.5``.

* Log transient connection and timeout errors as warnings to reduce log noise.

* Upgraded ``black`` to ``25.1.0```.

2.46.0 (2025-03-17)
-------------------

* Updated Python version to ``3.11``.

* Bumped ``kopf`` to ``1.36.2`` to fix an issue where the operator stopped watching
  CrateDB resources after receiving a ``429 Too Many Requests`` response from the
  Kubernetes API.

* Add collector for translog size monitoring to the sql_exporter.

2.45.0 (2025-02-19)
-------------------

* Enabled global settings for JWT authentication from CrateDB version 5.10.1 onwards.

2.44.0 (2025-02-17)
-------------------

* Add preStop hook to the CrateDB pods to ensure that the CrateDB process is
  stopped gracefully.

* Change nginx ``proxy-body-size`` annotation value as it has stricter validations now

* Bump ``sql_exporter`` to ``0.17.0`` and fix ``cluster_last_user_activity`` NULL warning.

* Switch to ``awk`` for zone and node name extraction.

* Replaced ``rev`` in STS command with an ``awk``-based equivalent during upgrades.

* Bump ``mypy`` to ``1.13.0``.

2.43.1 (2025-01-08)
-------------------

- Fixed ``get_cluster_admin_username`` when grand-central is enabled.

2.43.0 (2024-12-18)
-------------------

* Add azure backup cronjob and metrics-exporter for clusters that are deployed
  in an azure region.

* Bump ``sql_exporter`` to ``0.16.0``

* Set CORS annotations in ``grand-central`` ingress.

* Update ``-CProccessors`` on scale compute.

* Added support for quoting schema and table names when generating the keyword
  for restoring a snapshot.

* Change sql_exporter ``min_interval`` to 60s to avoid scraping every 15s.

* Updated Python version to ``3.10``.

2.42.0 (2024-10-02)
-------------------

* Upgrade to kubernetes-asyncio 31.1.0

* Add ``minDomains`` to ``TopologySpreadConstraint`` to ensure that pods are spread across zones.

* Fixed JWT auth handlers for clusters without users.

* Clarify usage of ``CLOUD_PROVIDER`` in the documentation.

2.41.1 (2024-08-30)
-------------------

* Fixed password update for users who have ``jwt`` configured.

2.41.0 (2024-08-22)
-------------------

* Added configuration for JWT authentication from CrateDB version 5.7.2 onwards.
* Bumped version of the JMX Exporter to ``1.2.0``

2.40.2 (2024-08-01)
-------------------

* Bumped version of the JMX Exporter to ``1.1.0``

2.40.1 (2024-07-24)
-------------------

* Bumped setuptools to 70.3.0 to fix CVE-2024-6345.

* Fixed Grand Central initContainer image update.

2.40.0 (2024-07-17)
-------------------

* Added explicit security context capabilities for compatibility with cri-o.

2.39.1 (2024-07-10)
-------------------

* Increase memory for grand central, reduce CPU limit.

* Changed the ``node.attr.zone`` parameter for AWS to use IMDSv2.

2.39.0 (2024-05-22)
-------------------

* Bump ``sql_exporter`` to 0.14.2

* Set a hard limit for max TCP connections

* Use a global Kubernetes API client.

* Set an explicit ingress class for GC ingresses.

* Bumped aiohttp to latest to fix CVE-2024-30251

2.38.1 (2024-03-14)
-------------------

* Lower cpu requests for grand central.

2.38.0 (2024-03-12)
-------------------

* Bootstrap ``gc_admin`` user when installing ``grandCentral``.

2.37.0 (2024-02-26)
-------------------

* Added an init container for grand central, that explicitly waits for the associated
  CrateDB to be started.

* Changed the image pull policy for Grand Central to ``IfNotPresent``, there's no reason
  to always pull.


2.36.0 (2024-02-21)
-------------------

* Delayed cronjob re-enabling after upgrading or resuming a cluster.

* Implemented a handler allowing installing ``grandCentral`` for existing clusters.

* Fixed a bug that subhandlers were erroneously considered to be timed out.


2.35.0 (2024-02-15)
-------------------

* Updated user modification operations to leverage parameterized queries and
  ``curl``, replacing direct usage of ``crash``.

* Added ``GRAND_CENTRAL_API_URL`` envvar required for sending webhooks.

* Added the `Authorization` header to the allowed list for GC.

* Moved cluster update timeout to the handlers level.

* Add GCP as a cloud provider and fetch the zone from the node's metadata.

2.34.1 (2024-02-06)
-------------------

* Fixed compatibility with CrateDB 5.6, which returns a slightly different version of
  ``UserAlreadyExistsException`` (``RoleAlreadyExistsException``) and breaks bootstrap.

2.34.0 (2024-02-05)
-------------------

* Added ``GRAND_CENTRAL_SENTRY_DSN`` envvar to allow specifying the sentry dsn for
  grand central deployments.

* Bumped aiohttp to latest to fix CVE-2023-49081

* Added ``grandCentral`` section to the CRD and create the resources for grand-central
  backend when a cluster is deployed.

* Implemented a handler allowing changing the ``backendImage`` of ``grandCentral``.

* Added the Prometheus annotations to ``grandCentral`` to allow metrics scrapping on it.

2.33.0 (2023-11-14)
-------------------

* Changed the operator CRD to be able to specify a nodepool and set node affinity and
  tolerations accordingly when creating a cluster or changing its compute.

2.32.0 (2023-11-09)
-------------------

* Updated the ``CrateVersion`` nightly parsing to accept the new datetime format
  of ``yyyy-mm-dd-hh-mm`` while still being compatible with the old ``yyyymmdd`` format.

2.31.0 (2023-09-11)
-------------------

* Added support for performing different types of restore operations, e.g. only
  metadata, users or tables.

* Increased the timeout for querying ``sys.snapshots`` table when verifying backup
  repository.

* Explicitly lowering TCP keepalives to 120s to deal with naughty load balancers.
  Looking at you, AWS NLB.

2.30.3 (2023-08-29)
-------------------

* Fixed a bug that made cloning/restoring an empty partitioned table report a failure
  regardless of whether it succeeded or not.

2.30.2 (2023-08-10)
-------------------

* Fixed a bug that lead to the namespace not being deleted after deleting a cluster
  that had a snapshot restore/clone operation in progress.

2.30.1 (2023-07-06)
-------------------

* Bump sql_exporter to 0.11.1

* Fixed patching of sql exporter configmap.

2.30.0 (2023-06-27)
-------------------

* Changed the metrics to also export the cluster name as a label.

* Print exception details if an exception happens in a sub handler.

2.29.0 (2023-06-07)
-------------------

* Changed the metrics to also export the namespace as a label.

* Fixed a bug that prevented snapshots from being restored.

2.28.0 (2023-06-05)
-------------------

* Increased ``max_restore_bytes_per_sec`` when creating a repository for a backup restore operation.

* Added ``cratedb_unreplicated_tables`` metric to the sql exporter.

2.27.0 (2023-05-08)
-------------------

* Upgraded sql_exporter with arm64 support

* Suspending a cluster now deletes the load balancer.
  Resuming the cluster re-creates it.

* Fixed PVC resize tests. They were impacted by the fact that we're not deleting the load balancer.

2.26.1 (2023-04-12)
-------------------

* Added a build of the linux/arm64 platform when pushing to docker hub.

2.26.0 (2023-04-05)
-------------------

* Added the type of operation to the feedback webhooks payload.

* Removed handling of licenses. The operator will no longer attempt to set a license,
  even if one is configured in the CRD. Licenses are deprecated since CrateDB 4.5.

2.25.0 (2023-03-23)
-------------------

* Added an ability to specify additional annotations for the created LoadBalancer
  services. This is useful when, i.e., wanting to override the type of load balancer
  to be used.

* Ignore failed chown operation on AWS efs volumes.

2.24.0 (2023-03-21)
-------------------

* Remove ``beta1`` from `PodDisruptionBudget` and ``Cronjob/Batch`` API version.

* Fixed a missing permission that was causing a warning on kopf startup.

* Updated CRD to show the CPU requests and limits.

2.23.0 (2023-02-28)
-------------------

* Do not perform cluster pre-flights checks when expanding disk.

* Fix failing operator tests.

* Include ``sys.cluster`` for checking cluster healthiness.

2.22.0 (2023-01-31)
-------------------

* Cluster cloning now restores the original admin username in CrateDB CRD.

* Added a check if all shards have been restored completely after a ``restore snapshot``
  operation.

2.21.0 (2023-01-09)
-------------------

* Fixed a bug that lead to sending false succeed webhooks when updating an admin password.

* sql_exporter 0.9.2 has been released.

* Downgrade to busybox 1.35.0 for a few containers. Apparently 1.36 was erroneously marked
  as 'latest' whereas it is unstable -> https://github.com/docker-library/busybox/issues/162

2.20.0 (2022-12-15)
-------------------

* Added support to restore a snapshot from a backup repository.

2.19.0 (2022-11-29)
-------------------

* Change the value of ``when_unsatisfiable`` in the ``TopologySpreadConstraint`` to
  ``DoNotSchedule``, this seems to work now. Tested on kubernetes `1.22.12`.

2.18.0 (2022-11-24)
-------------------

* Cluster backup cronjob schedules can now be updated.

* Fixed the way user passwords are updated to not require the old password anymore.

2.17.0 (2022-10-31)
-------------------

* Added support for expanding volumes online (without suspending the cluster).
  This is controlled by the ``NO_DOWNTIME_STORAGE_EXPANSION`` config option
  and defaults to false. The feature must be supported by the underlying infrastructure,
  i.e. Azure AKS or AWS EKS supports it using CSI drivers.

* Disabled parallel cluster suspension and volume resizing. This was causing issues on
  Azure AKS. Will now first suspend the cluster and only then attempt to resize volumes.

2.16.0 (2022-10-17)
-------------------

* Added cratedb_cluster_last_user_activity metric to the sql exporter

* Fixed success notifications being sent too soon for update operations.

2.15.0 (2022-09-28)
-------------------

* Added support for parallel testing, which greatly reduces test runtime.

* Change AWS Loadbalancer to type NLB instead of CLASSIC.

* Added Helm Chart and ``Helm Chart Releaser`` GitHub action.

* Added the ``-A`` option (all-namespaces) to the operator run command in the Dockerfile.
  This fixes a warning that the operator prints when starting.

* Removed the testing load balancer. We didn't actually need it for testing, and
  it was using up another external IP, which are in short supply.

* Added reporting of cluster's health to the status field in the CRD. This allows us to
  print the status as part of ``kubectl get cratedbs``.

* Fixed an issue that might result in CronJobs not being re-enabled after suspension.

2.14.0 (2022-09-13)
-------------------

* Fixed a bug that would prematurely send a notification about the success of updating
  the user's password.

* Added support to change cpu, memory and heap ratio on running clusters.

* LICENSE CHANGE: Moving from AGPL to Apache 2.0, to be in-line with our other open-source
  projects.

2.13.3 (2022-07-12)
-------------------

* Fix a bug that would cause suspending a cluster to get stuck.

2.13.2 (2022-07-11)
-------------------

* Scale backup-metrics deployment down/up when suspending/resuming a cluster.

2.13.1 (2022-07-04)
-------------------

* Fix a bug that would lead to the operator getting stuck when performing repeated
  operations (i.e. suspend/resume/suspend/resume/...)

2.13.0 (2022-06-21)
-------------------

* Change the value of ``when_unsatisfiable`` in the ``TopologySpreadConstraint`` to
  ``ScheduleAnyway`` to be able to deploy a cluster with more than 3 nodes again.

* Eliminated the minimum of 1 replica data nodes to allow suspending clusters.

* Clusters can now be suspended (replicas set to 0, keeping the storage) and resumed.

* Switch to the better maintained burningalchemist/sql_exporter.

2.12.0 (2022-05-03)
-------------------

* Changed the operator CRD to be able to specify resource requests and limits
  separately.

* Update cratedbs CRD for Kubernetes 1.22 API changes.

2.11.0 (2022-04-07)
-------------------

* Removed two no-longer required migration handlers - these have been around for some
  time.

* Changed the ``crate-discovery`` internal service to be headless - there is no reason
  at all for it to be load balanced by k8s.

* Added subhandlers allowing to expand volume size on existing CrateDB clusters.

2.10.0 (2022-02-17)
-------------------

* Added status update notifications for cluster creation and updates of the
  allowed CIDRs and user password secrets.

* Changed ``imagePullPolicy`` on container init scripts to not always pull busybox
  and similar images. This is wasteful in light of the new docker hub limits.

2.9.0 (2022-01-27)
------------------

* Added status update notifications during a cluster scaling operation.

2.8.0 (2021-12-29)
------------------

* Replaced kopf timeout handling with a decorator ``@crate.timeout()`` to be
  able to run code when a timeout happens.

* Added a decorator ``@crate.on.error()`` which catches timeouts as well as
  other permanent handler errors and performs actions passed in an error
  handler, like sending a notification.

* Fixed the issue that notifications of successful upgrades pile up in the
  status of the CrateDB resource if an upgrade succeeds but the subsequent
  restart fails or times out. These notifications were erroneously sent in the
  next run of the handler.

* Changed the registration of all kopf subhandlers in the creation process
  to use StateBasedSubhandler.

* Renamed webhook event ``error`` to ``feedback`` and added more status updates
  during a cluster upgrade.

* Added timeouts to ``create`` and ``update`` handlers.

2.7.2 (2021-12-10)
------------------

* Added mitigation for log4j vulnerability

2.7.1 (2021-11-12)
------------------

* Changed how the metrics are reported so that they disappear if a cluster is deleted.

2.7.0 (2021-11-09)
------------------

* Upgraded to the latest version of kopf (1.35.1)

* Added a Prometheus endpoint, enabling some metrics in the operator to be scraped.
  Namely, this exposes information from the ping handler, which checks if the running
  clusters are reachable and healthy.

2.6.0 (2021-10-27)
------------------

* Added a kopf timer function that retrieves the cluster health for all CrateDB clusters
  the operator knows off and sends the corresponding notification.

* Changed the operator to use the internal ``discovery`` service for all operations
  on the cluster, because the public ``crate`` service might be IP-restricted.

* Changed the usage of ``yaml.load()`` to specify the Loader parameter, which is now
  required from PyYAML 6.0.

* Changed the debug volume to be provisioned in the same way as the data volume is,
   which ensures better compatibility with different k8s providers.

2.5.0 (2021-10-12)
------------------

* Changed the operator CRD to print additional information about the running CrateDBs:
  the cluster name, version and number of data nodes.

* Added an annotation for AWS ELB load balancers running on EKS to up the idle
  connection timeout to 1 hour. Without this, connections with long-running queries
  were being killed by the ELB.

* Changed the operator CRD to be able add allowed IPs (CIDR notation) to the CrateDB clusters.

* Added ``loadBalancerSourceIPRanges`` for crate service to allow IP Whitelisting.

* Use settings names ``gateway.recover_after_data_nodes`` and
  ``gateway.expected_data_nodes`` instead of ``gateway.recover_after_nodes`` and
  ``gateway.expected_nodes`` from CrateDB version 4.7 onwards.

* Implemented a handler allowing changing ``allowedCIDRs`` on CrateDB resources.

* Added ``BOOTSTRAP_RETRY_DELAY`` and ``HEALTH_CHECK_RETRY_DELAY`` settings that allow
  adjusting the respective delays in the bootstrap process.

2.4.0 (2021-08-26)
------------------

* Add additional environment variable to use a custom S3 backup ``endpointUrl``.

2.3.0 (2021-07-26)
------------------

* Added update of ``cluster.routing.allocation.enable`` setting to ``new_primaries``
  before performing scaling/upgrades/restarts in order to disable shard allocations
  during that time. Once the update is finished the setting is reset.

* Replace AntiAffinity Rule with topologySpreadConstraints

* Fixed a problem with reporting the load balancer ip (hostname) for AWS EKS.
  EKS gives load balancers hostnames and not IPs. We treat these as one and the same.

2.2.0 (2021-06-23)
------------------

* Added a new kopf handler that watches for services getting external IPs
  (i.e. Load Balancers) and sending a webhook back with that info.

* Fix tests that did not catch the async TimeoutError that aiopg started using
  following a dependabot-triggered update.

* Added an ability to throw exceptions from webhooks, for handlers that require it.

2.1.0 (2021-04-28)
------------------

* Send a notification if a snapshot / backup is in progress while attempting a
  cluster update.

2.0.0 (2021-04-15)
------------------

* Removed the deprecated ``zalando...`` annotations. This will require a 2.0 release.

* Added PodDisruptionBudget to keep a cratedb statefulset up during kubernetes upgrades.

* Added a check for any running snapshots (either k8s jobs or CREATE SNAPSHOT stmts.)
  before performing scaling/upgrades/restarts. This ensures we don't inadvertently
  interfere with an existing snapshot operation

* Fixed a bug that caused us not to wait for a cluster to be healthy when performing
  scaling operations (due to a missing await).

* Refactored some of the tests, specifically reusing repetitive operations.

* Removed handling of master & cold replicas from integration tests as these are not
  used in practice.

* Changed how (sub)handlers are treated to allow returning statuses, which get persisted
  against the CrateDB resource in k8s.

* Changed cluster updates to disable any backup cronjobs, so that a job doesn't
  kick in just as we are performing a cluster update. The job will be re-enabled
  once the update is complete.

* Completely refactored cluster updates to not use the state machine any more,
  but rather added an ability to specify dependencies between handlers.

* Removed the Context class in favour of simple storing the context as a dictionary.

1.2.0 (2021-03-22)
------------------

* Changed the external traffic policy to local. This allows seeing the actual IP of
  the client that is connecting to CrateDB.

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
