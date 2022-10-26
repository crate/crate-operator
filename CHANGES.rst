=========
Changelog
=========

Unreleased
----------

* Added support for expanding volumes online (without suspending the cluster).
  This is controlled by the ``NO_DOWNTIME_STORAGE_EXPANSION`` config option
  and defaults to false. The feature must be supported by the underlying infrastructure,
  i.e. Azure AKS or AWS EKS supports it using CSI drivers.

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
