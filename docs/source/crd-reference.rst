.. _crd-reference:

======================
CrateDB CRD reference
======================

The CrateDB Kubernetes Operator is driven by a single `CustomResourceDefinition
<https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/>`_
(CRD) named ``cratedbs.cloud.crate.io``. Creating, updating or deleting a
``CrateDB`` object of that kind is how you tell the operator what cluster you
want.

This page documents every field of the ``CrateDB`` resource. The field
descriptions follow the ``openAPIV3Schema`` embedded in the `CRD definition`_.
The schema descriptions are also available directly from a live cluster via
``kubectl explain``, for example::

    kubectl explain cratedb.spec.nodes.data

.. contents::
   :local:
   :depth: 2


Overview
========

Each ``CrateDB`` object describes exactly one CrateDB cluster.

============  ========================
Property      Value
============  ========================
Group         ``cloud.crate.io``
Version       ``v1``
Kind          ``CrateDB``
Plural        ``cratedbs``
Singular      ``cratedb``
Scope         ``Namespaced``
============  ========================

A minimal manifest looks like this:

.. code-block:: yaml

   apiVersion: cloud.crate.io/v1
   kind: CrateDB
   metadata:
     name: my-cluster
     namespace: my-namespace
   spec:
     cluster:
       imageRegistry: crate
       name: my-cluster
       version: 6.4.4
     nodes:
       data:
         - name: hot
           replicas: 3
           resources:
             heapRatio: 0.25
             disk:
               count: 1
               size: 100GiB
               storageClass: default
             limits:
               cpu: 2
               memory: 4Gi
             requests:
               cpu: 2
               memory: 4Gi

.. admonition:: Before you apply
   :class: important

   A few values in the example depend on your own cluster — adjust them before
   applying so the pods can schedule:

   * **Namespace** — the target namespace must already exist and be one the
     operator watches. Create it first, e.g. ``kubectl create namespace
     my-namespace``.
   * **Storage class** — ``storageClass`` must name a StorageClass that exists
     on your cluster; the ``default`` above is only a placeholder. List the
     available classes with ``kubectl get storageclass`` and use one of them.
     Make sure the operator's own storage-class setting (the
     ``CRATEDB_OPERATOR_DEBUG_VOLUME_STORAGE_CLASS`` value it was installed
     with) also points at a class that exists on your cluster.
   * **Replicas and nodes** — the operator spreads the nodes of a cluster across
     hosts (one CrateDB pod per node), so ``replicas: 3`` needs at least three
     schedulable worker nodes. On a small test cluster, lower ``replicas`` to
     match the number of nodes you have.
   * **Resources** — size ``requests``/``limits`` to fit your nodes; the values
     above are only an example.

The top-level ``spec`` object has the following members. Only ``cluster`` and
``nodes`` are required.

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Required
     - Description
   * - :ref:`spec.cluster <crd-cluster>`
     - **yes**
     - Core cluster identity and configuration.
   * - :ref:`spec.nodes <crd-nodes>`
     - **yes**
     - The data (and optional dedicated master) nodes making up the cluster.
   * - :ref:`spec.backups <crd-backups>`
     - no
     - Scheduled snapshot backups to AWS S3 or Azure Blob Storage.
   * - :ref:`spec.grandCentral <crd-grandcentral>`
     - no
     - Grand Central backend deployment for the cluster.
   * - :ref:`spec.ports <crd-ports>`
     - no
     - Port numbers exposed by the cluster.
   * - :ref:`spec.users <crd-users>`
     - no
     - CrateDB users to create and manage.


.. _crd-secretkeyref:

Referencing Kubernetes Secrets
==============================

Many fields do not take a literal value but instead reference a key inside a
Kubernetes `Secret
<https://kubernetes.io/docs/concepts/configuration/secret/>`_. Wherever this
reference documentation says a field is a **secret reference**, it has the
following shape:

.. code-block:: yaml

   someField:
     secretKeyRef:
       name: <name-of-the-kubernetes-secret>
       key: <key-within-that-secret>

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Required
     - Description
   * - ``secretKeyRef.name``
     - **yes**
     - Name of a Kubernetes Secret in the same namespace.
   * - ``secretKeyRef.key``
     - **yes**
     - The key within that Secret whose value should be used.

This indirection keeps sensitive values (credentials, bucket names, license
data) out of the ``CrateDB`` object itself.


.. _crd-cluster:

``spec.cluster``
================

Core identity and cluster-wide configuration. **Required.**

.. list-table::
   :header-rows: 1
   :widths: 24 14 12 50

   * - Field
     - Type
     - Required
     - Description
   * - ``name``
     - string
     - **yes**
     - Name of the cluster. Must match ``^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$``.
   * - ``version``
     - string
     - **yes**
     - CrateDB version to run.
   * - ``imageRegistry``
     - string
     - **yes**
     - Docker registry for the CrateDB image. For the official image this is
       ``crate``; testing and nightly releases live under ``crate/crate``;
       others under a full registry URL such as
       ``https://example.com/path/to/registry``.
   * - ``allowedCIDRs``
     - list of string
     - no
     - Whitelisted CIDRs allowed to reach the cluster.
   * - ``externalDNS``
     - string
     - no\*
     - The external DNS name record that should point to the CrateDB cluster.
       \*Required when ``exposure`` is ``traefik``.
   * - ``exposure``
     - string
     - no
     - Service exposure type. One of ``loadbalancer`` or ``traefik``.
   * - ``settings``
     - object
     - no
     - Additional CrateDB settings applied to all nodes in the cluster.
       Free-form (unknown keys are preserved).
   * - ``license``
     - object
     - no
     - **Deprecated.** CrateDB no longer requires a license, so this field can
       be omitted. Retained for backwards compatibility as a secret reference
       under ``license.secretKeyRef``.
   * - ``service.annotations``
     - object
     - no
     - Additional annotations to add to the Kubernetes load balancer service.
       Free-form (unknown keys are preserved).
   * - ``ssl``
     - object
     - no
     - TLS keystore configuration; see :ref:`crd-cluster-ssl`.
   * - ``restoreSnapshot``
     - object
     - no
     - Restore data from an existing snapshot on cluster creation; see
       :ref:`crd-cluster-restoresnapshot`.


.. _crd-cluster-ssl:

``spec.cluster.ssl``
--------------------

Configures TLS for the cluster from a Java keystore. When present, all three
members are required and each is a :ref:`secret reference <crd-secretkeyref>`.

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Field
     - Required
     - Description
   * - ``keystore``
     - **yes**
     - Secret reference to the CrateDB SSL keystore.
   * - ``keystorePassword``
     - **yes**
     - Secret reference to the keystore password.
   * - ``keystoreKeyPassword``
     - **yes**
     - Secret reference to the keystore key password.


.. _crd-cluster-restoresnapshot:

``spec.cluster.restoreSnapshot``
--------------------------------

Restores data from a snapshot when the cluster is created. Credentials and
repository coordinates are provided as :ref:`secret references
<crd-secretkeyref>`. ``snapshot`` and ``basePath`` are required.

.. list-table::
   :header-rows: 1
   :widths: 26 14 60

   * - Field
     - Required
     - Description
   * - ``snapshot``
     - **yes**
     - The name of the snapshot to restore.
   * - ``basePath``
     - **yes**
     - Secret reference to the base path of the repository.
   * - ``backupProvider``
     - no
     - Storage provider holding the snapshot. One of ``aws`` or ``azure_blob``.
   * - ``bucket``
     - no
     - *(AWS)* Secret reference to the snapshot's AWS S3 bucket name.
   * - ``accessKeyId``
     - no
     - *(AWS)* Secret reference to the AWS S3 Access Key ID.
   * - ``secretAccessKey``
     - no
     - *(AWS)* Secret reference to the AWS S3 Secret Access Key.
   * - ``endpointUrl``
     - no
     - *(AWS)* Secret reference to the S3 endpoint URL.
   * - ``accountName``
     - no
     - *(Azure)* Secret reference to the Azure Storage account name.
   * - ``accountKey``
     - no
     - *(Azure)* Secret reference to the Azure Storage Account Key.
   * - ``container``
     - no
     - *(Azure)* Secret reference to the Azure Blob Container name.
   * - ``type``
     - no
     - What to restore. One of ``all``, ``tables``, ``metadata``,
       ``partitions``, ``sections``.
   * - ``tables``
     - no
     - List of tables to restore, each formatted ``<schema_name>.<table_name>``.
   * - ``sections``
     - no
     - Restore a single metadata group. A list whose items are each one of
       ``tables``, ``views``, ``users``, ``privileges``, ``analyzers``,
       ``udfs``.
   * - ``partitions``
     - no
     - Restore specific table partitions. A list where each item has
       ``table_ident`` (string) and ``columns`` (a list of ``{name, value}``
       pairs).


.. _crd-nodes:

``spec.nodes``
==============

Describes the nodes making up the cluster. **Required.**

.. list-table::
   :header-rows: 1
   :widths: 20 15 12 53

   * - Field
     - Type
     - Required
     - Description
   * - ``data``
     - list of object
     - **yes**
     - One or more data-node specifications (at least one). See
       :ref:`crd-nodes-data`.
   * - ``master``
     - object
     - no
     - An optional set of dedicated master nodes. See :ref:`crd-nodes-master`.


.. _crd-nodes-data:

``spec.nodes.data[]``
---------------------

Each entry describes one type of data node (for example a ``hot`` tier). At
least one entry is required. ``name``, ``replicas`` and ``resources`` are
required per entry.

.. list-table::
   :header-rows: 1
   :widths: 22 14 12 52

   * - Field
     - Type
     - Required
     - Description
   * - ``name``
     - string
     - **yes**
     - Uniquely identifying name of this type of node. Must match
       ``^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$``.
   * - ``replicas``
     - number
     - **yes**
     - Number of CrateDB nodes of this type.
   * - ``resources``
     - object
     - **yes**
     - Compute and storage resources; see :ref:`crd-node-resources`.
   * - ``nodepool``
     - string
     - no
     - Type of nodepool where the cluster should run (``shared`` or
       ``dedicated``). See the note below — primarily a CrateDB Cloud concept.
   * - ``annotations``
     - object
     - no
     - Additional annotations to put on the corresponding pods. Free-form.
   * - ``labels``
     - object
     - no
     - Additional labels to put on the corresponding pods. Free-form.
   * - ``settings``
     - object
     - no
     - Additional CrateDB settings applied to all nodes of this type.
       Free-form.

.. admonition:: CrateDB Cloud oriented
   :class: note

   ``nodepool`` targets the ``shared`` / ``dedicated`` node pools used by
   CrateDB Cloud. Setting it to ``shared`` makes the operator schedule pods with
   a ``nodeAffinity`` on a ``cratedb: shared`` node label plus a matching
   toleration; ``dedicated`` (or leaving it unset) uses standard per-host
   anti-affinity. Self-hosted users only need this if they have deliberately
   labelled and tainted nodes to mirror that layout — otherwise leave it unset.


.. _crd-nodes-master:

``spec.nodes.master``
---------------------

An optional set of dedicated master nodes. When omitted, the cluster runs
without dedicated masters and the **first** ``spec.nodes.data`` group is used as
the master-eligible nodes (the initial master set is derived from ``data[0]``).
``replicas`` and ``resources`` are required when this object is present.

.. list-table::
   :header-rows: 1
   :widths: 22 14 12 52

   * - Field
     - Type
     - Required
     - Description
   * - ``replicas``
     - number
     - **yes**
     - Number of master nodes. Should be an odd number; minimum ``3``.
   * - ``resources``
     - object
     - **yes**
     - Compute and storage resources; see :ref:`crd-node-resources`.
   * - ``nodepool``
     - string
     - no
     - Type of nodepool where the cluster should run.
   * - ``annotations``
     - object
     - no
     - Additional annotations to put on the corresponding pods. Free-form.
   * - ``labels``
     - object
     - no
     - Additional labels to put on the corresponding pods. Free-form.
   * - ``settings``
     - object
     - no
     - Additional CrateDB settings applied to all master nodes. Free-form.


.. _crd-node-resources:

Node ``resources``
------------------

Shared by both ``spec.nodes.data[]`` and ``spec.nodes.master``. ``disk`` and
``heapRatio`` are required.

.. list-table::
   :header-rows: 1
   :widths: 22 14 12 52

   * - Field
     - Type
     - Required
     - Description
   * - ``heapRatio``
     - number (float)
     - **yes**
     - Allocated CrateDB heap size as a fraction of the container memory
       *limit* (``limits.memory``, or the deprecated ``memory`` fallback). For
       example ``0.25`` gives the heap a quarter of the memory limit.
   * - ``disk``
     - object
     - **yes**
     - Persistent storage; see below.
   * - ``requests.cpu``
     - number
     - no
     - Requested CPUs for each CrateDB container. Supports the `Kubernetes
       resource syntax
       <https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/>`_.
   * - ``requests.memory``
     - string
     - no
     - Requested memory for each CrateDB container (Kubernetes resource syntax).
   * - ``limits.cpu``
     - number
     - no
     - CPU limit for each CrateDB container (Kubernetes resource syntax).
   * - ``limits.memory``
     - string
     - no
     - Memory limit for each CrateDB container (Kubernetes resource syntax).
   * - ``cpus``
     - number
     - no
     - **Deprecated** — use ``requests``/``limits`` instead.
   * - ``memory``
     - string
     - no
     - **Deprecated** — use ``requests``/``limits`` instead.

The ``resources.disk`` object requires all of ``count``, ``size`` and
``storageClass``:

.. list-table::
   :header-rows: 1
   :widths: 24 14 62

   * - Field
     - Required
     - Description
   * - ``count``
     - **yes**
     - Number of disks.
   * - ``size``
     - **yes**
     - Size of the disk (for example ``100GiB``).
   * - ``storageClass``
     - **yes**
     - The name of a Kubernetes StorageClass.


.. _crd-backups:

``spec.backups``
================

Configures scheduled snapshot backups. Configure exactly one provider per
cluster — either ``aws`` or ``azure_blob``. For the selected provider the
operator creates a Kubernetes CronJob (plus a metrics exporter Deployment) that
runs the backups on the schedule you define.


``spec.backups.aws``
--------------------

Required members: ``accessKeyId``, ``bucket``, ``cron``, ``region`` and
``secretAccessKey``. Each credential/coordinate is a :ref:`secret reference
<crd-secretkeyref>`.

.. list-table::
   :header-rows: 1
   :widths: 26 14 60

   * - Field
     - Required
     - Description
   * - ``accessKeyId``
     - **yes**
     - Secret reference to the AWS Access Key ID.
   * - ``secretAccessKey``
     - **yes**
     - Secret reference to the AWS Secret Access Key.
   * - ``bucket``
     - **yes**
     - Secret reference to the AWS S3 bucket name.
   * - ``region``
     - **yes**
     - Secret reference to the AWS region.
   * - ``cron``
     - **yes**
     - A crontab-formatted string indicating when and how often to back up.
   * - ``endpointUrl``
     - no
     - Secret reference to the S3 endpoint URL.
   * - ``basePath``
     - no
     - The base path within the backup under which the snapshots are placed.
       Optional and retained for backwards compatibility; current backups
       derive the path automatically, so new deployments can leave it unset.


``spec.backups.azure_blob``
---------------------------

Required members: ``accountName``, ``accountKey``, ``container``, ``cron`` and
``region``. Each credential/coordinate is a :ref:`secret reference
<crd-secretkeyref>`.

.. list-table::
   :header-rows: 1
   :widths: 26 14 60

   * - Field
     - Required
     - Description
   * - ``accountName``
     - **yes**
     - Secret reference to the Azure Storage account name.
   * - ``accountKey``
     - **yes**
     - Secret reference to the Azure Storage Account Key.
   * - ``container``
     - **yes**
     - Secret reference to the Azure Blob Container name.
   * - ``region``
     - **yes**
     - Secret reference to a region value. Used by the backup metrics exporter
       and kept consistent with the AWS configuration layout.
   * - ``cron``
     - **yes**
     - A crontab-formatted string indicating when and how often to back up. Set
       the desired schedule when you create the cluster.


.. _crd-grandcentral:

``spec.grandCentral``
=====================

.. admonition:: CrateDB Cloud only
   :class: note

   Grand Central is part of the managed CrateDB Cloud stack. ``apiUrl`` points
   at the CrateDB Cloud API and ``jwkUrl`` verifies Cloud-issued JWT tokens.
   **Self-hosted operator users do not need this section** and can leave
   ``spec.grandCentral`` unset.

Deploys the Grand Central backend for the cluster. Required members:
``backendImage``, ``backendEnabled``, ``jwkUrl`` and ``apiUrl``.

.. list-table::
   :header-rows: 1
   :widths: 22 14 12 52

   * - Field
     - Type
     - Required
     - Description
   * - ``backendImage``
     - string
     - **yes**
     - The image of the Grand Central backend.
   * - ``backendEnabled``
     - boolean
     - **yes**
     - Controls whether the Grand Central backend is deployed for this cluster.
       Set to ``true`` to deploy it.
   * - ``jwkUrl``
     - string
     - **yes**
     - Endpoint returning the JWK public keys used to verify JWT tokens.
   * - ``apiUrl``
     - string
     - **yes**
     - The CrateDB Cloud API URL.
   * - ``exposure``
     - string
     - no
     - Grand Central routing exposure type: ``traefik`` routes through Traefik,
       and ``nginx`` (the default) routes through an nginx Ingress. When unset,
       it follows ``spec.cluster.exposure``.


.. _crd-ports:

``spec.ports``
==============

Port numbers exposed by the cluster. The ``ports`` object is optional and each
value must be between ``1`` and ``65535``. Any port you do not specify uses the
default shown below, so most deployments can omit this section entirely and only
set the ports they need to change.

.. list-table::
   :header-rows: 1
   :widths: 22 16 62

   * - Field
     - Default
     - Description
   * - ``http``
     - ``4200``
     - HTTP port number.
   * - ``postgres``
     - ``5432``
     - PostgreSQL (wire protocol) port number.
   * - ``jmx``
     - ``6666``
     - JMX port number.
   * - ``prometheus``
     - ``7071``
     - Prometheus (JMX exporter) port number.

The SQL exporter runs on a fixed port (``9399``) and is managed by the operator,
so it does not need to be configured here.


.. _crd-users:

``spec.users``
==============

A list of CrateDB users to create and manage. Each item requires ``name`` and
``password``.

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Required
     - Description
   * - ``name``
     - **yes**
     - The username for a CrateDB cluster user.
   * - ``password``
     - **yes**
     - A :ref:`secret reference <crd-secretkeyref>` to the user's password.

Each user in ``spec.users`` is granted ``ALL`` privileges. Changing a user's
password by updating the referenced Kubernetes Secret is reconciled into the
cluster automatically.

.. note::

   The operator also creates a ``system`` user that is **not** part of
   ``spec.users`` — its own administrative user, created on every cluster with
   ``ALL`` privileges.

   On CrateDB Cloud only, an additional ``gc_admin`` user is created when Grand
   Central is enabled, and it becomes the sole user with access to the internal
   ``gc`` schema (all other users are denied that schema). Self-hosted clusters
   without ``spec.grandCentral`` have neither the ``gc_admin`` user nor the
   ``gc`` schema restriction.

See :ref:`concept-bootstrapping` for how the operator creates and reconciles
these users.


.. _CRD definition: https://github.com/crate/crate-operator/blob/master/deploy/charts/crate-operator-crds/templates/cratedbs-cloud-crate-io.yaml
