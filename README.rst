.. image:: https://github.com/crate/crate-operator/workflows/CI/badge.svg
   :alt: Continuous Integration
   :target: https://github.com/crate/crate-operator

.. image:: https://github.com/crate/crate-operator/workflows/Build%20and%20publish%20Docker%20Image/badge.svg
   :alt: Docker Build
   :target: https://github.com/crate/crate-operator

.. image:: https://img.shields.io/badge/docs-latest-brightgreen.svg
   :alt: Documentation
   :target: https://crate-operator.readthedocs.io/en/latest/

.. image:: https://img.shields.io/badge/container-docker-green.svg
   :alt: Docker Hub
   :target: https://hub.docker.com/crate/crate-operator/


=============================
⚙️ CrateDB Kubernetes Operator
=============================

The **CrateDB Kubernetes Operator** provides convenient way to run `CrateDB`_
clusters inside `Kubernetes`_. It is built on top of the `Kopf: Kubernetes
Operators Framework`_.

🗒️ Contents
==========

- `🤹 Usage`_
- `🎉 Features`_
- `💽 Installation`_
- `💻 Development`_

🤹 Usage
=======

A minimal custom resource for a 3 node CrateDB cluster may look like this:

``dev-cluster.yaml``:

.. code-block:: yaml

   apiVersion: cloud.crate.io/v1
   kind: CrateDB
   metadata:
     name: my-cluster
     namespace: dev
   spec:
     cluster:
       imageRegistry: crate
       name: crate-dev
       version: 5.7.3
     nodes:
       data:
       - name: hot
         replicas: 3
         resources:
           limits:
             cpu: 4
             memory: 4Gi
           disk:
             count: 1
             size: 128GiB
             storageClass: default
           heapRatio: 0.5

.. code-block:: console

   $ kubectl --namespace dev create -f dev-cluster.yaml
   ...

   $ kubectl --namespace dev get cratedbs
   NAMESPACE   NAME         AGE
   dev         my-cluster   36s


Please note that the minimum version of CrateDB that the operator supports is **4.5**.
Previous versions might work, but the operator will not attempt to set a license.


🎉 Features
===========

- "all equal nodes" cluster setup
- "master + data nodes" cluster setup
- safe scaling of clusters
- safe rolling version upgrades for clusters
- SSL for HTTP and PG connections via Let's Encrypt certificate
- custom node settings
- custom cluster settings
- custom storage classes
- region/zone awareness for AWS and Azure
- OpenShift support (Red Hat OpenShift Container Platform 4.x)

💽 Installation
===============

Installation with Helm
----------------------

To be able to deploy the custom resource ``CrateDB`` to a Kubernetes cluster,
the API needs to be extended with a `Custom Resource Definition` (CRD).
It can be installed separately by installing the `CRD Helm Chart`_ or as a
dependency of the `Operator Helm Chart`_.

.. code-block:: console

   helm repo add crate-operator https://crate.github.io/crate-operator
   helm install crate-operator crate-operator/crate-operator

To override the environment variables from values.yaml, please refer to
the `configuration documentation`_.

Installation on OpenShift
-------------------------

When installing on Red Hat OpenShift Container Platform, additional configuration
is required, after adding the Helm repo:

.. code-block:: console

   helm install crate-operator crate-operator/crate-operator \
      --set env.CRATEDB_OPERATOR_CLOUD_PROVIDER=openshift \
      --set env.CRATEDB_OPERATOR_CRATE_CONTROL_IMAGE=your-registry/crate-control:latest \
      --namespace crate-operator \
      --create-namespace

Replace ``your-registry/crate-control:latest`` with the location of your built
crate-control sidecar image. See the `OpenShift documentation`_ for details.

Installation with kubectl
-------------------------

To be able to deploy the custom resource ``CrateDB`` to a Kubernetes cluster,
the API needs to be extended with a `Custom Resource Definition` (CRD). The CRD
for ``CrateDB`` can be found in the ``deploy/`` folder and can be applied
(assuming sufficient privileges).

.. code-block:: console

   $ kubectl apply -f deploy/crd.yaml
   customresourcedefinition.apiextensions.k8s.io/cratedbs.cloud.crate.io created

Once the CRD is installed, the operator itself can be deployed using a
``Deployment`` in the ``crate-operator`` namespace.

.. code-block:: console

   $ kubectl create namespace crate-operator
   ...
   $ kubectl create -f deploy/rbac.yaml
   ...
   $ kubectl create -f deploy/deployment.yaml
   ...

Please refer to the `configuration documentation`_ for further details.

💻 Development
=============

Please refer to the `Working on the operator`_ section of the documentation.


.. _CrateDB: https://github.com/crate/crate
.. _Custom Resource Definition: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/
.. _Kubernetes: https://kubernetes.io/
.. _`Kopf: Kubernetes Operators Framework`: https://kopf.readthedocs.io/en/latest/
.. _configuration documentation: ./docs/source/configuration.rst
.. _Working on the operator: ./docs/source/development.rst
.. _CRD Helm Chart: ./deploy/charts/crate-operator-crds/README.md
.. _Operator Helm Chart: ./deploy/charts/crate-operator/README.md
.. _OpenShift documentation: ./docs/source/openshift.rst
