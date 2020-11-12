Installation
============

The CrateDB Kubernetes Operator makes use of Kubernetes' `Custom Resources`_.
Therefore, in order to use the operator, one needs to install the CrateDB
custom resource:

.. code-block:: console

   $ kubectl create -f deploy/crd.yaml
   customresourcedefinition.apiextensions.k8s.io/cratedbs.cloud.crate.io created


.. _Custom Resources: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/
