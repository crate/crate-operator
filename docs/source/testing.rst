Testing
=======

Testing the *CrateDB Kubernetes Operator* comes at two levels. There are
unittests that run without a Kubernetes cluster, and there are integration and
end-to-end style tests which require a Kubernetes cluster to run.

To run either set of tests, one needs to first install the operator and its
test dependencies. This is typically done inside a Python virtual environemnt:

.. code-block:: console

   $ python3.8 -m venv venv
   (venv)$ source venv/bin/activate
   (venv)$ python -m pip install -e '.[testing]'
   Successfully installed ... crate-operator ...

Now, running the unittest can be done by using pytest_ from within the
activated virtual environemnt:

.. code-block:: console

   (venv)$ pytest

The default pytest_ invokation is fairly quiet. Verbosity can be increased by
passing ``-v`` (multiple times):

.. code-block:: console

   (venv)$ pytest -vv

Running the integration or end-to-end style tests requires a running Kubernetes
cluster and access to it through ``kubectl``. That also mean, you need to have
a ``kubeconfig`` file at hand. Often enough, you can find that file in the
``.kube`` folder in your home directory (``$HOME/.kube/config``).

.. warning::

   Before you run the tests, make sure you're not using a production Kubernetes
   cluster, as some tests will remove resources which can quickly interfer with
   your production operation!

To reduce the risk of accidentally using a production Kubernetes context, the
operator requires not only the *path* to the ``kubeconfig`` but *additionally*
the Kubernetes context to use. These two parameters need to be passed via the
``--kube-config`` and ``--kube-context`` arguments to pytest_: Furthermore, the
context *must* start with either ``crate-`` or be called ``minikube``.

.. code-block:: console

   (venv)$ pytest -vv --kube-config=~/.kube/test_config --kube-context=crate-testing

.. _pytest: https://docs.pytest.org/en/latest/
