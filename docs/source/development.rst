Working on the operator
=======================

Testing
-------

Testing the CrateDB Kubernetes Operator is possible at two levels. There are
unit tests that run without a Kubernetes cluster, and there are integration and
end-to-end style tests that do require a Kubernetes cluster to run.

To run either set of tests, one needs to first install the operator and its
test dependencies. This is typically done inside a Python virtual environment:

.. code-block:: console

   $ python3.8 -m venv env
   $ source env/bin/activate
   (env)$ python -m pip install -e '.[testing]'
   Successfully installed ... crate-operator ...

Now, running the unit test can be done by using pytest_ from within the
activated virtual environment:

.. code-block:: console

   (env)$ pytest

Running the integration or end-to-end style tests requires a running Kubernetes
cluster and access to it through ``kubectl``. That also means you need to have
a ``kubeconfig`` file at hand. Often enough, you can find that file in the
``.kube`` folder in your home directory (``$HOME/.kube/config``).

.. warning::

   Before you run the tests, make sure you're not using a production Kubernetes
   cluster, as some tests will remove resources. This can easily interfere with
   your production operation!

To reduce the risk of accidentally using a production Kubernetes context, the
operator requires not only specifying the path to the ``kubeconfig``, but
additionally the Kubernetes context to use. These two parameters need to be
passed via the ``--kube-config`` and ``--kube-context`` arguments to pytest_.
Furthermore, the context *must* start with either ``crate-`` or be called
``minikube``.

.. code-block:: console

   (env)$ pytest -vv --kube-config=~/.kube/test_config --kube-context=crate-testing


.. _pytest: https://docs.pytest.org/en/latest/


Making a release
----------------

``crate-operator`` uses `setuptools-scm`_. That means, bumping the package's
version happens automatically for each git commit or git tag. The operator's
versions follows Python's :pep:`440` format, where the first 3 parts represent
the *major*, *minor*, and *patch* parts according to `Semantic
Versioning`_.

For the following steps we assume the next version is going to be ``$VERSION``.

#. When ready to prepare a new release, start a new branch ``release/$VERSION``:

   .. code-block:: console

      $ git checkout -b "release/$VERSION"

#. Next, go ahead and ensure the changelog ``CHANGES.rst`` is up to date.

#. Commit the changes to the ``CHANGES.rst``, push them to GitHub, and open a
   pull request against the ``master`` branch:

   .. code-block:: console

      $ git add CHANGES.rst
      $ git commit -m "Prepare release $VERSION"
      $ git push --set-upstream origin "release/$VERSION"

#. After merging the pull request to the ``master`` branch, fetch the latest
   changes and create the release:

   .. code-block:: console

      $ git checkout master
      $ git pull
      $ ./devtools/create_tag.sh "$VERSION"

.. _setuptools-scm: https://pypi.org/project/setuptools-scm/
.. _Semantic Versioning: https://semver.org/
