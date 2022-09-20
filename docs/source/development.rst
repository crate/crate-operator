Working on the operator
=======================

Local installation
------------------

The ``crate-operator`` package requires **Python 3.8**.

It is recommended to use a virtual environment to the operator and its
dependencies for local development.

.. note::

   If you are using macOS, you need to have PostgreSQL installed. You can
   install it via Homebrew_ (``brew install postgresql``) or follow the
   `PostgreSQL docs`_.

.. code-block:: console

   $ python3.8 -m venv env
   $ source env/bin/activate
   (env)$ python -m pip install -e .

Contributing
------------

Before we can accept any pull requests, we need you to agree to our CLA_.

Once that is complete, you should:

- Create an issue on GitHub to let us know that you're working on the issue.

- Use a feature branch and not ``master``.

- Rebase your feature branch onto the upstream ``master`` before creating the
  pull request.

- `Be descriptive in your PR and commit messages
  <#meaningful-commit-messages>`_. What is it for? Why is it needed? And so on.

- If applicable, `run the tests <#testing>`_.

- Squash related commits.


.. _testing:

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

Running with a local k8s cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``microk8s``, ``kind`` and ``Minikube`` have all been proven to work, however there are
some pre-requisites:

- You **must** have enough CPU and memory to run the tests: Some of the upgrade tests
  provision a 3-node cluster with a reservation of 2 CPU cores and 4GiB of RAM each.
  ``kubectl get nodes`` and ``kubectl describe node [your node]`` to check.

- You **must** have a k8s Load Balancer implementation that gives out IP addresses,
  usually this is MetalLB.

- You **must** have a StorageClass called ``default`` that actually works and provisions
  volumes.

Minikube
^^^^^^^^

Using minikube_ as Kubernetes backend for the tests requires a few
preparations. For example, make sure your minikube has configured enough
resources for example:

.. code-block:: console

   $ minikube config view
   - cpus: 4
   - disk-size: 64G
   - memory: 8192

Make sure you have a storage class named ``default``, which uses the
``k8s.io/minikube-hostpath`` provisioner:

.. code-block:: yaml

   apiVersion: storage.k8s.io/v1
   kind: StorageClass
   metadata:
     name: default
   provisioner: k8s.io/minikube-hostpath
   reclaimPolicy: Delete
   volumeBindingMode: Immediate

Additionally, you need to set up a `minikube tunnel`_ in order to get
an external IP address assigned to the CrateDB services.

.. code-block:: console

   $ minikube tunnel

Finally, you can run ``pytest`` using the required arguments:

.. code-block:: console

   (env)$ pytest -vv --kube-config=~/.kube/test_config --kube-context=crate-testing


.. _pytest: https://docs.pytest.org/en/latest/
.. _minikube: https://minikube.sigs.k8s.io
.. _minikube tunnel: https://minikube.sigs.k8s.io/docs/handbook/accessing/#using-minikube-tunnel


Running test in parallel
^^^^^^^^^^^^^^^^^^^^^^^^

The operator supports running the entire test suite in parallel, to significantly speed
up execution. To do that, simply run the tests with the ``-n X`` parameter to pytest:

.. code-block:: console

   (env)$ pytest -n 2 --dist loadfile -vv --kube-config=~/.kube/test_config --kube-context=crate-testing

Code style
----------

This project uses `pre-commit`_ to ensure proper linting, code formatting, and
type checking. Tools, such as ``black``, ``flake8``, ``isort``, and ``mypy``
should be run as hooks upon committing or pushing code. When at least one of
the hooks fails, committing or pushing changes is aborted and manual
intervention is necessary. For example, an ill-formatted piece of Python code
that is staged for committing with Git, would be cleaned up by the ``black``
hook. It's then up to the developer to either amend the changes or stage them
as well.

Install ``pre-commit`` for your user and verify that the installation worked:

.. code-block:: console

   $ pip install --user pre-commit
   $ pre-commit --version
   pre-commit 2.4.0

Please keep in mind that the version shown above might vary.

Once you've confirmed the successful installation of ``pre-commit``, install
the hooks for this project:

.. code-block:: console

   $ pre-commit install -t pre-commit -t pre-push --install-hooks
   pre-commit installed at .git/hooks/pre-commit
   pre-commit installed at .git/hooks/pre-push
   [INFO] Installing environment for

From now on, each time you run ``git commit`` or ``git push``, ``pre-commit``
will run "hooks" defined in the ``.pre-commit-config.yaml`` file on the staged
files.


.. _pre-commit: https://pre-commit.com/


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

#. Run ``./devtools/bumpversion.sh $VERSION`` to update version:

        $ ./devtools/bumpversion.sh 2.14.1
         INFO:  Current branch is 'release/2.14.1'.
         INFO:  Incrementing crate-operator Helm Chart to version '2.14.1'
         INFO:  Incrementing crate-operator-crds Helm Chart to version '2.14.1'
         INFO:  Done. ✨

   If you want to inspect the changes made, run ``git diff``.

#. Commit the changes to the ``CHANGES.rst``,
   ``deploy/charts/crate-operator/Chart.yaml`` and
   ``deploy/charts/crate-operator-crds/Chart.yaml``, push them to GitHub, and open a
   pull request against the ``master`` branch:

   .. code-block:: console

      $ git add CHANGES.rst deploy/charts/crate-operator/Chart.yaml deploy/charts/crate-operator-crds/Chart.yaml
      $ git commit -m "Prepare release $VERSION"
      $ git push --set-upstream origin "release/$VERSION"

#. After merging the pull request to the ``master`` branch, fetch the latest
   changes and create the release:

   .. code-block:: console

      $ git checkout master
      $ git pull
      $ ./devtools/create_tag.sh "$VERSION"


General Tips
------------

.. _commit-message-style:

Meaningful Commit Messages
^^^^^^^^^^^^^^^^^^^^^^^^^^

Please choose a meaningful commit message. The commit message is not only
valuable during the review process, but can be helpful for reasoning about any
changes in the code base. For example, PyCharm's "Annotate" feature, brings up
the commits which introduced the code in a source file. Without meaningful
commit messages, the commit history does not provide any valuable information.

The first line of the commit message (also known as "subject line") should
contain a summary of the changes. Please use the imperative mood. The subject
can be prefixed with "Test: " or "Docs: " to indicate the changes are not
primarily to the main code base. For example::

   Put a timeout on all bootstrap operations
   Test: Increase bootstrap timeout in tests
   Docs: Copyedit docs on configuration options

See also: https://chris.beams.io/posts/git-commit/

Updating Your Branch
^^^^^^^^^^^^^^^^^^^^

If new commits have been added to the upstream ``master`` branch since you
created your feature branch, please do not merge them in to your branch.
Instead, rebase your branch::

   $ git fetch upstream
   $ git rebase upstream/master

This will apply all commits on your feature branch on top of the upstream
``master`` branch. If there are conflicts, they can be resolved with ``git
merge``. After the conflict has been resolved, use ``git rebase --continue`` to
continue the rebase process.

Squashing Minor Commits
^^^^^^^^^^^^^^^^^^^^^^^

Minor commits that only fix typos or rename variables that are related to a
bigger change should be squashed into that commit.

This can be done with the following command::

   $ git rebase -i origin/master

This will open up a text editor where you can annotate your commits.

Generally, you'll want to leave the first commit listed as ``pick``, or change
it to ``reword`` (or ``r`` for short) if you want to change the commit message.
And then, if you want to squash every subsequent commit, you could mark them
all as ``fixup`` (or ``f`` for short).

Once you're done, you can check that it worked by running::

   $ git log

If you're happy with the result, do a **force** push (since you're rewriting
history) to your feature branch::

   $ git push -f


See also: http://www.ericbmerritt.com/2011/09/21/commit-hygiene-and-git.html

.. _Homebrew: https://brew.sh
.. _PostgreSQL docs: https://postgresapp.com/
.. _CLA: https://crate.io/community/contribute/agreements/
.. _setuptools-scm: https://pypi.org/project/setuptools-scm/
.. _Semantic Versioning: https://semver.org/
