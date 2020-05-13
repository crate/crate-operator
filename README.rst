===========================
CrateDB Kubernetes Operator
===========================

The *CrateDB Kubernetes Operator* provides a nice and convenient way to run
CrateDB_ inside Kubernetes_.

Development
===========

The project requires **Python 3.8**.

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

.. _CrateDB: https://github.com/crate/crate
.. _Kubernetes: https://kubernetes.io/
.. _pre-commit: https://pre-commit.com/
