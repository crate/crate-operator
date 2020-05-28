import asyncio
import os
import random
import subprocess
from unittest import mock

import pytest
from kopf.testing import KopfRunner
from kubernetes_asyncio.client import (
    CoreV1Api,
    V1DeleteOptions,
    V1Namespace,
    V1ObjectMeta,
)
from kubernetes_asyncio.config import load_kube_config

from crate.operator.config import config

from .utils import assert_wait_for, does_namespace_exist

KUBECONFIG_OPTION = "--kube-config"
KUBECONTEXT_OPTION = "--kube-context"


def pytest_configure(config):
    # `pytest_configure` is an entrypoint hook to configure pytest. We leverage
    # it to ensure tests are only run on specifically allowed K8s contexts.

    config.addinivalue_line("markers", "k8s: mark test to require a Kubernetes cluster")

    kubeconfig = config.getoption(KUBECONFIG_OPTION)
    if kubeconfig:
        p = subprocess.run(
            ["kubectl", "--kubeconfig", kubeconfig, "config", "current-context"],
            check=True,
            capture_output=True,
        )
        k8s_context = p.stdout.decode().strip()

        if not k8s_context.startswith("crate-") and k8s_context != "minikube":
            raise RuntimeError(
                f"Cannot run tests on '{k8s_context}'. "
                "Expected a context prefixed with 'cloud-'."
            )


def pytest_addoption(parser):
    parser.addoption(KUBECONFIG_OPTION, help="Path to kubeconfig")
    parser.addoption(KUBECONTEXT_OPTION, help="Name of the context")


def pytest_collection_modifyitems(config, items):
    if config.getoption(KUBECONFIG_OPTION):
        # --kube-config given in cli: do not skip k8s tests
        return
    skip = pytest.mark.skip(reason=f"Need {KUBECONFIG_OPTION} option to run")
    for item in items:
        if "k8s" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session", autouse=True)
def load_config():
    env = {
        "CRATEDB_OPERATOR_CLUSTER_BACKUP_IMAGE": "crate/does-not-exist-backup",
        "CRATEDB_OPERATOR_CLUSTER_BOOTSTRAP_IMAGE": "crate/does-not-exist-bootstrap",
        "CRATEDB_OPERATOR_DEBUG_VOLUME_SIZE": "2GiB",
        "CRATEDB_OPERATOR_DEBUG_VOLUME_STORAGE_CLASS": "standard",
        "CRATEDB_OPERATOR_IMAGE_PULL_SECRETS": "",
        "CRATEDB_OPERATOR_JMX_EXPORTER_VERSION": "0.6.0",
        "CRATEDB_OPERATOR_LOG_LEVEL": "DEBUG",
    }
    with mock.patch.dict(os.environ, env):
        config.load()
        yield


@pytest.fixture(autouse=True)
async def kube_config(request, load_config):
    config = request.config.getoption(KUBECONFIG_OPTION)
    context = request.config.getoption(KUBECONTEXT_OPTION)
    if config:
        await load_kube_config(config_file=config, context=context)


@pytest.fixture(scope="session")
def cratedb_crd(request, load_config):
    kubeconfig = request.config.getoption(KUBECONFIG_OPTION)
    assert kubeconfig is not None, f"{KUBECONFIG_OPTION} must be present"
    fname = "deploy/manifests/00-crd-cratedb.yaml"
    subprocess.run(
        ["kubectl", "--kubeconfig", kubeconfig, "apply", "-f", fname],
        check=True,
        capture_output=True,
    )
    yield


@pytest.fixture
async def cleanup_handler(event_loop):
    handlers = []
    yield handlers
    await asyncio.gather(*handlers, return_exceptions=True)


@pytest.fixture(scope="session")
def kopf_runner(request, cratedb_crd):
    # Make all handlers available to the runner
    from crate.operator import main

    # Make sure KUBECONFIG env variable is set because KOPF depends on it
    env = {
        "CRATEDB_OPERATOR_KUBECONFIG": request.config.getoption(KUBECONFIG_OPTION),
    }
    with mock.patch.dict(os.environ, env):
        with KopfRunner(["run", "--verbose", "--standalone", main.__file__]) as runner:
            yield runner


@pytest.fixture("function", autouse=True)
def faker_seed():
    # This sets a new seed for each test that uses the `Faker` library.
    return random.randint(1, 999999)


@pytest.fixture
async def namespace(faker) -> V1Namespace:
    core = CoreV1Api()
    name = faker.domain_word()
    await assert_wait_for(False, does_namespace_exist, core, name)
    ns: V1Namespace = await core.create_namespace(
        body=V1Namespace(metadata=V1ObjectMeta(name=name))
    )
    await assert_wait_for(True, does_namespace_exist, core, name)
    yield ns
    await core.delete_namespace(name=ns.metadata.name, body=V1DeleteOptions())
