# CrateDB Kubernetes Operator
# Copyright (C) 2020 Crate.IO GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import asyncio
import os
import pathlib
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
from kubernetes_asyncio.client.api_client import ApiClient
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
    k8s_context = config.getoption(KUBECONTEXT_OPTION)

    if kubeconfig:
        if k8s_context is None:
            p = subprocess.run(
                [
                    "kubectl",
                    "--kubeconfig",
                    str(pathlib.Path(kubeconfig).expanduser().resolve()),
                    "config",
                    "current-context",
                ],
                check=True,
                capture_output=True,
            )
            k8s_context = p.stdout.decode().strip()

        if k8s_context is None or (
            not k8s_context.startswith("crate-") and k8s_context != "minikube"
        ):
            raise RuntimeError(
                f"Cannot run tests on '{k8s_context}'. "
                "Expected a context prefixed with 'crate-'."
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
        "CRATEDB_OPERATOR_DEBUG_VOLUME_SIZE": "2GiB",
        "CRATEDB_OPERATOR_DEBUG_VOLUME_STORAGE_CLASS": "default",
        "CRATEDB_OPERATOR_IMAGE_PULL_SECRETS": "",
        "CRATEDB_OPERATOR_JMX_EXPORTER_VERSION": "1.0.0",
        "CRATEDB_OPERATOR_LOG_LEVEL": "DEBUG",
        "CRATEDB_OPERATOR_TESTING": "true",
        "CRATEDB_OPERATOR_JOBS_TABLE": "test.test_sys_jobs",
        "CRATEDB_OPERATOR_BOOTSTRAP_RETRY_DELAY": "5",
        "CRATEDB_OPERATOR_HEALTH_CHECK_RETRY_DELAY": "5",
        "CRATEDB_OPERATOR_CRATEDB_STATUS_CHECK_INTERVAL": "5",
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


@pytest.fixture(name="api_client")
async def k8s_asyncio_api_client(kube_config) -> ApiClient:
    async with ApiClient() as api_client:
        yield api_client


@pytest.fixture(scope="session")
def cratedb_crd(request, load_config):
    kubeconfig = request.config.getoption(KUBECONFIG_OPTION)
    assert kubeconfig is not None, f"{KUBECONFIG_OPTION} must be present"
    fname = "deploy/crd.yaml"
    subprocess.run(
        [
            "kubectl",
            "--kubeconfig",
            str(pathlib.Path(kubeconfig).expanduser().resolve()),
            "apply",
            "-f",
            fname,
        ],
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
        "CRATEDB_OPERATOR_KUBECONFIG": str(
            pathlib.Path(request.config.getoption(KUBECONFIG_OPTION))
            .expanduser()
            .resolve()
        ),
    }
    with mock.patch.dict(os.environ, env):
        with KopfRunner(["run", "--standalone", main.__file__]) as runner:
            yield runner


@pytest.fixture(autouse=True)
def faker_seed():
    # This sets a new seed for each test that uses the `Faker` library.
    return random.randint(1, 999999)


@pytest.fixture
async def namespace(faker, api_client) -> V1Namespace:
    core = CoreV1Api(api_client)
    name = faker.uuid4()
    await assert_wait_for(False, does_namespace_exist, core, name)
    ns: V1Namespace = await core.create_namespace(
        body=V1Namespace(metadata=V1ObjectMeta(name=name))
    )
    await assert_wait_for(True, does_namespace_exist, core, name)
    yield ns
    await core.delete_namespace(name=ns.metadata.name, body=V1DeleteOptions())
