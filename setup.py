# CrateDB Kubernetes Operator
#
# Licensed to Crate.IO GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.


import os
from pathlib import Path

from setuptools import find_namespace_packages, setup


def cwd() -> Path:
    return Path(os.path.dirname(__file__))


def read(path: str) -> str:
    filepath: Path = cwd() / path
    with open(filepath.absolute(), "r", encoding="utf-8") as f:
        return f.read()


setup(
    name="crate-operator",
    author="Crate.IO GmbH",
    author_email="office@crate.io",
    description="CrateDB Kubernetes Operator",
    license="Apache License 2.0",
    long_description=read("README.rst"),
    long_description_content_type="text/x-rst",
    packages=find_namespace_packages(include=["crate.*"]),
    include_package_data=True,
    package_data={"crate.operator": ["data/*"]},
    setup_requires=["setuptools>=70.3.0", "setuptools_scm>=8.1.0"],
    install_requires=[
        "aiopg==1.4.0",
        "bitmath==1.3.3.1",
        "importlib-metadata; python_version<'3.8'",
        "kopf==1.37.5",
        "kubernetes-asyncio==31.1.0",
        "PyYAML<7.0",
        "prometheus_client==0.21.1",
        "aiohttp==3.11.16",
        "verlib2==0.3.1",
        "wrapt==1.17.2",
        "python-json-logger==3.3.0",
    ],
    extras_require={
        "docs": [
            "alabaster<2",
            "sphinx>=5,<9",
            "sphinx-autodoc-typehints<4",
        ],
        "testing": [
            "faker==18.3.1",
            "pytest==8.3.5",
            "pytest-aiohttp==1.0.5",
            "pytest-asyncio==0.26.0",
            "pytest-xdist==3.6.1",  # enables parallel testing
            "filelock==3.18.0",  # used for locks when running in parallel mode
        ],
        "develop": [
            "black==22.3.0",
            "flake8==3.8.4",
            "isort==5.12.0",
            "mypy==1.13.0",
        ],
    },
    python_requires=">=3.10,<3.14",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    use_scm_version=True,
)
