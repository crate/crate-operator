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
    setup_requires=["setuptools>=58", "setuptools_scm>=6.2"],
    install_requires=[
        "aiopg==1.4.0",
        "bitmath==1.3.3.1",
        "kopf==1.35.6",
        # Careful with 22+ - it is currently not compatible
        # and results in various "permission denied" errors.
        "kubernetes-asyncio==21.7.1",
        "PyYAML<7.0",
        "prometheus_client==0.15.0",
        # Versions 3.8+ incompatible with pytest-aiohttp.
        "aiohttp<=3.7.4",
        "wrapt==1.14.1",
    ],
    extras_require={
        "docs": [
            "sphinx>=3.0,<3.4",
            "sphinx-autodoc-typehints",
            # Pinning this as 0.18 does not work
            "docutils==0.17.1",
            "Jinja2<3.1",
        ],
        "testing": [
            "faker==15.3.4",
            "pytest==7.2.0",
            "pytest-aiohttp==0.3.0",
            "pytest-asyncio==0.20.3",
            "pytest-xdist==3.1.0",  # enables parallel testing
            "filelock==3.8.2",  # used for locks when running in parallel mode
        ],
        "develop": [
            "black==22.3.0",
            "flake8==3.8.4",
            "isort==5.6.4",
            "mypy==0.770",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    use_scm_version=True,
)
