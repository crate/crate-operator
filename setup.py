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
    license="AGPLv3",
    long_description=read("README.rst"),
    long_description_content_type="text/x-rst",
    packages=find_namespace_packages(include=["crate.*"]),
    include_package_data=True,
    package_data={"crate.operator": ["data/*"]},
    setup_requires=["setuptools>=58", "setuptools_scm>=6.2"],
    install_requires=[
        "aiopg==1.3.2",
        "bitmath==1.3.3.1",
        "kopf==0.28.3",
        # kopf depends on an undefined click version, and the latest (8.0.0) has broken
        # backwards-compatibility. Fixing the version here instead.
        "click==7.1.2",
        "kubernetes-asyncio==18.20.0",
        "PyYAML<7.0",
    ],
    extras_require={
        "docs": [
            "sphinx>=3.0,<3.4",
            "sphinx-autodoc-typehints",
            # Pinning this as 0.18 does not work
            "docutils==0.17.1",
        ],
        "testing": [
            "faker==9.6.0",
            "pytest==6.2.5",
            "pytest-aiohttp==0.3.0",
            "pytest-asyncio==0.16.0",
        ],
        "develop": ["black==20.8b1", "flake8==3.8.4", "isort==5.6.4", "mypy==0.770"],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 1 - Beta",
        "License :: OSI Approved :: GNU Affero General Public License v3"
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    use_scm_version=True,
)
