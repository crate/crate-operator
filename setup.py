# -*- coding: utf-8 -*-

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
    author="Crate.io",
    author_email="office@crate.io",
    description="CrateDB Kubernetes Operator",
    long_description=read("README.rst"),
    long_description_content_type="text/x-rst",
    packages=find_namespace_packages(include=["crate.*"]),
    setup_requires=["setuptools_scm"],
    install_requires=["kopf==0.26", "kubernetes-asyncio==11.2.0"],
    extras_require={"docs": ["sphinx"]},
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 1 - Beta",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    use_scm_version=True,
)
