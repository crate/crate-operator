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


import pathlib

from docutils.nodes import Element
from sphinx.addnodes import pending_xref
from sphinx.application import Sphinx
from sphinx.environment import BuildEnvironment
from sphinx.errors import NoUri


def run_apidoc(_):
    from sphinx.ext import apidoc

    argv = [
        "--ext-autodoc",
        "--ext-intersphinx",
        "--separate",
        "--implicit-namespaces",
        "--no-toc",
        "-o",
        str(pathlib.Path(__file__).parent / "ref"),
        str(pathlib.Path(__file__).parent.parent.parent / "crate"),
    ]
    apidoc.main(argv)


def missing_reference(
    app: Sphinx, env: BuildEnvironment, node: pending_xref, contnode: Element
) -> Element:
    """
    Remove or resolve references to third party packages.

    - The ``kubernetes_asyncio`` package doesn't provide any reference
      documentsion for its API. To allow nitpicking on missing references we
      take the approach to exclude all ``kubernetes_asyncio`` references.

    - The ``aiopg`` package uses top-level imports (e.g. ``from aiopg import
        Connection``) and writes the documentation as such. But the actual
        classes and functions are defined in e.g. ``aiopg.conection`` or
        ``aiopg.cursor``. We rewrite the references accordingly.
    """
    reftarget = node.get("reftarget", "")
    if node.get("refdomain") == "py":
        if reftarget.startswith("kubernetes_asyncio."):
            raise NoUri()
        elif reftarget.startswith("aiohttp.client_reqrep."):
            node.attributes["reftarget"] = "aiohttp." + reftarget[22:]
        elif reftarget.startswith("aiopg.connection."):
            node.attributes["reftarget"] = "aiopg." + reftarget[17:]
        elif reftarget.startswith("aiopg.cursor."):
            node.attributes["reftarget"] = "aiopg." + reftarget[13:]


def setup(app):
    app.connect("builder-inited", run_apidoc)
    app.connect("missing-reference", missing_reference)
