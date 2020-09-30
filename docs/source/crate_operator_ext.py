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
