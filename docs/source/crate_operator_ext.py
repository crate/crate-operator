from docutils.nodes import Element
from sphinx.addnodes import pending_xref
from sphinx.application import Sphinx
from sphinx.environment import BuildEnvironment
from sphinx.errors import NoUri


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
    app.connect("missing-reference", missing_reference)
