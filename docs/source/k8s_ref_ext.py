from docutils.nodes import Element
from sphinx.addnodes import pending_xref
from sphinx.application import Sphinx
from sphinx.environment import BuildEnvironment
from sphinx.errors import NoUri


def missing_reference(
    app: Sphinx, env: BuildEnvironment, node: pending_xref, contnode: Element
) -> Element:
    """
    Remove references to ``kubernetes_asyncio``.

    The ``kubernetes_asyncio`` package doesn't provide any reference
    documentsion for its API. To allow nitpicking on missing references we take
    the approach to exclude all ``kubernetes_asyncio`` references.
    """
    reftarget = node.get("reftarget", "")
    if node.get("refdomain") == "py" and reftarget.startswith("kubernetes_asyncio."):
        raise NoUri()


def setup(app):
    app.connect("missing-reference", missing_reference)
