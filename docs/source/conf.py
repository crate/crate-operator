# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from typing import List

from crate.operator import __version__

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------


sys.path.insert(0, os.path.abspath("."))


# -- Project information -----------------------------------------------------

project = "CrateDB Kubernetes Operator"
copyright = "2020, Crate.io"
author = "Crate.io"

version = release = __version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx_autodoc_typehints",
    "crate_operator_ext",
    "sphinx.ext.intersphinx",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns: List[str] = []

# Some references just don't exist even though they maybe should. Adding them
# here.
nitpick_ignore = [
    # undoc'd; https://docs.python.org/3/distutils/apiref.html#module-distutils.version
    ("py:class", "distutils.version.Version"),
    ("py:class", "prometheus_client.registry.Collector"),
    ("py:class", "verlib2.distutils.version.Version"),
]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "alabaster"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]


# -- Extensions configuration ------------------------------------------------

intersphinx_mapping = {
    "aiohttp": ("https://docs.aiohttp.org/en/stable/", None),
    "aiopg": ("https://aiopg.readthedocs.io/en/stable/", None),
    "bitmath": ("https://bitmath.readthedocs.io/en/stable/", None),
    "cratedb": ("https://crate.io/docs/crate/reference/en/stable/", None),
    "kopf": ("https://kopf.readthedocs.io/en/stable/", None),
    "python": ("https://docs.python.org/3.12/", None),
}
always_document_param_types = True
