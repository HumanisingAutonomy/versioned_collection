# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

import git

PROJECT_ROOT = "../.."

sys.path.insert(0, os.path.abspath(PROJECT_ROOT))
autodoc_mock_imports = ['pymongo', 'treelib', 'colorama', 'deepdiff', 'bson']

# -- Project information -----------------------------------------------------

project = 'VersionedCollection'
copyright = '2024, Humanising Autonomy'
author = 'Humanising Autonomy'


# The full version, including alpha/beta/rc tags
def fetch_version() -> str:
    # When using nox, `VERSION` is exported to the environment
    if env_version := os.getenv("VERSION"):
        return f"v{env_version}"

    # for ReadTheDocs
    repo = git.Repo(PROJECT_ROOT)
    if len(repo.tags):
        return str(next(reversed(repo.tags)))

    return ""


release = fetch_version()

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosectionlabel',
    'sphinxcontrib.katex',
    'sphinxcontrib.pseudocode',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'furo'

html_theme_options = {
    "dark_css_variables": {
        "color-api-name": "#f4c2f0",
        "color-api-pre-name": "#f4c2f0",
    },
}

html_logo = '_static/img/logo.png'
html_favicon = '_static/img/favicon.png'

html_show_sourcelink = True

# Disable docstring inheritance
autodoc_inherit_docstrings = False

numfig = True

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']


def setup(app):
    app.add_css_file('stylesheet.css')
