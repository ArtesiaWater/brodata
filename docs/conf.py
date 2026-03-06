# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from brodata import __version__
import itables

# Initialize interactive tables in all notebooks
itables.init_notebook_mode(all_interactive=True)

# Optional: tweak table appearance & behavior
itables.options.lengthMenu = [5, 10, 25, 50]  # pagination options
itables.options.style = "full"  # full table width
itables.options.classes = "display nowrap"  # ensure scrolling if wide
itables.options.responsive = True  # responsive layout
nb_execution_prelude = """
import itables
from itables import init_notebook_mode
init_notebook_mode(all_interactive=True)
"""

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "brodata"
copyright = "2026, Artesia"
author = "Artesia"
release = __version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.autodoc", "sphinx.ext.napoleon", "myst_nb"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_css_files = [
    #'custom.css',
]
html_theme_options = {"navigation_depth": 4}

# Ensure HTML output is prioritized (important for pandas DataFrames)
nb_mime_priority_overrides = [
    ("html", "text/html", 100),
]

nb_execution_mode = "auto"
# Allow errors in notebooks, so we can see the error online
nb_execution_allow_errors = True
nb_merge_streams = True
nb_execution_timeout = -1

# add a logo
html_logo = "_static/logo_brodata_200.png"
