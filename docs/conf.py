# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html


import os
import sys
from importlib.metadata import version

# Define path to the code to be documented **relative to where conf.py (this file) is kept**
sys.path.insert(0, os.path.abspath("../src/"))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "alert-archive"
copyright = "2025, LINCC Frameworks"
author = "LINCC Frameworks"
release = version("alert-archive")
# for example take major/minor
version = ".".join(release.split(".")[:2])

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.mathjax", "sphinx.ext.napoleon", "sphinx.ext.viewcode"]

extensions.append("autoapi.extension")
extensions.append("nbsphinx")

# -- MyST (Markdown) and Mermaid ---------------------------------------------
# Pages are written in Markdown (MyST); docstrings stay reStructuredText.
extensions.append("myst_parser")
extensions.append("sphinxcontrib.mermaid")
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
## Render ```mermaid fences as diagrams, so the same Markdown works on GitHub and here.
myst_fence_as_directive = ["mermaid"]
## The RTD theme has no dark mode, so use Mermaid's light theme even when the
## reader's OS is in dark mode (the extension follows the OS by default).
mermaid_dark_theme = "default"
## Size each diagram to its content instead of the extension's fixed 500px height.
mermaid_height = "auto"
## ::: fences for admonitions, and anchors for headings so pages can link to sections.
myst_enable_extensions = ["colon_fence"]
myst_heading_anchors = 3

# -- nbsphinx ----------------------------------------------------------------
## The notebooks read the catalog on USDF, which ReadTheDocs can't reach, so
## they're committed with their outputs and never executed during the build.
nbsphinx_execute = "never"

# -- sphinx-copybutton configuration ----------------------------------------
extensions.append("sphinx_copybutton")
## sets up the expected prompt text from console blocks, and excludes it from
## the text that goes into the clipboard.
copybutton_exclude = ".linenos, .gp"
copybutton_prompt_text = ">> "

## lets us suppress the copy button on select code blocks.
copybutton_selector = "div:not(.no-copybutton) > div.highlight > pre"

templates_path = []
exclude_patterns = ["_build", "**.ipynb_checkpoints", "**/README.md"]

# This assumes that sphinx-build is called from the root directory
master_doc = "index"
# Remove 'view source code' from top of page (for html, not python)
html_show_sourcelink = False
# Remove namespaces from class/method signatures
add_module_names = False

## Render docstrings' "Attributes" sections as field lists, so dataclass fields
## aren't documented twice (once from the docstring, once from the class body).
napoleon_use_ivar = True

autoapi_type = "python"
autoapi_dirs = ["../src"]
autoapi_ignore = ["*/__main__.py", "*/_version.py"]
autoapi_add_toc_tree_entry = False
autoapi_member_order = "bysource"

html_theme = "sphinx_rtd_theme"
