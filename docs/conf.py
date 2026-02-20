# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

from datetime import date

from yellowdog_ray import __version__

project = f"RayDog: YellowDog Integration with Ray"
copyright = f"{date.today().year}, YellowDog Limited. Version {__version__}"
author = "YellowDog Limited"
release = __version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.todo", "sphinx.ext.viewcode", "sphinx.ext.autodoc"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# autoclass_content = "both"
# autodoc_default_options = {
#     "members": True,
#     "special-members": "__init__",
# }

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_logo = "_static/yellowdog-wordmark.svg"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
