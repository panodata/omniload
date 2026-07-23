# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "omniload"
copyright = "2024-2026, The omniload developers"  # noqa: A001
author = "The omniload developers"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_sitemap",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.ifconfig",
    "sphinxcontrib.mermaid",
    "sphinxext.opengraph",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "_vale", "Thumbs.db", ".DS_Store"]
suppress_warnings = [
    "myst.header",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# Where the documentation is published to. Needed for sphinx-sitemap.
html_baseurl = "https://omniload.readthedocs.io/"

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.

html_title = "omniload"

html_theme_options = {
    "sidebar_hide_name": False,
    # https://github.com/pradyunsg/furo/blob/main/src/furo/assets/styles/variables/_colors.scss
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# Custom sidebar templates, must be a dictionary that maps document names
# to template names.
#
# The default sidebars (for documents that don't match any pattern) are
# defined by theme itself.  Builtin themes are using these templates by
# default: ``['localtoc.html', 'relations.html', 'sourcelink.html',
# 'searchbox.html']``.
#
# html_sidebars = {}

html_show_sourcelink = True


# -- Intersphinx ----------------------------------------------------------

intersphinx_mapping = {
    "crash": ("https://cratedb.com/docs/crate/crash/en/latest/", None),
    "cloud": ("https://cratedb.com/docs/cloud/en/latest/", None),
    "croud": ("https://cratedb.com/docs/cloud/cli/en/latest/", None),
    "guide": ("https://cratedb.com/docs/guide/", None),
    "influxio": ("https://influxio.readthedocs.io/", None),
}
linkcheck_ignore = [
    r"https://pulse.internetsociety.org/",
    r"https://www.g2.com/",
    r"https://www.trustpilot.com/",
    r"https://developer.eu.surveymonkey.com/",
    r"https://developer.salesforce.com/",
    r"https://developers.facebook.com/",
    r"https://docs.indeed.com/",
    r"https://quickbooks.intuit.com/",
    r"https://app.asana.com/",
    r"https://developer.ca.surveymonkey.com/",
    r"https://docs.customer.io/",
    r"https://personio.de/",
    r"https://www.adjust.com/",
    r"https://support.appsflyer.com/",
    r"https://help.docebo.com/",
    r"https://www.reddit.com/",
    r"https://www.linkedin.com/developers.*",
    r"https://github.com/",
    r"https://web.archive.org/",
    r"https://images.minimus.io/",
]
linkcheck_anchors_ignore_for_url = [
    r"https://developers.zoom.us/",
    r"https://docs.customer.io/",
    r"https://github.com/",
    r"https://support.appsflyer.com/",
    r"https://support.axon.ai/",
    r"https://developers.zoom.us/",
    r"https://docs.customer.io/",
]

# Retry a link a few times before declaring it broken. The linkcheck runs against ~150
# external URLs on every PR (and nightly); without retries a single transient timeout on
# any one host reds the whole lane.
linkcheck_retries = 3

# Disable caching remote inventories completely.
# http://www.sphinx-doc.org/en/stable/ext/intersphinx.html#confval-intersphinx_cache_limit
# intersphinx_cache_limit = 0


# -- Extension configuration -------------------------------------------------

sphinx_tabs_valid_builders = ["linkcheck"]
todo_include_todos = True

# Configure sphinx-copybutton
copybutton_remove_prompts = True
copybutton_line_continuation_character = "\\"
copybutton_prompt_text = (
    r">>> |\.\.\. |\$ |sh\$ |PS> |cr> |mysql> |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: "
)
copybutton_prompt_is_regexp = True

# Configure sphinxext-opengraph
ogp_site_url = "https://omniload.readthedocs.io/"
ogp_enable_meta_description = True
# ogp_image = "http://example.org/image.png"
# ogp_description_length = 300

# Configure sphinx-sitemap
sitemap_url_scheme = "{link}"
sitemap_excludes = [
    "search.html",
    "genindex.html",
    "_snippet/*",
]

# -- Options for MyST -------------------------------------------------

myst_heading_anchors = 3
myst_enable_extensions = [
    "attrs_block",
    "attrs_inline",
    "colon_fence",
    "deflist",
    "fieldlist",
    "html_admonition",
    "html_image",
    "linkify",
    "replacements",
    "strikethrough",
    "substitution",
    "tasklist",
]
myst_substitutions = {}
myst_html_meta = {
    "google-site-verification": "kDH2Ws762oLwf5V3l-fGrYR9-lgLF6S_Bpd4Dairh7A",
    "msvalidate.01": "E9B1983DD903904FDB7F4E5612962EE7",
}
